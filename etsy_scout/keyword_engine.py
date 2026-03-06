"""Keyword mining, scoring, and reverse listing engine.

Coordinates autocomplete mining, deduplication, database storage,
keyword scoring based on multiple signals, and reverse listing lookups
via search result probing.
"""

import logging
import re
import signal
import time
from datetime import datetime, date

from bs4 import BeautifulSoup

from etsy_scout.db import (
    KeywordRepository, ListingRepository, KeywordRankingRepository, init_db,
)
from etsy_scout.collectors.autocomplete import mine_autocomplete
from etsy_scout.http_client import fetch, get_browser_headers
from etsy_scout.rate_limiter import registry as rate_registry
from etsy_scout.config import Config

logger = logging.getLogger(__name__)


def mine_keywords(seed, depth=1, progress_callback=None):
    """Mine keywords from autocomplete and store results.

    Args:
        seed: Seed keyword to mine (e.g., "custom mug").
        depth: Mining depth (1 = seed + a-z, 2 = recursive expansion).
        progress_callback: Optional callable(completed, total) for progress.

    Returns:
        Dict with mining results.
    """
    init_db()

    logger.info(f'Starting keyword mining: seed="{seed}", depth={depth}')

    raw_results = mine_autocomplete(
        seed,
        depth=depth,
        progress_callback=progress_callback,
    )

    repo = KeywordRepository()
    try:
        new_count = 0
        existing_count = 0
        keywords = []

        for keyword, position in raw_results:
            keyword_id, is_new = repo.upsert_keyword(
                keyword,
                source='autocomplete',
                category=seed,
            )

            repo.add_metric(keyword_id, autocomplete_position=position)

            if is_new:
                new_count += 1
            else:
                existing_count += 1

            keywords.append((keyword, position, is_new))

        logger.info(
            f'Mining complete: {new_count} new, {existing_count} existing, '
            f'{len(keywords)} total'
        )

        return {
            'new_count': new_count,
            'existing_count': existing_count,
            'total_mined': len(keywords),
            'keywords': keywords,
            'seed': seed,
            'depth': depth,
        }

    finally:
        repo.close()


class KeywordScorer:
    """Scores keywords based on multiple signals.

    Scoring combines autocomplete presence, competition level (listing count),
    engagement signals (favorites), and real ads performance data.
    """

    def __init__(self):
        init_db()
        self._repo = KeywordRepository()

    def close(self):
        self._repo.close()

    def score_keyword(self, keyword_id: int) -> float:
        """Compute composite score for a keyword.

        Score components:
        - Autocomplete presence: up to 100 points (pos 1 = 100, pos 10 = 10)
        - Low competition (few listings): up to 30 points
        - High engagement (favorites): up to 25 points
        - Real ads impressions: up to 20 points
        - Real ads orders: up to 30 points

        Returns:
            Composite score (0-205 theoretical max).
        """
        kw = self._repo.get_keyword_with_metrics(keyword_id)
        if kw is None:
            return 0.0

        score = 0.0

        # Autocomplete presence (people are searching for this)
        autocomplete_position = kw['autocomplete_position']
        if autocomplete_position is not None and autocomplete_position > 0:
            score += max(0, 11 - autocomplete_position) * 10

        # Low competition (fewer competing listings = easier to rank)
        listing_count = kw['listing_count']
        if listing_count is not None:
            if listing_count < 10000:
                score += 30
            elif listing_count < 50000:
                score += 15

        # High engagement (top listings have lots of favorites)
        avg_favorites = kw['avg_favorites_top']
        if avg_favorites is not None:
            if avg_favorites > 1000:
                score += 25
            elif avg_favorites > 100:
                score += 10

        # Real ads data (highest quality signal)
        impressions = kw['impressions']
        if impressions is not None and impressions > 100:
            score += 20
        elif impressions is not None and impressions > 0:
            score += 5

        orders = kw['orders']
        if orders is not None and orders > 0:
            score += 30
            if orders >= 5:
                score += 10
            if orders >= 10:
                score += 10

        return score

    def score_all_keywords(self, recalculate=False) -> int:
        """Score active keywords in the database."""
        if recalculate:
            keyword_ids = self._repo.get_all_keyword_ids(active_only=True)
        else:
            keyword_ids = self._repo.get_unscored_keyword_ids()

        count = 0

        for keyword_id in keyword_ids:
            score = self.score_keyword(keyword_id)
            self._repo.update_score(keyword_id, score)
            count += 1

        logger.info(f'Scored {count} keywords (recalculate={recalculate})')
        return count

    def get_top_keywords(self, limit=50, min_score=0) -> list:
        return self._repo.get_keywords_with_latest_metrics(
            limit=limit,
            min_score=min_score,
            order_by='score',
        )


class ReverseListing:
    """Reverse listing lookup via Etsy search probing.

    Finds which keywords a given listing ranks for by searching Etsy
    for each known keyword and checking if the target listing appears
    in the results.
    """

    SEARCH_URL = 'https://www.etsy.com/search'

    def __init__(self):
        init_db()
        self._kw_repo = KeywordRepository()
        self._listing_repo = ListingRepository()
        self._ranking_repo = KeywordRankingRepository()

        rate_registry.get_limiter(
            'search_probe', rate=Config.SEARCH_PROBE_RATE_LIMIT
        )

        self._interrupted = False

    def close(self):
        self._kw_repo.close()
        self._listing_repo.close()
        self._ranking_repo.close()

    def reverse_listing_probe(self, listing_id, top_n=None,
                              progress_callback=None):
        """Find keywords that a given listing ranks for.

        Args:
            listing_id: The Etsy listing ID to look up.
            top_n: Only check top N keywords (by score, or all if None).
            progress_callback: Optional callable(completed, total, found, keyword).

        Returns:
            List of dicts: [{'keyword': str, 'position': int,
                            'snapshot_date': str, 'source': str}]
        """
        listing_id = str(listing_id).strip()

        listing = self._listing_repo.find_by_listing_id(listing_id)
        if not listing:
            db_id, _ = self._listing_repo.upsert_listing(listing_id=listing_id)
        else:
            db_id = listing['id']

        if top_n:
            keywords = self._kw_repo.get_keywords_with_latest_metrics(
                limit=top_n, min_score=0, order_by='score',
            )
        else:
            keywords = self._kw_repo.get_all_keywords(active_only=True)

        if not keywords:
            logger.warning('No keywords in database to probe.')
            return []

        total = len(keywords)
        today = date.today().isoformat()
        results = []
        completed = 0
        self._interrupted = False

        original_handler = signal.getsignal(signal.SIGINT)

        def interrupt_handler(signum, frame):
            self._interrupted = True
            logger.info('Interrupt received, saving partial results...')

        signal.signal(signal.SIGINT, interrupt_handler)

        try:
            for kw_row in keywords:
                if self._interrupted:
                    logger.info(
                        f'Interrupted after {completed}/{total} keywords. '
                        f'Partial results saved.'
                    )
                    break

                keyword = kw_row['keyword']
                keyword_id = kw_row['id']

                position = self._probe_search(keyword, listing_id)

                if position is not None:
                    self._ranking_repo.add_ranking(
                        keyword_id=keyword_id,
                        listing_db_id=db_id,
                        position=position,
                        source='probe',
                        snapshot_date=today,
                    )
                    results.append({
                        'keyword': keyword,
                        'position': position,
                        'snapshot_date': today,
                        'source': 'probe',
                    })

                completed += 1
                if progress_callback:
                    progress_callback(completed, total, len(results), keyword)

        finally:
            signal.signal(signal.SIGINT, original_handler)

        logger.info(
            f'Search probe reverse listing for {listing_id}: '
            f'{len(results)} rankings found out of {completed} keywords checked'
        )
        return results

    def _probe_search(self, keyword, target_listing_id):
        """Search Etsy for a keyword and check if the target listing appears.

        Returns:
            1-based position if found, None if not found.
        """
        rate_registry.acquire('search_probe')

        params = {
            'q': keyword,
            'ref': 'search_bar',
        }

        try:
            response = fetch(
                self.SEARCH_URL,
                params=params,
                headers=get_browser_headers(),
            )

            if response.status_code != 200:
                logger.debug(
                    f'Search returned {response.status_code} for "{keyword}"'
                )
                return None

            html = response.text

            if self._is_captcha(html):
                logger.warning(
                    f'CAPTCHA detected during search probe for "{keyword}". '
                    'Backing off 30 seconds...'
                )
                time.sleep(30)
                return None

            return self._find_listing_in_results(html, target_listing_id)

        except Exception as e:
            logger.error(f'Error probing search for "{keyword}": {e}')
            return None

    def _is_captcha(self, html):
        captcha_markers = [
            'Enter the characters you see below',
            'we need to make sure you\'re not a robot',
            'captcha',
            'Please verify you are a human',
        ]
        html_lower = html.lower()
        return any(marker.lower() in html_lower for marker in captcha_markers)

    def _find_listing_in_results(self, html, target_listing_id):
        """Parse Etsy search results HTML and find the target listing position.

        Filters out ad/promoted results. Returns the 1-based organic position.
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Etsy uses data-listing-id attributes on result cards
        result_divs = soup.find_all(
            attrs={'data-listing-id': True}
        )

        # Also try finding listing IDs in href patterns
        if not result_divs:
            result_divs = soup.find_all('a', href=re.compile(r'/listing/\d+'))

        organic_position = 0

        for div in result_divs:
            lid = div.get('data-listing-id', '')
            if not lid:
                # Extract from href
                href = div.get('href', '')
                match = re.search(r'/listing/(\d+)', href)
                if match:
                    lid = match.group(1)

            if not lid:
                continue

            # Check if this is a promoted/ad result
            if self._is_promoted(div):
                continue

            organic_position += 1

            if str(lid) == str(target_listing_id):
                return organic_position

        return None

    def _is_promoted(self, element):
        """Check if a search result element is a promoted/ad listing."""
        el_str = str(element)
        promoted_markers = [
            'is-ad',
            'promoted',
            'Ad by',
            'data-is-ad="true"',
            'wt-text-link-no-underline',
        ]
        for marker in promoted_markers:
            if marker in el_str:
                return True
        return False
