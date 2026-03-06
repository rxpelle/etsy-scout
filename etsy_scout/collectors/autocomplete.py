"""Etsy autocomplete/suggestions keyword miner.

Mines keywords from Etsy's search suggestions by querying a seed keyword
and expanding with a-z suffix variations. The suggestions reflect actual
Etsy buyer search behavior.
"""

import json
import logging
import string

import requests

from etsy_scout.http_client import fetch
from etsy_scout.rate_limiter import registry as rate_registry
from etsy_scout.config import Config

logger = logging.getLogger(__name__)

# Etsy's search suggestions endpoint
AUTOCOMPLETE_URL = 'https://www.etsy.com/api/v3/ajax/member/search/suggestions'


def mine_autocomplete(seed, depth=1, progress_callback=None):
    """Mine keywords from Etsy's autocomplete/suggestions.

    Queries the seed keyword directly, then expands with a-z suffix
    variations. At depth 2, each result is further expanded with a-z.

    Args:
        seed: The seed keyword to mine (e.g., "custom mug").
        depth: Mining depth. 1 = seed + a-z (27 queries).
               2 = depth 1 + expand each result with a-z.
        progress_callback: Optional callable(completed, total) for progress updates.

    Returns:
        List of (keyword, position) tuples, deduplicated and sorted.
    """
    rate_registry.get_limiter('autocomplete', rate=Config.AUTOCOMPLETE_RATE_LIMIT)

    all_results = {}  # keyword -> best position

    # Phase 1: Query seed keyword directly + a-z expansions
    prefixes = [seed] + [f'{seed} {c}' for c in string.ascii_lowercase]
    total_queries = len(prefixes)
    completed = 0

    for prefix in prefixes:
        suggestions = _query_autocomplete(prefix)
        for kw, pos in suggestions:
            if kw not in all_results or pos < all_results[kw]:
                all_results[kw] = pos

        completed += 1
        if progress_callback:
            progress_callback(completed, total_queries)

    # Phase 2: Depth 2 expansion
    if depth >= 2:
        depth1_keywords = list(all_results.keys())
        expansion_prefixes = []
        for kw in depth1_keywords:
            for c in string.ascii_lowercase:
                expansion_prefixes.append(f'{kw} {c}')

        total_queries = completed + len(expansion_prefixes)

        for prefix in expansion_prefixes:
            suggestions = _query_autocomplete(prefix)
            for kw, pos in suggestions:
                if kw not in all_results or pos < all_results[kw]:
                    all_results[kw] = pos

            completed += 1
            if progress_callback:
                progress_callback(completed, total_queries)

    results = sorted(all_results.items(), key=lambda x: (x[1], x[0]))

    logger.info(
        f'Autocomplete mining for "{seed}" (depth={depth}): '
        f'{len(results)} keywords found'
    )

    return results


def _query_autocomplete(prefix):
    """Query the Etsy suggestions endpoint for a single prefix.

    Etsy uses a few possible endpoints. We try the public search
    suggestions first, then fall back to scraping the search page
    for suggestion data.

    Args:
        prefix: Search prefix string.

    Returns:
        List of (keyword, position) tuples.
    """
    rate_registry.acquire('autocomplete')

    # Primary method: Etsy's AJAX suggestions endpoint
    params = {
        'query': prefix,
        'type': 'query',
        'limit': 10,
    }

    try:
        response = fetch(AUTOCOMPLETE_URL, params=params)
    except (requests.Timeout, requests.ConnectionError) as e:
        logger.error(f'Network error querying autocomplete for "{prefix}": {e}')
        return []
    except requests.RequestException as e:
        logger.error(f'Request error querying autocomplete for "{prefix}": {e}')
        return []

    try:
        if response.status_code != 200:
            return _query_suggest_fallbacks(prefix)

        try:
            data = response.json()
        except (json.JSONDecodeError, ValueError):
            return _query_suggest_fallbacks(prefix)

        # Parse Etsy's response format
        results = []
        suggestions = data if isinstance(data, list) else data.get('results', data.get('suggestions', []))

        if isinstance(suggestions, list):
            for i, item in enumerate(suggestions):
                if isinstance(item, str):
                    keyword = item.strip().lower()
                elif isinstance(item, dict):
                    keyword = (item.get('query') or item.get('value') or
                              item.get('text') or '').strip().lower()
                else:
                    continue
                if keyword:
                    results.append((keyword, i + 1))

        if results:
            logger.debug(f'"{prefix}" -> {len(results)} suggestions (etsy api)')
            return results

        return _query_suggest_fallbacks(prefix)

    except Exception as e:
        logger.error(f'Error processing autocomplete for "{prefix}": {e}')
        return []


def _query_suggest_fallbacks(prefix):
    """Try multiple suggest APIs to maximize keyword coverage."""
    # Try Google suggest first
    results = _query_google_suggest(prefix)
    if results:
        return results

    # Try Bing suggest
    return _query_bing_suggest(prefix)


def _query_bing_suggest(prefix):
    """Fallback: Use Bing's autocomplete scoped to Etsy queries."""
    url = 'https://api.bing.com/osjson.aspx'
    params = {
        'query': f'etsy {prefix}',
    }

    try:
        response = fetch(url, params=params)
        if response.status_code != 200:
            return []

        data = response.json()
        if not isinstance(data, list) or len(data) < 2:
            return []

        suggestions = data[1]
        results = []
        for i, suggestion in enumerate(suggestions):
            keyword = suggestion.strip().lower()
            if keyword.startswith('etsy '):
                keyword = keyword[5:]
            keyword = keyword.strip()
            if keyword and keyword != prefix.lower():
                results.append((keyword, i + 1))

        logger.debug(f'"{prefix}" -> {len(results)} suggestions (bing fallback)')
        return results

    except Exception as e:
        logger.debug(f'Bing suggest fallback failed for "{prefix}": {e}')
        return []


def _query_google_suggest(prefix):
    """Fallback: Use Google's autocomplete scoped to etsy.com.

    Google's suggest API can return Etsy-related suggestions when
    we prepend "etsy" or "site:etsy.com" to the query.

    Args:
        prefix: Search prefix string.

    Returns:
        List of (keyword, position) tuples.
    """
    url = 'https://suggestqueries.google.com/complete/search'
    params = {
        'client': 'firefox',
        'q': f'etsy {prefix}',
    }

    try:
        response = fetch(url, params=params)
        if response.status_code != 200:
            return []

        data = response.json()
        if not isinstance(data, list) or len(data) < 2:
            return []

        suggestions = data[1]
        results = []
        for i, suggestion in enumerate(suggestions):
            # Strip "etsy " prefix from results
            keyword = suggestion.strip().lower()
            if keyword.startswith('etsy '):
                keyword = keyword[5:]
            keyword = keyword.strip()
            if keyword and keyword != prefix.lower():
                results.append((keyword, i + 1))

        logger.debug(f'"{prefix}" -> {len(results)} suggestions (google fallback)')
        return results

    except Exception as e:
        logger.debug(f'Google suggest fallback failed for "{prefix}": {e}')
        return []
