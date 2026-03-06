"""Competitor analysis engine for Etsy Scout.

Coordinates listing tracking, snapshots, and competitor comparisons.
"""

import logging

from etsy_scout.db import ListingRepository, init_db
from etsy_scout.collectors.listing_scraper import ListingScraper, CaptchaDetected

logger = logging.getLogger(__name__)


class CompetitorEngine:
    """Manages listing tracking, snapshots, and competitor comparisons."""

    def __init__(self):
        init_db()
        self._repo = ListingRepository()
        self._scraper = ListingScraper()

    def close(self):
        self._repo.close()

    def add_listing(self, listing_id, name=None, is_own=False):
        """Add a listing to tracking. Scrapes initial data and stores in DB.

        Args:
            listing_id: Etsy listing ID.
            name: Optional display name override.
            is_own: Whether this is the user's own listing.

        Returns:
            Dict with listing data and snapshot info, or None on failure.
        """
        listing_id = str(listing_id).strip()
        logger.info(f'Adding listing to tracking: {listing_id}')

        try:
            scraped = self._scraper.scrape_listing(listing_id)
        except CaptchaDetected:
            raise
        except Exception as e:
            logger.error(f'Failed to scrape listing {listing_id}: {e}')
            scraped = None

        title = name
        shop_name = None
        if scraped:
            title = name or scraped.get('title')
            shop_name = scraped.get('shop_name')

        db_id, is_new = self._repo.upsert_listing(
            listing_id=listing_id,
            title=title,
            shop_name=shop_name,
            is_own=is_own,
        )

        result = {
            'db_id': db_id,
            'listing_id': listing_id,
            'title': title,
            'shop_name': shop_name,
            'is_own': is_own,
            'is_new': is_new,
            'scraped': scraped,
            'snapshot': None,
        }

        if scraped:
            snapshot = self._store_snapshot(db_id, scraped)
            result['snapshot'] = snapshot

        return result

    def remove_listing(self, listing_id):
        listing_id = str(listing_id).strip()
        removed = self._repo.remove_listing(listing_id)
        if removed:
            logger.info(f'Removed listing from tracking: {listing_id}')
        else:
            logger.warning(f'Listing not found for removal: {listing_id}')
        return removed

    def list_listings(self):
        return self._repo.get_listings_with_latest_snapshot()

    def take_snapshot(self, listing_id=None):
        """Take a snapshot of tracked listings.

        If listing_id is None, snapshots ALL tracked listings.
        """
        if listing_id:
            listings = [self._repo.find_by_listing_id(str(listing_id).strip())]
            if listings[0] is None:
                logger.warning(f'Listing not found: {listing_id}')
                return []
        else:
            listings = self._repo.get_all_listings()

        results = []
        for listing in listings:
            lid = listing['listing_id']
            db_id = listing['id']

            prev_snapshot = self._repo.get_latest_snapshot(db_id)

            try:
                scraped = self._scraper.scrape_listing(lid)
                if scraped is None:
                    results.append({
                        'listing_id': lid,
                        'title': listing['title'],
                        'success': False,
                        'error': 'Scrape returned no data',
                    })
                    continue

                if scraped.get('title') and not listing['title']:
                    self._repo.upsert_listing(
                        listing_id=lid,
                        title=scraped['title'],
                        shop_name=scraped.get('shop_name'),
                    )

                snapshot = self._store_snapshot(db_id, scraped)

                changes = {}
                if prev_snapshot:
                    changes = self._calculate_changes(prev_snapshot, snapshot)

                results.append({
                    'listing_id': lid,
                    'title': listing['title'] or scraped.get('title', 'Unknown'),
                    'success': True,
                    'snapshot': snapshot,
                    'changes': changes,
                })

            except CaptchaDetected as e:
                logger.warning(f'CAPTCHA detected while snapshotting {lid}')
                results.append({
                    'listing_id': lid,
                    'title': listing['title'],
                    'success': False,
                    'error': str(e),
                })
            except Exception as e:
                logger.error(f'Error snapshotting {lid}: {e}')
                results.append({
                    'listing_id': lid,
                    'title': listing['title'],
                    'success': False,
                    'error': str(e),
                })

        return results

    def compare_listings(self, listing_ids=None):
        all_listings = self._repo.get_listings_with_latest_snapshot()

        if listing_ids:
            lid_set = {str(lid).strip() for lid in listing_ids}
            return [l for l in all_listings if l['listing_id'] in lid_set]

        return all_listings

    def _store_snapshot(self, db_id, scraped):
        snapshot_id = self._repo.add_snapshot(
            db_listing_id=db_id,
            price=scraped.get('price'),
            favorites=scraped.get('favorites'),
            review_count=scraped.get('review_count'),
            avg_rating=scraped.get('avg_rating'),
            total_sales=scraped.get('total_sales'),
            views=scraped.get('views'),
        )

        return {
            'snapshot_id': snapshot_id,
            'price': scraped.get('price'),
            'favorites': scraped.get('favorites'),
            'review_count': scraped.get('review_count'),
            'avg_rating': scraped.get('avg_rating'),
            'total_sales': scraped.get('total_sales'),
            'views': scraped.get('views'),
        }

    def _calculate_changes(self, prev, current):
        changes = {}

        comparisons = [
            ('favorites', 'Favorites', False),    # higher is better
            ('review_count', 'Reviews', False),
            ('avg_rating', 'Rating', False),
            ('price', 'Price', None),              # neutral
            ('total_sales', 'Sales', False),
        ]

        for field, label, lower_is_better in comparisons:
            old_val = prev[field] if prev[field] is not None else None
            new_val = current.get(field)

            if old_val is not None and new_val is not None and old_val != new_val:
                if new_val < old_val:
                    direction = 'improved' if lower_is_better else 'declined'
                elif new_val > old_val:
                    direction = 'declined' if lower_is_better else 'improved'
                else:
                    direction = 'unchanged'

                changes[label] = {
                    'old': old_val,
                    'new': new_val,
                    'direction': direction,
                }

        return changes
