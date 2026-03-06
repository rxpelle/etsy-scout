"""SQLite database management for Etsy Scout.

Handles schema creation, migrations, and provides repository classes
for each entity type.
"""

import os
import sqlite3
import logging
from datetime import datetime, date
from pathlib import Path

from etsy_scout.config import Config

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    category TEXT,
    first_seen TEXT NOT NULL,
    last_updated TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    score REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS keyword_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword_id INTEGER NOT NULL REFERENCES keywords(id),
    snapshot_date TEXT NOT NULL,
    autocomplete_position INTEGER,
    listing_count INTEGER,
    avg_favorites_top INTEGER,
    avg_reviews_top INTEGER,
    impressions INTEGER,
    clicks INTEGER,
    orders INTEGER,
    revenue REAL,
    spend REAL,
    UNIQUE(keyword_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id TEXT NOT NULL UNIQUE,
    title TEXT,
    shop_name TEXT,
    is_own INTEGER DEFAULT 0,
    added_date TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS listing_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    snapshot_date TEXT NOT NULL,
    price REAL,
    favorites INTEGER,
    review_count INTEGER,
    avg_rating REAL,
    total_sales INTEGER,
    views INTEGER,
    UNIQUE(listing_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS keyword_rankings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword_id INTEGER NOT NULL REFERENCES keywords(id),
    listing_id INTEGER NOT NULL REFERENCES listings(id),
    snapshot_date TEXT NOT NULL,
    rank_position INTEGER,
    source TEXT,
    UNIQUE(keyword_id, listing_id, snapshot_date)
);

CREATE TABLE IF NOT EXISTS ads_search_terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_title TEXT,
    search_term TEXT NOT NULL,
    impressions INTEGER,
    clicks INTEGER,
    ctr REAL,
    spend REAL,
    revenue REAL,
    orders INTEGER,
    roas REAL,
    report_date TEXT NOT NULL,
    imported_at TEXT NOT NULL
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_keywords_keyword ON keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_keywords_source ON keywords(source);
CREATE INDEX IF NOT EXISTS idx_keywords_category ON keywords(category);
CREATE INDEX IF NOT EXISTS idx_keywords_active ON keywords(is_active);

CREATE INDEX IF NOT EXISTS idx_keyword_metrics_keyword_id ON keyword_metrics(keyword_id);
CREATE INDEX IF NOT EXISTS idx_keyword_metrics_date ON keyword_metrics(snapshot_date);

CREATE INDEX IF NOT EXISTS idx_listings_listing_id ON listings(listing_id);
CREATE INDEX IF NOT EXISTS idx_listings_is_own ON listings(is_own);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_listing_id ON listing_snapshots(listing_id);
CREATE INDEX IF NOT EXISTS idx_listing_snapshots_date ON listing_snapshots(snapshot_date);

CREATE INDEX IF NOT EXISTS idx_keyword_rankings_keyword ON keyword_rankings(keyword_id);
CREATE INDEX IF NOT EXISTS idx_keyword_rankings_listing ON keyword_rankings(listing_id);
CREATE INDEX IF NOT EXISTS idx_keyword_rankings_date ON keyword_rankings(snapshot_date);

CREATE INDEX IF NOT EXISTS idx_ads_search_terms_term ON ads_search_terms(search_term);
CREATE INDEX IF NOT EXISTS idx_ads_search_terms_date ON ads_search_terms(report_date);
"""


def get_connection():
    """Get a database connection, creating the database if needed."""
    db_path = Config.get_db_path()

    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')

    return conn


def init_db():
    """Initialize the database schema and indexes."""
    conn = get_connection()
    try:
        conn.executescript(SCHEMA_SQL)
        conn.executescript(INDEX_SQL)
        conn.commit()
        logger.info(f'Database initialized at {Config.get_db_path()}')
    finally:
        conn.close()


class KeywordRepository:
    """Data access for keywords and keyword_metrics tables."""

    def __init__(self, conn=None):
        self._conn = conn or get_connection()
        self._owns_conn = conn is None

    def close(self):
        if self._owns_conn:
            self._conn.close()

    def find_by_keyword(self, keyword):
        cursor = self._conn.execute(
            'SELECT * FROM keywords WHERE keyword = ?',
            (keyword.lower().strip(),),
        )
        return cursor.fetchone()

    def upsert_keyword(self, keyword, source='autocomplete', category=None):
        """Insert a keyword or update its last_updated timestamp.

        Returns:
            Tuple of (keyword_id, is_new).
        """
        keyword = keyword.lower().strip()
        now = datetime.now().isoformat()

        existing = self.find_by_keyword(keyword)
        if existing:
            self._conn.execute(
                'UPDATE keywords SET last_updated = ? WHERE id = ?',
                (now, existing['id']),
            )
            self._conn.commit()
            return existing['id'], False

        cursor = self._conn.execute(
            'INSERT INTO keywords (keyword, source, category, first_seen, last_updated) '
            'VALUES (?, ?, ?, ?, ?)',
            (keyword, source, category, now, now),
        )
        self._conn.commit()
        return cursor.lastrowid, True

    def add_metric(self, keyword_id, autocomplete_position=None, **kwargs):
        """Add a keyword_metrics snapshot for today. Merges with existing."""
        today = date.today().isoformat()

        existing = self._conn.execute(
            'SELECT * FROM keyword_metrics WHERE keyword_id = ? AND snapshot_date = ?',
            (keyword_id, today),
        ).fetchone()

        if existing:
            updates = []
            params = []

            if autocomplete_position is not None:
                updates.append('autocomplete_position = ?')
                params.append(autocomplete_position)

            merge_fields = [
                'listing_count', 'avg_favorites_top', 'avg_reviews_top',
                'impressions', 'clicks', 'orders', 'revenue', 'spend',
            ]
            for field in merge_fields:
                val = kwargs.get(field)
                if val is not None:
                    updates.append(f'{field} = ?')
                    params.append(val)

            if updates:
                params.append(existing['id'])
                self._conn.execute(
                    f'UPDATE keyword_metrics SET {", ".join(updates)} WHERE id = ?',
                    params,
                )
        else:
            self._conn.execute(
                'INSERT INTO keyword_metrics '
                '(keyword_id, snapshot_date, autocomplete_position, '
                'listing_count, avg_favorites_top, avg_reviews_top, '
                'impressions, clicks, orders, revenue, spend) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    keyword_id,
                    today,
                    autocomplete_position,
                    kwargs.get('listing_count'),
                    kwargs.get('avg_favorites_top'),
                    kwargs.get('avg_reviews_top'),
                    kwargs.get('impressions'),
                    kwargs.get('clicks'),
                    kwargs.get('orders'),
                    kwargs.get('revenue'),
                    kwargs.get('spend'),
                ),
            )
        self._conn.commit()

    def get_all_keywords(self, active_only=True):
        query = 'SELECT * FROM keywords'
        if active_only:
            query += ' WHERE is_active = 1'
        query += ' ORDER BY last_updated DESC'
        return self._conn.execute(query).fetchall()

    def get_keyword_count(self):
        row = self._conn.execute('SELECT COUNT(*) as cnt FROM keywords').fetchone()
        return row['cnt']

    def get_keywords_with_latest_metrics(self, limit=20, min_score=0,
                                         order_by='score'):
        if order_by == 'score':
            order_clause = """
                ORDER BY k.score DESC,
                    CASE WHEN km.autocomplete_position IS NOT NULL THEN 0 ELSE 1 END,
                    km.autocomplete_position ASC
            """
        elif order_by == 'impressions':
            order_clause = """
                ORDER BY km.impressions DESC NULLS LAST,
                    k.score DESC
            """
        else:
            order_clause = """
                ORDER BY
                    CASE WHEN km.autocomplete_position IS NOT NULL THEN 0 ELSE 1 END,
                    km.autocomplete_position ASC,
                    k.last_updated DESC
            """

        query = f"""
            SELECT k.id, k.keyword, k.source, k.first_seen, k.category, k.score,
                   km.autocomplete_position, km.snapshot_date,
                   km.listing_count, km.avg_favorites_top,
                   km.impressions, km.clicks, km.orders, km.revenue, km.spend
            FROM keywords k
            LEFT JOIN keyword_metrics km ON k.id = km.keyword_id
                AND km.snapshot_date = (
                    SELECT MAX(snapshot_date)
                    FROM keyword_metrics
                    WHERE keyword_id = k.id
                )
            WHERE k.is_active = 1 AND k.score >= ?
            {order_clause}
            LIMIT ?
        """
        return self._conn.execute(query, (min_score, limit)).fetchall()

    def get_keyword_with_metrics(self, keyword_id):
        query = """
            SELECT k.id, k.keyword, k.source, k.first_seen, k.category, k.score,
                   km.autocomplete_position, km.snapshot_date,
                   km.listing_count, km.avg_favorites_top,
                   km.impressions, km.clicks, km.orders, km.revenue, km.spend
            FROM keywords k
            LEFT JOIN keyword_metrics km ON k.id = km.keyword_id
                AND km.snapshot_date = (
                    SELECT MAX(snapshot_date)
                    FROM keyword_metrics
                    WHERE keyword_id = k.id
                )
            WHERE k.id = ?
        """
        return self._conn.execute(query, (keyword_id,)).fetchone()

    def update_score(self, keyword_id, score):
        self._conn.execute(
            'UPDATE keywords SET score = ? WHERE id = ?',
            (score, keyword_id),
        )
        self._conn.commit()

    def get_all_keyword_ids(self, active_only=True):
        query = 'SELECT id FROM keywords'
        if active_only:
            query += ' WHERE is_active = 1'
        rows = self._conn.execute(query).fetchall()
        return [row['id'] for row in rows]

    def get_unscored_keyword_ids(self):
        query = 'SELECT id FROM keywords WHERE is_active = 1 AND (score IS NULL OR score = 0)'
        rows = self._conn.execute(query).fetchall()
        return [row['id'] for row in rows]


class ListingRepository:
    """Data access for listings and listing_snapshots tables."""

    def __init__(self, conn=None):
        self._conn = conn or get_connection()
        self._owns_conn = conn is None

    def close(self):
        if self._owns_conn:
            self._conn.close()

    def find_by_listing_id(self, listing_id):
        cursor = self._conn.execute(
            'SELECT * FROM listings WHERE listing_id = ?',
            (str(listing_id).strip(),),
        )
        return cursor.fetchone()

    def upsert_listing(self, listing_id, title=None, shop_name=None,
                       is_own=False, notes=None):
        """Insert a listing or update its metadata.

        Returns:
            Tuple of (db_id, is_new).
        """
        listing_id = str(listing_id).strip()
        now = datetime.now().isoformat()

        existing = self.find_by_listing_id(listing_id)
        if existing:
            updates = []
            params = []
            if title is not None:
                updates.append('title = ?')
                params.append(title)
            if shop_name is not None:
                updates.append('shop_name = ?')
                params.append(shop_name)
            if is_own:
                updates.append('is_own = ?')
                params.append(1)
            if notes is not None:
                updates.append('notes = ?')
                params.append(notes)

            if updates:
                params.append(existing['id'])
                self._conn.execute(
                    f'UPDATE listings SET {", ".join(updates)} WHERE id = ?',
                    params,
                )
                self._conn.commit()

            return existing['id'], False

        cursor = self._conn.execute(
            'INSERT INTO listings (listing_id, title, shop_name, is_own, added_date, notes) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (listing_id, title, shop_name, 1 if is_own else 0, now, notes),
        )
        self._conn.commit()
        return cursor.lastrowid, True

    def remove_listing(self, listing_id):
        listing_id = str(listing_id).strip()
        existing = self.find_by_listing_id(listing_id)
        if not existing:
            return False

        db_id = existing['id']
        self._conn.execute(
            'DELETE FROM listing_snapshots WHERE listing_id = ?', (db_id,)
        )
        self._conn.execute(
            'DELETE FROM listings WHERE id = ?', (db_id,)
        )
        self._conn.commit()
        return True

    def get_all_listings(self):
        return self._conn.execute(
            'SELECT * FROM listings ORDER BY is_own DESC, title ASC'
        ).fetchall()

    def add_snapshot(self, db_listing_id, price=None, favorites=None,
                     review_count=None, avg_rating=None, total_sales=None,
                     views=None):
        """Add a snapshot for a tracked listing. Updates if exists for today."""
        today = date.today().isoformat()

        existing = self._conn.execute(
            'SELECT id FROM listing_snapshots WHERE listing_id = ? AND snapshot_date = ?',
            (db_listing_id, today),
        ).fetchone()

        if existing:
            self._conn.execute(
                'UPDATE listing_snapshots SET '
                'price = ?, favorites = ?, review_count = ?, avg_rating = ?, '
                'total_sales = ?, views = ? '
                'WHERE id = ?',
                (price, favorites, review_count, avg_rating,
                 total_sales, views, existing['id']),
            )
            self._conn.commit()
            return existing['id']

        cursor = self._conn.execute(
            'INSERT INTO listing_snapshots '
            '(listing_id, snapshot_date, price, favorites, review_count, '
            'avg_rating, total_sales, views) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (db_listing_id, today, price, favorites, review_count,
             avg_rating, total_sales, views),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_latest_snapshot(self, db_listing_id):
        return self._conn.execute(
            'SELECT * FROM listing_snapshots WHERE listing_id = ? '
            'ORDER BY snapshot_date DESC LIMIT 1',
            (db_listing_id,),
        ).fetchone()

    def get_listings_with_latest_snapshot(self):
        query = """
            SELECT l.*, ls.price, ls.favorites,
                   ls.review_count, ls.avg_rating, ls.total_sales,
                   ls.views, ls.snapshot_date as last_snapshot_date
            FROM listings l
            LEFT JOIN listing_snapshots ls ON l.id = ls.listing_id
                AND ls.snapshot_date = (
                    SELECT MAX(snapshot_date)
                    FROM listing_snapshots
                    WHERE listing_id = l.id
                )
            ORDER BY l.is_own DESC, ls.favorites DESC
        """
        return self._conn.execute(query).fetchall()


class AdsRepository:
    """Data access for ads_search_terms table."""

    def __init__(self, conn=None):
        self._conn = conn or get_connection()
        self._owns_conn = conn is None

    def close(self):
        if self._owns_conn:
            self._conn.close()

    def add_search_term(self, listing_title=None, search_term=None,
                        impressions=None, clicks=None, ctr=None,
                        spend=None, revenue=None, orders=None, roas=None,
                        report_date=None, imported_at=None):
        cursor = self._conn.execute(
            'INSERT INTO ads_search_terms '
            '(listing_title, search_term, impressions, clicks, ctr, '
            'spend, revenue, orders, roas, report_date, imported_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (listing_title, search_term, impressions, clicks, ctr,
             spend, revenue, orders, roas, report_date, imported_at),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_aggregated_search_terms(self):
        return self._conn.execute(
            'SELECT search_term, '
            '  SUM(impressions) as total_impressions, '
            '  SUM(clicks) as total_clicks, '
            '  SUM(spend) as total_spend, '
            '  SUM(revenue) as total_revenue, '
            '  SUM(orders) as total_orders, '
            '  CASE WHEN SUM(spend) > 0 '
            '    THEN SUM(revenue) / SUM(spend) '
            '    ELSE NULL END as avg_roas, '
            '  CASE WHEN SUM(impressions) > 0 '
            '    THEN CAST(SUM(clicks) AS REAL) / SUM(impressions) '
            '    ELSE NULL END as avg_ctr '
            'FROM ads_search_terms '
            'GROUP BY search_term '
            'ORDER BY total_orders DESC, total_impressions DESC'
        ).fetchall()

    def get_search_term_count(self):
        row = self._conn.execute(
            'SELECT COUNT(*) as cnt FROM ads_search_terms'
        ).fetchone()
        return row['cnt']

    def get_opportunity_keywords(self):
        """Keywords with impressions but no orders."""
        return self._conn.execute(
            'SELECT search_term, '
            '  SUM(impressions) as total_impressions, '
            '  SUM(clicks) as total_clicks, '
            '  SUM(spend) as total_spend, '
            '  SUM(orders) as total_orders '
            'FROM ads_search_terms '
            'GROUP BY search_term '
            'HAVING SUM(impressions) > 0 AND (SUM(orders) IS NULL OR SUM(orders) = 0) '
            'ORDER BY total_impressions DESC'
        ).fetchall()


class KeywordRankingRepository:
    """Data access for keyword_rankings table."""

    def __init__(self, conn=None):
        self._conn = conn or get_connection()
        self._owns_conn = conn is None

    def close(self):
        if self._owns_conn:
            self._conn.close()

    def add_ranking(self, keyword_id, listing_db_id, position, source,
                    snapshot_date=None):
        if snapshot_date is None:
            snapshot_date = date.today().isoformat()

        existing = self._conn.execute(
            'SELECT id FROM keyword_rankings '
            'WHERE keyword_id = ? AND listing_id = ? AND snapshot_date = ?',
            (keyword_id, listing_db_id, snapshot_date),
        ).fetchone()

        if existing:
            self._conn.execute(
                'UPDATE keyword_rankings SET rank_position = ?, source = ? '
                'WHERE id = ?',
                (position, source, existing['id']),
            )
            self._conn.commit()
            return existing['id']

        cursor = self._conn.execute(
            'INSERT INTO keyword_rankings '
            '(keyword_id, listing_id, snapshot_date, rank_position, source) '
            'VALUES (?, ?, ?, ?, ?)',
            (keyword_id, listing_db_id, snapshot_date, position, source),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_rankings_for_listing(self, db_listing_id, snapshot_date=None):
        if snapshot_date:
            query = """
                SELECT kr.*, k.keyword
                FROM keyword_rankings kr
                JOIN keywords k ON kr.keyword_id = k.id
                WHERE kr.listing_id = ? AND kr.snapshot_date = ?
                ORDER BY kr.rank_position ASC
            """
            return self._conn.execute(query, (db_listing_id, snapshot_date)).fetchall()

        query = """
            SELECT kr.*, k.keyword
            FROM keyword_rankings kr
            JOIN keywords k ON kr.keyword_id = k.id
            WHERE kr.listing_id = ?
              AND kr.snapshot_date = (
                  SELECT MAX(snapshot_date) FROM keyword_rankings
                  WHERE listing_id = ?
              )
            ORDER BY kr.rank_position ASC
        """
        return self._conn.execute(query, (db_listing_id, db_listing_id)).fetchall()
