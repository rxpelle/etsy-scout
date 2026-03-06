"""Etsy Ads search term report importer.

Parses Etsy Ads CSV files exported from the Etsy Ads dashboard.
Supports common column name variations and metadata header rows.

Imported data is stored in ads_search_terms and cross-referenced with
the keywords table to enrich keyword metrics with real ads performance data.
"""

import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from etsy_scout.db import AdsRepository, KeywordRepository, init_db

logger = logging.getLogger(__name__)

# Column name mappings: canonical name -> list of possible column names (lowercase)
COLUMN_ALIASES = {
    'listing_title': [
        'listing title', 'listing', 'title', 'listing name',
    ],
    'search_term': [
        'search query', 'search term', 'query', 'customer search term',
        'keyword', 'search terms',
    ],
    'impressions': ['impressions', 'impr', 'impr.'],
    'clicks': ['clicks', 'click'],
    'ctr': [
        'click-through rate', 'ctr', 'click thru rate',
        'click-thru rate', 'click through rate',
    ],
    'spend': ['spend', 'cost', 'total spend', 'budget spent', 'ad spend'],
    'revenue': [
        'revenue', 'total revenue', 'sales', 'order revenue',
        'total sales',
    ],
    'orders': [
        'orders', 'total orders', 'conversions',
    ],
    'roas': [
        'roas', 'return on ad spend', 'return on spend',
    ],
}


class AdsImporter:
    """Imports Etsy Ads search term report CSVs into the database."""

    def __init__(self):
        init_db()
        self._ads_repo = AdsRepository()
        self._kw_repo = KeywordRepository()

    def close(self):
        self._ads_repo.close()
        self._kw_repo.close()

    def import_csv(self, filepath: str, listing_filter: str = None) -> dict:
        """Import Etsy Ads search term report.

        Args:
            filepath: Path to the CSV file.
            listing_filter: Optional listing title to filter by.

        Returns:
            dict with 'imported', 'skipped', 'keywords_enriched' counts.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f'File not found: {filepath}')

        logger.info(f'Importing Etsy Ads report: {filepath}')

        df = self._read_csv_flexible(filepath)

        if df is None or df.empty:
            logger.warning('No data found in CSV file')
            return {'imported': 0, 'skipped': 0, 'keywords_enriched': 0}

        column_map = self._map_columns(df.columns.tolist())

        if 'search_term' not in column_map:
            raise ValueError(
                'Could not find a search term column in the CSV. '
                f'Found columns: {list(df.columns)}'
            )

        if listing_filter and 'listing_title' in column_map:
            col = column_map['listing_title']
            df = df[df[col].str.contains(listing_filter, case=False, na=False)]
            if df.empty:
                logger.warning(f'No rows match listing filter: {listing_filter}')
                return {'imported': 0, 'skipped': 0, 'keywords_enriched': 0}

        imported = 0
        skipped = 0
        keywords_enriched = 0
        now = datetime.now().isoformat()
        today = datetime.now().strftime('%Y-%m-%d')

        for _, row in df.iterrows():
            search_term = self._get_value(row, column_map, 'search_term')
            if not search_term or not isinstance(search_term, str):
                skipped += 1
                continue

            search_term = search_term.strip().lower()
            if not search_term or search_term == '*':
                skipped += 1
                continue

            impressions = self._parse_int(
                self._get_value(row, column_map, 'impressions')
            )
            clicks = self._parse_int(
                self._get_value(row, column_map, 'clicks')
            )
            ctr = self._parse_percentage(
                self._get_value(row, column_map, 'ctr')
            )
            spend = self._parse_currency(
                self._get_value(row, column_map, 'spend')
            )
            revenue = self._parse_currency(
                self._get_value(row, column_map, 'revenue')
            )
            orders = self._parse_int(
                self._get_value(row, column_map, 'orders')
            )
            roas = self._parse_float(
                self._get_value(row, column_map, 'roas')
            )
            listing_title = self._get_value(row, column_map, 'listing_title')

            try:
                self._ads_repo.add_search_term(
                    listing_title=listing_title,
                    search_term=search_term,
                    impressions=impressions,
                    clicks=clicks,
                    ctr=ctr,
                    spend=spend,
                    revenue=revenue,
                    orders=orders,
                    roas=roas,
                    report_date=today,
                    imported_at=now,
                )
                imported += 1

                keyword_id, _ = self._kw_repo.upsert_keyword(
                    search_term, source='ads_report'
                )

                if impressions or clicks or orders:
                    self._kw_repo.add_metric(
                        keyword_id,
                        impressions=impressions,
                        clicks=clicks,
                        orders=orders,
                        revenue=revenue,
                        spend=spend,
                    )
                    keywords_enriched += 1

            except Exception as e:
                logger.error(
                    f'Database error importing search term "{search_term}": {e}'
                )
                skipped += 1

        result = {
            'imported': imported,
            'skipped': skipped,
            'keywords_enriched': keywords_enriched,
        }
        logger.info(
            f'Import complete: {imported} imported, {skipped} skipped, '
            f'{keywords_enriched} keywords enriched'
        )
        return result

    def _read_csv_flexible(self, filepath):
        """Read a CSV file, handling metadata rows before the header."""
        try:
            df = pd.read_csv(filepath, dtype=str)
            cols_lower = [str(c).lower().strip() for c in df.columns]
            if self._looks_like_header(cols_lower):
                return df
        except Exception:
            pass

        try:
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            with open(filepath, 'r', encoding='latin-1') as f:
                lines = f.readlines()

        for skip_rows in range(min(10, len(lines))):
            try:
                df = pd.read_csv(filepath, skiprows=skip_rows, dtype=str)
                cols_lower = [str(c).lower().strip() for c in df.columns]
                if self._looks_like_header(cols_lower):
                    return df
            except Exception:
                continue

        logger.error(f'Could not find a valid header row in {filepath}')
        return None

    def _looks_like_header(self, columns_lower):
        known_terms = set()
        for canonical, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in columns_lower:
                    known_terms.add(canonical)
                    break
        return len(known_terms) >= 3

    def _map_columns(self, columns):
        column_map = {}
        cols_lower = {str(c).lower().strip(): c for c in columns}

        for canonical, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in cols_lower:
                    column_map[canonical] = cols_lower[alias]
                    break
            else:
                for alias in aliases:
                    for col_lower, col_original in cols_lower.items():
                        if alias in col_lower:
                            column_map[canonical] = col_original
                            break
                    if canonical in column_map:
                        break

        logger.debug(f'Column mapping: {column_map}')
        return column_map

    def _get_value(self, row, column_map, canonical_name):
        col = column_map.get(canonical_name)
        if col is None:
            return None
        val = row.get(col)
        if pd.isna(val):
            return None
        return val

    def _parse_int(self, value):
        if value is None:
            return None
        try:
            cleaned = str(value).replace(',', '').replace(' ', '').strip()
            if not cleaned or cleaned == '-':
                return None
            return int(float(cleaned))
        except (ValueError, TypeError):
            return None

    def _parse_percentage(self, value):
        if value is None:
            return None
        try:
            cleaned = str(value).strip()
            if not cleaned or cleaned == '-':
                return None
            if '%' in cleaned:
                cleaned = cleaned.replace('%', '').strip()
                return float(cleaned) / 100.0
            val = float(cleaned)
            if val > 1:
                return val / 100.0
            return val
        except (ValueError, TypeError):
            return None

    def _parse_currency(self, value):
        if value is None:
            return None
        try:
            cleaned = str(value).replace('$', '').replace(',', '').strip()
            if not cleaned or cleaned == '-':
                return None
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def _parse_float(self, value):
        if value is None:
            return None
        try:
            cleaned = str(value).replace(',', '').strip()
            if not cleaned or cleaned == '-':
                return None
            return float(cleaned)
        except (ValueError, TypeError):
            return None
