"""Output formatters for Etsy Scout.

Provides consistent formatting across table, CSV, and JSON output
modes for keywords, listings, and ranking data.
"""

import csv
import io
import json
import logging

from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)
console = Console()


class OutputFormatter:
    """Formats data in table, CSV, or JSON output modes."""

    def __init__(self, output_format='table'):
        if output_format not in ('table', 'csv', 'json'):
            raise ValueError(
                f'Unknown format "{output_format}". '
                f'Must be one of: table, csv, json'
            )
        self.format = output_format

    def format_keywords(self, keywords, title='Keywords'):
        if self.format == 'json':
            return self._keywords_json(keywords)
        elif self.format == 'csv':
            return self._keywords_csv(keywords)
        else:
            self._keywords_table(keywords, title)
            return None

    def format_listings(self, listings, title='Tracked Listings'):
        if self.format == 'json':
            return self._listings_json(listings)
        elif self.format == 'csv':
            return self._listings_csv(listings)
        else:
            self._listings_table(listings, title)
            return None

    # -- Keyword formatters --

    def _keywords_json(self, keywords):
        data = []
        for i, kw in enumerate(keywords, 1):
            data.append({
                'rank': i,
                'keyword': _get(kw, 'keyword'),
                'score': _get(kw, 'score') or 0,
                'autocomplete_position': _get(kw, 'autocomplete_position'),
                'listing_count': _get(kw, 'listing_count'),
                'impressions': _get(kw, 'impressions'),
                'clicks': _get(kw, 'clicks'),
                'orders': _get(kw, 'orders'),
                'source': _get(kw, 'source'),
            })
        output = json.dumps(data, indent=2)
        print(output)
        return output

    def _keywords_csv(self, keywords):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'Rank', 'Keyword', 'Score', 'Autocomplete Position',
            'Listings', 'Impressions', 'Clicks', 'Orders', 'Source',
        ])
        for i, kw in enumerate(keywords, 1):
            writer.writerow([
                i,
                _get(kw, 'keyword'),
                _get(kw, 'score') or 0,
                _get(kw, 'autocomplete_position') or '',
                _get(kw, 'listing_count') or '',
                _get(kw, 'impressions') or '',
                _get(kw, 'clicks') or '',
                _get(kw, 'orders') or '',
                _get(kw, 'source') or '',
            ])
        content = output.getvalue()
        print(content, end='')
        return content

    def _keywords_table(self, keywords, title):
        table = Table(title=title, show_lines=False)
        table.add_column('#', style='dim', width=4, justify='right')
        table.add_column('Keyword', style='bold', min_width=20, no_wrap=False)
        table.add_column('Score', justify='right', width=7, style='bold cyan')
        table.add_column('AC Pos', justify='center', width=7)
        table.add_column('Listings', justify='right', width=10)
        table.add_column('Impressions', justify='right', width=12)
        table.add_column('Clicks', justify='right', width=8)
        table.add_column('Orders', justify='right', width=8)
        table.add_column('Source', justify='center', width=14)

        for i, kw in enumerate(keywords, 1):
            score_val = _get(kw, 'score') or 0
            score_str = f'{score_val:.0f}'

            if score_val >= 100:
                score_str = f'[bold green]{score_str}[/bold green]'
            elif score_val >= 75:
                score_str = f'[green]{score_str}[/green]'
            elif score_val >= 50:
                score_str = f'[yellow]{score_str}[/yellow]'
            elif score_val >= 25:
                score_str = f'[dim]{score_str}[/dim]'

            pos = _get(kw, 'autocomplete_position')
            lc = _get(kw, 'listing_count')
            imp = _get(kw, 'impressions')
            clicks = _get(kw, 'clicks')
            orders = _get(kw, 'orders')

            table.add_row(
                str(i),
                _get(kw, 'keyword') or '',
                score_str,
                str(pos) if pos else '-',
                f'{lc:,}' if lc else '-',
                f'{imp:,}' if imp else '-',
                f'{clicks:,}' if clicks else '-',
                str(orders) if orders else '-',
                _get(kw, 'source') or '-',
            )

        console.print(table)

    # -- Listing formatters --

    def _listings_json(self, listings):
        data = []
        for listing in listings:
            data.append({
                'listing_id': _get(listing, 'listing_id'),
                'title': _get(listing, 'title'),
                'shop_name': _get(listing, 'shop_name'),
                'is_own': bool(_get(listing, 'is_own')),
                'price': _get(listing, 'price'),
                'favorites': _get(listing, 'favorites'),
                'review_count': _get(listing, 'review_count'),
                'avg_rating': _get(listing, 'avg_rating'),
                'total_sales': _get(listing, 'total_sales'),
            })
        output = json.dumps(data, indent=2)
        print(output)
        return output

    def _listings_csv(self, listings):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'Listing ID', 'Title', 'Shop', 'Own', 'Price',
            'Favorites', 'Reviews', 'Rating', 'Total Sales',
        ])
        for listing in listings:
            writer.writerow([
                _get(listing, 'listing_id'),
                _get(listing, 'title') or '',
                _get(listing, 'shop_name') or '',
                'Yes' if _get(listing, 'is_own') else 'No',
                _get(listing, 'price') or '',
                _get(listing, 'favorites') or '',
                _get(listing, 'review_count') or '',
                _get(listing, 'avg_rating') or '',
                _get(listing, 'total_sales') or '',
            ])
        content = output.getvalue()
        print(content, end='')
        return content

    def _listings_table(self, listings, title):
        table = Table(title=title, show_lines=True, expand=True)
        table.add_column('Listing ID', width=12, no_wrap=True)
        table.add_column('Title', ratio=3, no_wrap=False)
        table.add_column('Shop', ratio=1)
        table.add_column('Price', justify='right', width=8)
        table.add_column('Favs', justify='right', width=8)
        table.add_column('Reviews', justify='right', width=8)
        table.add_column('Rating', justify='center', width=6)
        table.add_column('Sales', justify='right', width=9)
        table.add_column('Updated', width=10)

        for listing in listings:
            is_own = _get(listing, 'is_own')
            style = 'bold green' if is_own else ''

            price = _get(listing, 'price')
            favorites = _get(listing, 'favorites')
            reviews = _get(listing, 'review_count')
            rating = _get(listing, 'avg_rating')
            sales = _get(listing, 'total_sales')
            updated = (_get(listing, 'last_snapshot_date') or '')[:10] or '-'

            title_text = _get(listing, 'title') or 'Unknown'
            if len(title_text) > 50:
                title_text = title_text[:47] + '...'
            if is_own:
                title_text = f'[bold]{title_text}[/bold]'

            table.add_row(
                _get(listing, 'listing_id') or '',
                title_text,
                _get(listing, 'shop_name') or '-',
                f'${price:.2f}' if price else '-',
                f'{int(favorites):,}' if favorites else '-',
                f'{int(reviews):,}' if reviews else '-',
                f'{rating:.1f}' if rating else '-',
                f'{int(sales):,}' if sales else '-',
                updated,
                style=style,
            )

        console.print(table)


def _get(obj, key):
    """Safely get a value from a dict-like object."""
    try:
        return obj[key]
    except (KeyError, IndexError, TypeError):
        try:
            return getattr(obj, key, None)
        except Exception:
            return None
