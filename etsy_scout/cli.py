"""Etsy Scout CLI entry point.

Provides the command-line interface using Click and Rich for
keyword research, competitor analysis, ads integration, and reporting.
"""

import sys
import json
import signal
import logging

import click
from rich.console import Console
from rich.table import Table
from rich.progress import (
    Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn,
)
from rich.panel import Panel

from etsy_scout import __version__
from etsy_scout.config import Config
from etsy_scout.db import init_db

console = Console()


def handle_interrupt(signum, frame):
    console.print('\n[yellow]Interrupted. Partial results have been saved.[/yellow]')
    sys.exit(0)


signal.signal(signal.SIGINT, handle_interrupt)


@click.group()
@click.version_option(version=__version__, prog_name='etsy-scout')
def main():
    """Etsy Scout - Etsy keyword research and competitor analysis."""
    Config.setup_logging()


# -- Mine command ----------------------------------------------------------


@main.command()
@click.argument('seed')
@click.option(
    '--depth',
    type=click.IntRange(1, 2),
    default=1,
    help='Mining depth: 1 = seed + a-z (27 queries), 2 = recursive expansion.',
)
def mine(seed, depth):
    """Mine keywords from Etsy autocomplete.

    SEED is the keyword to expand (e.g., "custom mug").

    Examples:
        etsy-scout mine "custom mug"
        etsy-scout mine "vintage jewelry" --depth 2
    """
    from etsy_scout.keyword_engine import mine_keywords

    console.print(
        Panel(
            f'[bold]Seed:[/bold] {seed}\n'
            f'[bold]Depth:[/bold] {depth}',
            title='[bold cyan]Etsy Scout - Keyword Mining[/bold cyan]',
            border_style='cyan',
        )
    )

    expected_queries = 27

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
        TextColumn('({task.completed}/{task.total})'),
        console=console,
    ) as progress:
        task = progress.add_task(
            f'Mining "{seed}"...', total=expected_queries
        )

        def on_progress(completed, total):
            progress.update(task, completed=completed, total=total)

        try:
            results = mine_keywords(
                seed,
                depth=depth,
                progress_callback=on_progress,
            )
        except KeyboardInterrupt:
            console.print(
                '\n[yellow]Mining interrupted. Partial results saved.[/yellow]'
            )
            return
        except Exception as e:
            console.print(f'\n[red]Error during mining: {e}[/red]')
            logging.getLogger(__name__).exception('Mining failed')
            return

    console.print()

    summary_table = Table(show_header=False, box=None, padding=(0, 2))
    summary_table.add_column('Label', style='bold')
    summary_table.add_column('Value', style='green')

    summary_table.add_row('Total keywords mined', str(results['total_mined']))
    summary_table.add_row('New keywords', str(results['new_count']))
    summary_table.add_row('Already in database', str(results['existing_count']))

    console.print(
        Panel(summary_table, title='[bold green]Results Summary[/bold green]', border_style='green')
    )

    if results['keywords']:
        console.print()
        kw_table = Table(
            title='Top Keywords (showing up to 20)',
            show_lines=False,
        )
        kw_table.add_column('#', style='dim', width=4, justify='right')
        kw_table.add_column('Keyword', style='bold')
        kw_table.add_column('Position', justify='center', width=10)
        kw_table.add_column('Status', justify='center', width=10)

        sorted_kws = sorted(results['keywords'], key=lambda x: x[1])
        for i, (kw, pos, is_new) in enumerate(sorted_kws[:20], 1):
            status = '[green]NEW[/green]' if is_new else '[dim]exists[/dim]'
            kw_table.add_row(str(i), kw, str(pos), status)

        console.print(kw_table)

    console.print()
    console.print(f'[dim]Database: {Config.get_db_path()}[/dim]')


# -- Config command group --------------------------------------------------


@main.group()
def config():
    """View and manage configuration."""
    pass


@config.command('show')
def config_show():
    """Show current configuration."""
    cfg = Config.as_dict()

    table = Table(title='Etsy Scout Configuration')
    table.add_column('Setting', style='bold cyan')
    table.add_column('Value')

    for key, value in cfg.items():
        table.add_row(key, str(value))

    console.print(table)


@config.command('init')
def config_init():
    """Initialize configuration and database."""
    console.print('[bold]Initializing Etsy Scout...[/bold]')

    init_db()
    console.print(f'[green]Database created at {Config.get_db_path()}[/green]')

    from pathlib import Path
    env_file = Path(__file__).parent.parent / '.env'
    if not env_file.exists():
        console.print(
            '[yellow]No .env file found. Copy .env.example to .env '
            'and configure your settings.[/yellow]'
        )
    else:
        console.print('[green].env file found[/green]')

    console.print('[bold green]Initialization complete![/bold green]')


# -- Track command group ---------------------------------------------------


@main.group()
def track():
    """Track and monitor competitor listings."""
    pass


@track.command('add')
@click.argument('listing_id')
@click.option('--name', default=None, help='Display name for the listing.')
@click.option('--own', is_flag=True, help='Mark as your own listing.')
def track_add(listing_id, name, own):
    """Add a listing to tracking by Etsy listing ID.

    Examples:
        etsy-scout track add 1234567890 --name "Custom Mug"
        etsy-scout track add 1234567890 --own --name "My Listing"
    """
    from etsy_scout.competitor_engine import CompetitorEngine
    from etsy_scout.collectors.listing_scraper import CaptchaDetected

    engine = CompetitorEngine()
    try:
        console.print(f'\n[bold]Adding listing:[/bold] {listing_id}')
        if name:
            console.print(f'[bold]Name:[/bold] {name}')
        if own:
            console.print(f'[bold]Type:[/bold] [green]Your listing[/green]')
        console.print()

        with console.status('[bold cyan]Scraping Etsy listing page...'):
            result = engine.add_listing(listing_id, name=name, is_own=own)

        if result is None:
            console.print('[red]Failed to add listing. Scraping returned no data.[/red]')
            return

        scraped = result.get('scraped') or {}
        snapshot = result.get('snapshot') or {}
        title = result.get('title') or 'Unknown'
        shop = result.get('shop_name') or 'Unknown'

        lines = [
            f'[bold]Title:[/bold] {title}',
            f'[bold]Shop:[/bold] {shop}',
            f'[bold]Listing ID:[/bold] {result["listing_id"]}',
        ]

        price = snapshot.get('price')
        if price:
            lines.append(f'[bold]Price:[/bold] ${price:.2f}')

        favorites = snapshot.get('favorites')
        if favorites is not None:
            lines.append(f'[bold]Favorites:[/bold] {favorites:,}')

        reviews = snapshot.get('review_count')
        rating = snapshot.get('avg_rating')
        if reviews is not None:
            lines.append(f'[bold]Reviews:[/bold] {reviews:,}')
        if rating is not None:
            lines.append(f'[bold]Rating:[/bold] {rating:.1f}/5.0')

        total_sales = snapshot.get('total_sales')
        if total_sales is not None:
            lines.append(f'[bold]Total Sales:[/bold] {total_sales:,}')

        tags = scraped.get('tags', [])
        if tags:
            lines.append(f'[bold]Tags:[/bold] {", ".join(tags[:10])}')

        status = '[green]NEW - Added to tracking[/green]' if result['is_new'] else '[yellow]Already tracked - Updated[/yellow]'
        lines.append(f'\n[bold]Status:[/bold] {status}')

        border = 'green' if own else 'cyan'
        panel_title = '[bold green]Your Listing[/bold green]' if own else '[bold cyan]Competitor Listing[/bold cyan]'

        console.print(Panel(
            '\n'.join(lines),
            title=panel_title,
            border_style=border,
        ))

    except CaptchaDetected:
        console.print(
            '[red bold]CAPTCHA detected![/red bold] Etsy is blocking scraping.\n'
            '[yellow]Try again in a few minutes, or configure a proxy in .env.[/yellow]'
        )
    except Exception as e:
        console.print(f'[red]Error adding listing: {e}[/red]')
        logging.getLogger(__name__).exception('Failed to add listing')
    finally:
        engine.close()


@track.command('remove')
@click.argument('listing_id')
def track_remove(listing_id):
    """Remove a listing from tracking.

    Example:
        etsy-scout track remove 1234567890
    """
    from etsy_scout.competitor_engine import CompetitorEngine

    engine = CompetitorEngine()
    try:
        removed = engine.remove_listing(listing_id)
        if removed:
            console.print(f'[green]Removed listing {listing_id} from tracking.[/green]')
        else:
            console.print(f'[yellow]Listing {listing_id} not found in tracking.[/yellow]')
    finally:
        engine.close()


@track.command('list')
def track_list():
    """List all tracked listings with latest snapshot data."""
    from etsy_scout.competitor_engine import CompetitorEngine

    engine = CompetitorEngine()
    try:
        listings = engine.list_listings()

        if not listings:
            console.print(
                '[yellow]No listings tracked yet. Use "etsy-scout track add <ID>" to start.[/yellow]'
            )
            return

        table = Table(
            title='Tracked Listings',
            show_lines=True,
            expand=True,
        )
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
            is_own = listing['is_own']
            style = 'bold green' if is_own else ''

            price = f"${listing['price']:.2f}" if listing['price'] else '-'
            favorites = f"{int(listing['favorites']):,}" if listing['favorites'] else '-'
            reviews = f"{int(listing['review_count']):,}" if listing['review_count'] else '-'
            rating = f"{listing['avg_rating']:.1f}" if listing['avg_rating'] else '-'
            sales = f"{int(listing['total_sales']):,}" if listing['total_sales'] else '-'
            updated = (listing['last_snapshot_date'] or '')[:10] or '-'

            title = listing['title'] or 'Unknown'
            if len(title) > 40:
                title = title[:37] + '...'
            if is_own:
                title = f'[bold]{title}[/bold]'

            table.add_row(
                listing['listing_id'],
                title,
                listing['shop_name'] or '-',
                price,
                favorites,
                reviews,
                rating,
                sales,
                updated,
                style=style,
            )

        console.print(table)
        console.print(f'\n[dim]{len(listings)} listing(s) tracked[/dim]')

    finally:
        engine.close()


@track.command('snapshot')
@click.option('--quiet', is_flag=True, help='Suppress output (for cron jobs).')
def track_snapshot(quiet):
    """Take a fresh snapshot of all tracked listings.

    Example:
        etsy-scout track snapshot
    """
    from etsy_scout.competitor_engine import CompetitorEngine

    engine = CompetitorEngine()
    try:
        listings = engine.list_listings()
        if not listings:
            if not quiet:
                console.print('[yellow]No listings tracked.[/yellow]')
            return

        if not quiet:
            console.print(
                f'\n[bold cyan]Taking snapshots of {len(listings)} tracked listing(s)...[/bold cyan]\n'
            )

        results = []
        if quiet:
            results = engine.take_snapshot()
        else:
            with Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                BarColumn(),
                TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
                TextColumn('({task.completed}/{task.total})'),
                console=console,
            ) as progress:
                task = progress.add_task('Snapshotting...', total=len(listings))

                for listing in listings:
                    progress.update(task, description=f'Scraping {listing["listing_id"]}...')
                    listing_results = engine.take_snapshot(listing_id=listing['listing_id'])
                    results.extend(listing_results)
                    progress.advance(task)

        if quiet:
            return

        console.print()
        success_count = sum(1 for r in results if r['success'])
        fail_count = len(results) - success_count

        for result in results:
            if result['success']:
                title = result['title'] or 'Unknown'
                snapshot = result.get('snapshot', {})
                changes = result.get('changes', {})

                favs = snapshot.get('favorites')
                favs_str = f'{favs:,} favs' if favs else 'favs unknown'

                parts = [f'[green]OK[/green] {title} ({result["listing_id"]}) - {favs_str}']

                for field, change in changes.items():
                    old_val = change['old']
                    new_val = change['new']
                    direction = change['direction']

                    if direction == 'improved':
                        color = 'green'
                        arrow = '^'
                    elif direction == 'declined':
                        color = 'red'
                        arrow = 'v'
                    else:
                        color = 'dim'
                        arrow = '='

                    if isinstance(old_val, float):
                        parts.append(f'  [{color}]{arrow} {field}: {old_val:.2f} -> {new_val:.2f}[/{color}]')
                    else:
                        parts.append(f'  [{color}]{arrow} {field}: {old_val:,} -> {new_val:,}[/{color}]')

                console.print('\n'.join(parts))
            else:
                console.print(
                    f'[red]FAIL[/red] {result.get("title", "Unknown")} '
                    f'({result["listing_id"]}): {result.get("error", "Unknown error")}'
                )

        console.print()
        summary = f'[bold]Snapshot complete:[/bold] {success_count} succeeded'
        if fail_count:
            summary += f', [red]{fail_count} failed[/red]'
        console.print(summary)

    finally:
        engine.close()


# -- Import Ads command ----------------------------------------------------


@main.command('import-ads')
@click.argument('filepath', type=click.Path(exists=True))
@click.option(
    '--listing',
    default=None,
    help='Filter by listing title (substring match).',
)
def import_ads(filepath, listing):
    """Import Etsy Ads search term report CSV.

    FILEPATH is the path to the exported CSV file from Etsy Ads dashboard.

    Examples:
        etsy-scout import-ads search-terms.csv
        etsy-scout import-ads report.csv --listing "Custom Mug"
    """
    from etsy_scout.collectors.ads_importer import AdsImporter

    console.print(
        Panel(
            f'[bold]File:[/bold] {filepath}\n'
            f'[bold]Listing filter:[/bold] {listing or "(all listings)"}',
            title='[bold cyan]Etsy Ads Import[/bold cyan]',
            border_style='cyan',
        )
    )

    importer = AdsImporter()
    try:
        with console.status('[bold cyan]Importing search term report...'):
            result = importer.import_csv(filepath, listing_filter=listing)

        summary_table = Table(show_header=False, box=None, padding=(0, 2))
        summary_table.add_column('Label', style='bold')
        summary_table.add_column('Value', style='green')

        summary_table.add_row('Search terms imported', str(result['imported']))
        summary_table.add_row('Rows skipped', str(result['skipped']))
        summary_table.add_row('Keywords enriched', str(result['keywords_enriched']))

        console.print(
            Panel(
                summary_table,
                title='[bold green]Import Summary[/bold green]',
                border_style='green',
            )
        )

        if result['keywords_enriched'] > 0:
            console.print(
                '\n[dim]Tip: Run "etsy-scout score" to recalculate keyword '
                'scores with the new ads data.[/dim]'
            )

    except FileNotFoundError as e:
        console.print(f'[red]File not found: {e}[/red]')
    except ValueError as e:
        console.print(f'[red]Invalid file format: {e}[/red]')
    except Exception as e:
        console.print(f'[red]Error importing: {e}[/red]')
        logging.getLogger(__name__).exception('Ads import failed')
    finally:
        importer.close()


# -- Score command ---------------------------------------------------------


@main.command('score')
@click.option(
    '--recalculate',
    is_flag=True,
    help='Force recalculation of all scores.',
)
def score(recalculate):
    """Score all keywords based on available signals.

    Combines autocomplete position, competition data, and ads performance
    into a composite score for each keyword.

    Examples:
        etsy-scout score
        etsy-scout score --recalculate
    """
    from etsy_scout.keyword_engine import KeywordScorer

    scorer = KeywordScorer()
    try:
        label = 'Rescoring all keywords...' if recalculate else 'Scoring keywords...'
        with console.status(f'[bold cyan]{label}'):
            count = scorer.score_all_keywords(recalculate=recalculate)

        console.print(
            f'[bold green]Scored {count} keywords[/bold green]\n'
        )

        top = scorer.get_top_keywords(limit=10, min_score=0)
        if top:
            table = Table(
                title='Top 10 Keywords by Score',
                show_lines=False,
            )
            table.add_column('#', style='dim', width=4, justify='right')
            table.add_column('Keyword', style='bold', ratio=3)
            table.add_column('Score', justify='right', width=7,
                             style='bold cyan')
            table.add_column('AC Pos', justify='center', width=7)
            table.add_column('Listings', justify='right', width=10)
            table.add_column('Impressions', justify='right', width=12)
            table.add_column('Orders', justify='right', width=8)

            for i, kw in enumerate(top, 1):
                pos = (str(kw['autocomplete_position'])
                       if kw['autocomplete_position'] else '-')
                lc = (f"{kw['listing_count']:,}"
                      if kw['listing_count'] else '-')
                imp = (f"{kw['impressions']:,}"
                       if kw['impressions'] else '-')
                orders = (str(kw['orders'])
                          if kw['orders'] else '-')
                score_val = f"{kw['score']:.0f}" if kw['score'] else '0'

                table.add_row(str(i), kw['keyword'], score_val,
                              pos, lc, imp, orders)

            console.print(table)

        console.print(
            '\n[dim]Run "etsy-scout report keywords" for the full report.[/dim]'
        )

    finally:
        scorer.close()


# -- Report command group --------------------------------------------------


@main.group()
def report():
    """Generate analysis reports."""
    pass


@report.command('keywords')
@click.option('--limit', default=50, help='Maximum keywords to display.')
@click.option('--min-score', default=0, type=float,
              help='Minimum score threshold.')
@click.option('--format', 'output_format',
              type=click.Choice(['table', 'csv', 'json']),
              default='table', help='Output format.')
def report_keywords(limit, min_score, output_format):
    """Show top keywords ranked by score.

    Examples:
        etsy-scout report keywords
        etsy-scout report keywords --limit 100 --min-score 50
        etsy-scout report keywords --format csv > keywords.csv
    """
    from etsy_scout.keyword_engine import KeywordScorer
    from etsy_scout.formatters import OutputFormatter

    scorer = KeywordScorer()
    try:
        keywords = scorer.get_top_keywords(limit=limit, min_score=min_score)

        if not keywords:
            console.print('[yellow]No keywords found. Run "etsy-scout mine" first.[/yellow]')
            return

        formatter = OutputFormatter(output_format)
        formatter.format_keywords(keywords, title=f'Top {limit} Keywords (min score: {min_score})')

        if output_format == 'table':
            console.print(f'\n[dim]{len(keywords)} keywords shown[/dim]')

    finally:
        scorer.close()


@report.command('competitors')
def report_competitors():
    """Show competitor comparison report."""
    from etsy_scout.competitor_engine import CompetitorEngine
    from etsy_scout.formatters import OutputFormatter

    engine = CompetitorEngine()
    try:
        listings = engine.list_listings()
        if not listings:
            console.print('[yellow]No listings tracked.[/yellow]')
            return

        formatter = OutputFormatter('table')
        formatter.format_listings(listings)
    finally:
        engine.close()


@report.command('ads')
def report_ads():
    """Show Etsy Ads search term performance report."""
    from etsy_scout.db import AdsRepository, init_db

    init_db()
    repo = AdsRepository()
    try:
        terms = repo.get_aggregated_search_terms()
        if not terms:
            console.print(
                '[yellow]No ads data. Import with "etsy-scout import-ads <file>".[/yellow]'
            )
            return

        table = Table(
            title='Etsy Ads Performance (Aggregated)',
            show_lines=False,
        )
        table.add_column('#', style='dim', width=4, justify='right')
        table.add_column('Search Term', style='bold', ratio=3)
        table.add_column('Impressions', justify='right', width=12)
        table.add_column('Clicks', justify='right', width=8)
        table.add_column('CTR', justify='right', width=7)
        table.add_column('Spend', justify='right', width=9)
        table.add_column('Revenue', justify='right', width=9)
        table.add_column('Orders', justify='right', width=8)
        table.add_column('ROAS', justify='right', width=7)

        for i, term in enumerate(terms[:50], 1):
            imp = f"{term['total_impressions']:,}" if term['total_impressions'] else '-'
            clicks = f"{term['total_clicks']:,}" if term['total_clicks'] else '-'
            ctr = f"{term['avg_ctr']:.1%}" if term['avg_ctr'] else '-'
            spend = f"${term['total_spend']:.2f}" if term['total_spend'] else '-'
            rev = f"${term['total_revenue']:.2f}" if term['total_revenue'] else '-'
            orders = str(term['total_orders']) if term['total_orders'] else '-'
            roas = f"{term['avg_roas']:.1f}x" if term['avg_roas'] else '-'

            table.add_row(
                str(i), term['search_term'],
                imp, clicks, ctr, spend, rev, orders, roas,
            )

        console.print(table)
        console.print(f'\n[dim]{len(terms)} search terms total[/dim]')

    finally:
        repo.close()


@report.command('gaps')
def report_gaps():
    """Show keyword gap analysis (impressions but no orders)."""
    from etsy_scout.db import AdsRepository, init_db

    init_db()
    repo = AdsRepository()
    try:
        gaps = repo.get_opportunity_keywords()
        if not gaps:
            console.print('[yellow]No gap keywords found.[/yellow]')
            return

        table = Table(
            title='Keyword Gaps (Impressions, No Orders)',
            show_lines=False,
        )
        table.add_column('#', style='dim', width=4, justify='right')
        table.add_column('Search Term', style='bold', ratio=3)
        table.add_column('Impressions', justify='right', width=12)
        table.add_column('Clicks', justify='right', width=8)
        table.add_column('Spend', justify='right', width=9)

        for i, gap in enumerate(gaps[:30], 1):
            imp = f"{gap['total_impressions']:,}" if gap['total_impressions'] else '-'
            clicks = f"{gap['total_clicks']:,}" if gap['total_clicks'] else '-'
            spend = f"${gap['total_spend']:.2f}" if gap['total_spend'] else '-'

            table.add_row(str(i), gap['search_term'], imp, clicks, spend)

        console.print(table)
        console.print(
            f'\n[dim]{len(gaps)} gap keywords total. '
            f'Consider adding these to negative keywords or optimizing listings.[/dim]'
        )

    finally:
        repo.close()


# -- Export command group --------------------------------------------------


@main.group()
def export():
    """Export keywords for Etsy SEO and Ads."""
    pass


@export.command('tags')
@click.option('--min-score', default=0, type=float,
              help='Minimum keyword score to include.')
@click.option('--limit', default=13, type=int,
              help='Number of tags (Etsy allows 13 per listing).')
def export_tags(min_score, limit):
    """Generate optimized Etsy listing tags.

    Packs the highest-scoring keywords into tag slots (Etsy allows
    13 tags per listing, max 20 characters each).

    Examples:
        etsy-scout export tags
        etsy-scout export tags --min-score 50
    """
    from etsy_scout.keyword_engine import KeywordScorer

    scorer = KeywordScorer()
    try:
        keywords = scorer.get_top_keywords(limit=100, min_score=min_score)

        if not keywords:
            console.print('[yellow]No keywords found. Run "etsy-scout mine" first.[/yellow]')
            return

        # Filter to tags that fit Etsy's 20-character limit
        tags = []
        for kw in keywords:
            keyword = kw['keyword']
            if len(keyword) <= 20 and keyword not in tags:
                tags.append(keyword)
                if len(tags) >= limit:
                    break

        console.print(
            Panel(
                f'[bold]Tags ({len(tags)}/{limit}):[/bold]\n\n' +
                '\n'.join(f'  {i}. {tag}' for i, tag in enumerate(tags, 1)),
                title='[bold green]Etsy Listing Tags[/bold green]',
                border_style='green',
            )
        )

        # Also output comma-separated for easy copy
        console.print(f'\n[bold]Copy-paste:[/bold]')
        console.print(', '.join(tags))

        long_keywords = [kw['keyword'] for kw in keywords
                        if len(kw['keyword']) > 20][:5]
        if long_keywords:
            console.print(
                f'\n[dim]Skipped (>20 chars): {", ".join(long_keywords)}[/dim]'
            )

    finally:
        scorer.close()


@export.command('ads')
@click.option('--min-score', default=0, type=float,
              help='Minimum keyword score to include.')
@click.option('--format', 'output_format',
              type=click.Choice(['csv']),
              default='csv', help='Output format.')
def export_ads(min_score, output_format):
    """Export keywords formatted for Etsy Ads campaigns.

    Examples:
        etsy-scout export ads
        etsy-scout export ads --min-score 50 > keywords.csv
    """
    from etsy_scout.keyword_engine import KeywordScorer
    from etsy_scout.formatters import OutputFormatter

    scorer = KeywordScorer()
    try:
        keywords = scorer.get_top_keywords(limit=500, min_score=min_score)

        if not keywords:
            console.print('[yellow]No keywords found.[/yellow]')
            return

        formatter = OutputFormatter('csv')
        formatter.format_keywords(keywords, title='Etsy Ads Keywords')

    finally:
        scorer.close()


# -- Reverse Listing command -----------------------------------------------


@main.command('reverse')
@click.argument('listing_id')
@click.option(
    '--top',
    'top_n',
    type=int,
    default=None,
    help='Only check top N keywords by score.',
)
def reverse(listing_id, top_n):
    """Reverse listing lookup: find keywords a listing ranks for.

    LISTING_ID is the Etsy listing ID to look up.

    Searches Etsy for each keyword in your database and checks if the
    listing appears in results (free, but slow ~2s/keyword).

    Examples:
        etsy-scout reverse 1234567890
        etsy-scout reverse 1234567890 --top 50
    """
    from etsy_scout.keyword_engine import ReverseListing

    engine = ReverseListing()
    try:
        from etsy_scout.db import KeywordRepository, init_db
        init_db()
        repo = KeywordRepository()
        try:
            total_kws = len(repo.get_all_keywords(active_only=True))
        finally:
            repo.close()

        check_count = min(top_n, total_kws) if top_n else total_kws
        est_seconds = check_count * 2.5
        est_minutes = est_seconds / 60

        panel_lines = [
            f'[bold]Listing ID:[/bold] {listing_id}',
            f'[bold]Keywords in DB:[/bold] {total_kws}',
            f'[bold]Estimated time:[/bold] ~{est_minutes:.1f} minutes '
            f'({check_count} keywords x 2.5s)',
        ]

        console.print(
            Panel(
                '\n'.join(panel_lines),
                title='[bold cyan]Reverse Listing Lookup[/bold cyan]',
                border_style='cyan',
            )
        )
        console.print()

        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
            TextColumn('({task.completed}/{task.total})'),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task('Probing...', total=check_count)

            def on_progress(completed, total, found, keyword):
                short_kw = keyword[:30] + '...' if len(keyword) > 30 else keyword
                progress.update(
                    task,
                    completed=completed,
                    total=total,
                    description=f'Probing: "{short_kw}" (found: {found})',
                )

            try:
                results = engine.reverse_listing_probe(
                    listing_id, top_n=top_n,
                    progress_callback=on_progress,
                )
            except KeyboardInterrupt:
                console.print(
                    '\n[yellow]Interrupted. Partial results saved.[/yellow]'
                )
                return

        console.print()

        if not results:
            console.print(
                f'[yellow]No rankings found for listing {listing_id}.[/yellow]\n'
                '[dim]The listing may not appear in the first page of results '
                'for any keywords in your database.[/dim]'
            )
            return

        results.sort(key=lambda x: x['position'])

        table = Table(
            title=f'Keywords Ranking for Listing {listing_id}',
            show_lines=False,
        )
        table.add_column('#', style='dim', width=4, justify='right')
        table.add_column('Keyword', style='bold', ratio=3)
        table.add_column('Position', justify='center', width=10)
        table.add_column('Date', width=12)

        for i, result in enumerate(results, 1):
            pos = result['position']
            if pos <= 3:
                pos_str = f'[bold green]{pos}[/bold green]'
            elif pos <= 8:
                pos_str = f'[green]{pos}[/green]'
            elif pos <= 16:
                pos_str = f'[yellow]{pos}[/yellow]'
            else:
                pos_str = str(pos)

            table.add_row(
                str(i),
                result['keyword'],
                pos_str,
                result['snapshot_date'],
            )

        console.print(table)
        console.print(
            f'\n[bold green]{len(results)} keywords found[/bold green] '
            f'for listing {listing_id}'
        )

    except Exception as e:
        console.print(f'[red]Error during reverse listing lookup: {e}[/red]')
        logging.getLogger(__name__).exception('Reverse listing failed')
    finally:
        engine.close()


if __name__ == '__main__':
    main()
