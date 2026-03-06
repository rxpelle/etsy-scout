"""Reusable progress bar helpers for Etsy Scout."""

from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    TaskProgressColumn,
)


def create_mining_progress():
    return Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.description}'),
        BarColumn(),
        TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
        TimeRemainingColumn(),
        TextColumn('{task.fields[status]}', style='dim'),
    )


def create_scraping_progress():
    return Progress(
        SpinnerColumn(spinner_name='dots'),
        TextColumn('[bold cyan]{task.description}'),
        BarColumn(),
        TextColumn('({task.completed}/{task.total})'),
        TimeElapsedColumn(),
        TextColumn('{task.fields[status]}', style='dim'),
    )


def create_scoring_progress():
    return Progress(
        SpinnerColumn(spinner_name='line'),
        TextColumn('[bold green]{task.description}'),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn('{task.fields[status]}', style='dim'),
    )
