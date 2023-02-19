"""CLI and tools for yfinance."""

import logging

import click

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Yahoo! finance tools.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Drop and recreate tables, "
        "and scrape the recommended datasets and features into the SQL database."
    ),
)
@click.option(
    "--install-features",
    is_flag=True,
    default=False,
    help="Whether to install features with the recommended datasets.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Log installation errors for each ticker.",
)
def install(install_features: bool = False, verbose: bool = False) -> None:
    if verbose:
        logger.setLevel(logging.DEBUG)
