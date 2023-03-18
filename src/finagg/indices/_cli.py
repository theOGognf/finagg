"""CLI and tools for aggregating tickers in common indices."""

import logging

import click

from .. import backend
from . import api as _api
from . import sql as _sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Tools for managing popular indices's ticker data.")
def entry_point() -> None:
    ...


@entry_point.command(
    help=(
        "Drop and recreate tables, and install popular indices's ticker data "
        "into the SQL database."
    ),
)
@click.option(
    "--all",
    "-a",
    "all_",
    is_flag=True,
    default=False,
    help="Whether to install all defined tables.",
)
@click.option(
    "--djia",
    is_flag=True,
    default=False,
    help="Whether to install the DJIA ticker table.",
)
@click.option(
    "--djia",
    is_flag=True,
    default=False,
    help="Whether to install the DJIA ticker table.",
)
@click.option(
    "--sp500",
    is_flag=True,
    default=False,
    help="Whether to install the S&P 500 ticker table.",
)
@click.option(
    "--nasdaq100",
    is_flag=True,
    default=False,
    help="Whether to install the Nasdaq 100 ticker table.",
)
def install(
    all_: bool = False,
    djia: bool = False,
    sp500: bool = False,
    nasdaq100: bool = False,
) -> int:
    total_rows = 0
    with backend.engine.begin() as conn:
        if all_ or djia:
            _sql.djia.drop(conn, checkfirst=True)
            _sql.djia.create(conn)

            df = _api.djia.get()
            rowcount = len(df.index)
            conn.execute(
                _sql.djia.insert(), df.to_dict(orient="records")  # type: ignore[arg-type]
            )
            logger.info(f"Inserted {rowcount} rows into the DJIA table")
            total_rows += rowcount

        if all_ or sp500:
            _sql.sp500.drop(conn, checkfirst=True)
            _sql.sp500.create(conn)

            df = _api.sp500.get()
            rowcount = len(df.index)
            conn.execute(
                _sql.sp500.insert(), df.to_dict(orient="records")  # type: ignore[arg-type]
            )
            logger.info(f"Inserted {rowcount} rows into the S&P 500 table")
            total_rows += rowcount

        if all_ or nasdaq100:
            _sql.nasdaq100.drop(conn, checkfirst=True)
            _sql.nasdaq100.create(conn)

            df = _api.nasdaq100.get()
            rowcount = len(df.index)
            conn.execute(
                _sql.nasdaq100.insert(), df.to_dict(orient="records")  # type: ignore[arg-type]
            )
            logger.info(f"Inserted {rowcount} rows into the Nasdaq 100 table")
            total_rows += rowcount

    if all_ or djia or sp500 or nasdaq100:
        logger.info(f"{total_rows} total rows inserted for {__package__}")
    else:
        logger.info(
            f"Skipping {__package__} installation because no installation "
            "options are provided"
        )

    return total_rows
