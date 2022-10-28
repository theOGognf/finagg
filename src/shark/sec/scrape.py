"""Scrape the SEC API and store into local SQL tables."""

from typing import Callable

import pandas as pd

from .api import api
from .sql import engine, metadata, tags

_TRANSFORMER = Callable[
    [
        pd.DataFrame,
    ],
    pd.DataFrame,
]


def get_unique_10q(df: pd.DataFrame, /, *, units: str = "USD") -> pd.DataFrame:
    """Get all unique rows as determined by the
    accession number (`accn`) and tag for each quarter.

    Args:
        df: Dataframe without unique rows.
        units: Only keep rows with units `units`.

    Returns:
        Dataframe with unique rows measured in `units`.

    """
    df = df[(df["form"] == "10-Q") & (df["units"] == units)]
    df = df.drop_duplicates(["fy", "fp", "tag"])
    return df


def scrape(
    tickers: str | list[str],
    concepts: None | list[tuple[str, str]] = None,
    transformer: None | _TRANSFORMER = get_unique_10q,
) -> dict[str, int]:
    """Scrape company XBRL disclosures from the SEC API.

    ALL ROWS ARE DROPPED PRIOR TO SCRAPING!
    Scraped data is loaded into local SEC SQL tables.

    Dataframes can optionally be transformed by the callable
    `transformer` prior to being inserted into
    the SQL tables. The default transformer only scrapes
    unique rows corresponding to the quarterly 10-Q form.

    Args:
        tickers: Company tickers to scrape.
        concepts: Taxonomy-tag pairs to scrape. If `None`,
            scrape all concepts.
        transformer: Dataframe transformer callable.
            Called immediately prior to inserting the dataframe
            into the local SEC SQL table.
            By default, only quarterly unique rows
            (as determined by a submission's `accn` and tag
            pair) are inserted. If left as `False` or `None`,
            the transformer is just a passthrough.

    Returns:
        A dictionary mapping tickers to number of rows scraped
        for each ticker.

    """
    if isinstance(tickers, str):
        tickers = [tickers]

    if not transformer:
        transformer = lambda x: x

    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.connect() as conn:
        tickers_to_inserts = {}
        for ticker in tickers:
            if concepts is None:
                df = api.company_facts.get(ticker=ticker)
                df = transformer(df)
                tickers_to_inserts[ticker] = len(df.index)
                conn.execute(tags.insert(), df.to_dict(orient="records"))

            else:
                tickers_to_inserts[ticker] = 0
                for taxonomy, tag in concepts:
                    df = api.company_concept.get(tag, taxonomy=taxonomy, ticker=ticker)
                    df = transformer(df)
                    tickers_to_inserts[ticker] += len(df.index)
                    conn.execute(tags.insert(), df.to_dict(orient="records"))
    return tickers_to_inserts
