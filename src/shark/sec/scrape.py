"""Scrape the SEC API and store into local SQL tables."""

from typing import Callable

import pandas as pd

from ..tickers import api as tickers_api
from .api import api
from .sql import engine, metadata
from .sql import tags as tags_table

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
    df = df.drop_duplicates(["accn", "tag"])
    return df


def scrape(
    tickers: str | list[str],
    concepts: None | list[tuple[str, str]] = None,
    tags: None | list[str] = None,
    taxonomy: None | str = None,
    transformer: None | _TRANSFORMER = get_unique_10q,
) -> dict[str, int]:
    """Scrape company XBRL disclosures from the SEC API.

    ALL TABLES ARE DROPPED PRIOR TO SCRAPING!
    Scraped data is loaded into local SEC SQL tables.

    You can specify concepts by specifying tag-taxonomy
    pairs with the `concepts` arg or by specifying
    many tags with one taxonomy using the `tags` and
    `taxonomy` args.

    Dataframes can optionally be transformed by the callable
    `transformer` prior to being inserted into
    the SQL tables. The default transformer only scrapes
    unique rows corresponding to the quarterly 10-Q form.

    Args:
        tickers: Company tickers to scrape.
        concepts: Taxonomy-tag pairs to scrape. If `None`,
            scrape all concepts.
        tags: Tags to scrape. Mutually exclusive with the
            `concepts` arg.
        taxonomy: Taxonomy to scrape with `tags`. Mutually
            exclusive with the `concepts` arg.
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

    updates = set()
    unique_tickers = set(tickers)
    for ticker in unique_tickers:
        match ticker.upper():
            case "DJIA":
                updates.update(tickers_api.djia.get_ticker_list())

            case "NASDAQ100":
                updates.update(tickers_api.nasdaq100.get_ticker_list())

            case "SP500":
                updates.update(tickers_api.sp500.get_ticker_list())
    unique_tickers |= updates

    if tags and taxonomy:
        if concepts:
            raise ValueError(
                "The `concepts` and `tags` + `taxonomy` " "args are mutually exclusive."
            )

        concepts = []
        for tag in tags:
            concepts.append((taxonomy, tag))

    if not transformer:
        transformer = lambda x: x

    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.connect() as conn:
        tickers_to_inserts = {}
        for ticker in unique_tickers:
            if concepts is None:
                df = api.company_facts.get(ticker=ticker)
                df = transformer(df)
                tickers_to_inserts[ticker] = len(df.index)
                conn.execute(tags_table.insert(), df.to_dict(orient="records"))

            else:
                tickers_to_inserts[ticker] = 0
                for taxonomy, tag in concepts:
                    df = api.company_concept.get(tag, taxonomy=taxonomy, ticker=ticker)
                    df = transformer(df)
                    tickers_to_inserts[ticker] += len(df.index)
                    conn.execute(tags_table.insert(), df.to_dict(orient="records"))
    return tickers_to_inserts
