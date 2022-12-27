"""Scrape the SEC API and store into local SQL tables."""

from typing import Sequence

from sqlalchemy.engine import Engine

from .. import indices
from . import api, features, sql


def run(
    tickers: str | Sequence[str],
    /,
    *,
    concepts: Sequence[dict[str, str]] = features.quarterly_features.concepts,
    engine: Engine = sql.engine,
    drop_tables: bool = False,
) -> dict[str, int]:
    """Scrape company XBRL disclosures from the SEC API.

    Scraped data is loaded into local SEC SQL tables.

    You can specify concepts by specifying tag-taxonomy
    pairs with the `concepts` arg or get all company concepts
    by leaving `concepts` as `None`

    Args:
        tickers: Company tickers to scrape.
        concepts: Taxonomy-tag pairs to scrape. If `None`,
            scrape all concepts.
        engine: Custom database engine to use.
        drop_tables: Whether to drop tables before scraping.

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
                updates.update(indices.api.djia.get_ticker_list())

            case "NASDAQ100":
                updates.update(indices.api.nasdaq100.get_ticker_list())

            case "SP500":
                updates.update(indices.api.sp500.get_ticker_list())
    unique_tickers |= updates

    if drop_tables:
        sql.metadata.drop_all(engine)

    sql.metadata.create_all(engine)

    with engine.connect() as conn:
        tickers_to_inserts = {}
        for ticker in unique_tickers:
            if concepts is None:
                df = api.company_facts.get(ticker=ticker)
                tickers_to_inserts[ticker] = len(df.index)
                conn.execute(sql.tags.insert(), df.to_dict(orient="records"))

            else:
                tickers_to_inserts[ticker] = 0
                for concept in concepts:
                    tag = concept.pop("tag")
                    taxonomy = concept.pop("taxonomy", "us-gaap")
                    units = concept.pop("units", "USD")
                    df = api.company_concept.get(
                        tag, ticker=ticker, taxonomy=taxonomy, units=units
                    )
                    df = features.get_unique_10q(df, units=units)
                    count = conn.execute(
                        sql.tags.insert(), df.to_dict(orient="records")
                    ).rowcount
                    tickers_to_inserts[ticker] += count
    return tickers_to_inserts
