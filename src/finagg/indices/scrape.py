"""Scrape the indices API and store into local SQL tables."""

from sqlalchemy.engine import Engine

from . import api, sql


def run(
    *,
    djia: bool = True,
    sp500: bool = True,
    nasdaq100: bool = True,
    engine: Engine = sql.engine,
    drop_tables: bool = False,
) -> dict[str, int]:
    """Scrape popular ticker data from the indices API.

    Scraped data is loaded into local indices SQL tables.

    Args:
        djia: Whether to scrape DJIA tickers.
        sp500: Whether to scrape S&P 500 tickers.
        nasdaq100: Whether to scrape Nasdaq 100 tickers.
        engine: Custom database engine to use.
        drop_tables: Whether to drop tables before scraping.

    Returns:
        A dictionary mapping indices to number of rows scraped
        for each index

    Raises:
        ValueError if no indices are scraped.

    """
    if not (djia or sp500 or nasdaq100):
        raise ValueError("Need to scrape at least one index.")

    if drop_tables:
        sql.metadata.drop_all(engine)

    sql.metadata.create_all(engine)

    with engine.begin() as conn:
        indices_to_inserts = {"djia": 0, "sp500": 0, "nasdaq100": 0}
        if djia:
            df = api.djia.get()
            count = conn.execute(
                sql.djia.insert(), df.to_dict(orient="records")
            ).rowcount
            indices_to_inserts["djia"] = count

        if sp500:
            df = api.sp500.get()
            count = conn.execute(
                sql.sp500.insert(), df.to_dict(orient="records")
            ).rowcount
            indices_to_inserts["sp500"] = count

        if nasdaq100:
            df = api.nasdaq100.get()
            count = conn.execute(
                sql.nasdaq100.insert(), df.to_dict(orient="records")
            ).rowcount
            indices_to_inserts["nasdaq100"] = count

    return indices_to_inserts
