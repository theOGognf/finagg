"""Scrape the tickers API and store into local SQL tables."""

from sqlalchemy.engine import Engine

from . import api, sql


def run(
    *,
    djia: bool = True,
    sp500: bool = True,
    nasdaq100: bool = True,
    engine: Engine = sql.engine,
) -> dict[str, int]:
    """Scrape popular indices ticker data from the tickers API.

    ALL TABLES ARE DROPPED PRIOR TO SCRAPING!
    Scraped data is loaded into local tickers SQL tables.

    Args:
        djia: Whether to scrape DJIA tickers.
        sp500: Whether to scrape S&P 500 tickers.
        nasdaq100: Whether to scrape Nasdaq 100 tickers.
        engine: Custom database engine to use.

    Returns:
        A dictionary mapping indices to number of rows scraped
        for each index

    Raises:
        ValueError if no indices are scraped.

    """
    if not (djia or sp500 or nasdaq100):
        raise ValueError("Need to scrape at least one index.")

    sql.metadata.drop_all(engine)
    sql.metadata.create_all(engine)

    with engine.connect() as conn:
        indices_to_inserts = {"djia": 0, "sp500": 0, "nasdaq100": 0}
        if djia:
            df = api.djia.get()
            indices_to_inserts["djia"] = len(df.index)
            conn.execute(sql.djia.insert(), df.to_dict(orient="records"))

        if sp500:
            df = api.sp500.get()
            indices_to_inserts["sp500"] = len(df.index)
            conn.execute(sql.sp500.insert(), df.to_dict(orient="records"))

        if nasdaq100:
            df = api.nasdaq100.get()
            indices_to_inserts["nasdaq100"] = len(df.index)
            conn.execute(sql.nasdaq100.insert(), df.to_dict(orient="records"))

    return indices_to_inserts
