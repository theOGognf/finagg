"""Raw features from Yahoo! Finance sources."""

import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from ... import backend, indices, utils
from .. import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Prices:
    """Get a single company's daily stock history as-is from raw Yahoo! Finance
    data.

    The module variable :data:`finagg.yfinance.feat.prices` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get a single company's daily stock history as-is from raw
        Yahoo! Finance SQL tables.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            A dataframe containing the company's daily stock history
            across the specified period.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the raw
                SQL table.

        Examples:
            >>> finagg.yfinance.feat.prices.from_raw("AAPL").head(5)  # doctest: +SKIP
                          open    high     low   close      volume
            date
            1980-12-12  0.0997  0.1002  0.0997  0.0997  4.6903e+08
            1980-12-15  0.0950  0.0950  0.0945  0.0945  1.7588e+08
            1980-12-16  0.0880  0.0880  0.0876  0.0876  1.0573e+08
            1980-12-17  0.0897  0.0902  0.0897  0.0897  8.6442e+07
            1980-12-18  0.0924  0.0928  0.0924  0.0924  7.3450e+07

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.prices.name):
            sql.prices.create(engine)
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.prices.select().where(
                        sql.prices.c.ticker == ticker,
                        sql.prices.c.date >= start,
                        sql.prices.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No rows found for {ticker}.")
        return df.drop(columns=["ticker"]).set_index("date").sort_index()

    @classmethod
    def get_ticker_set(cls, lb: int = 1, *, engine: None | Engine = None) -> set[str]:
        """Get all unique ticker symbols in the raw SQL tables that have at least
        ``lb`` rows.

        This method is convenient for accessing the tickers that have raw SQL data
        associated with them so the data associated with those tickers can be
        further refined. A common pattern is to use this method and other
        ``get_ticker_set`` methods (such as those found in :mod:`finagg.yfinance.feat`)
        to determine which tickers are missing data from other tables or features.

        Args:
            lb: Lower bound number of rows that a company must have for its ticker
                to be included in the set returned by this method.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Examples:
            >>> "AAPL" in finagg.yfinance.feat.prices.get_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.prices.name):
            sql.prices.create(engine)
        with engine.begin() as conn:
            tickers = set(
                conn.execute(
                    sa.select(sql.prices.c.ticker)
                    .group_by(sql.prices.c.ticker)
                    .having(sa.func.count(sql.prices.c.date) >= lb)
                )
                .scalars()
                .all()
            )
        return tickers

    @classmethod
    def install(
        cls,
        tickers: None | set[str] = None,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install data associated with ``tickers`` by pulling data from the
        API, and then writing the data to the raw prices SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.indices.api.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's raw SQL table.

        """
        tickers = tickers or indices.api.get_ticker_set()
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.prices.name):
            sql.prices.drop(engine, checkfirst=True)
            sql.prices.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing raw Yahoo! Finance stock data",
            position=0,
            leave=True,
        ):
            try:
                df = api.get(ticker, interval="1d", period="max")
                rowcount = len(df.index)
                if rowcount:
                    cls.to_raw(df, engine=engine)
                    total_rows += rowcount
                    logger.debug(f"{rowcount} rows inserted for {ticker}")
                else:
                    logger.debug(f"Skipping {ticker} due to missing stock data")
            except Exception as e:
                logger.debug(f"Skipping {ticker}", exc_info=e)
        return total_rows

    @classmethod
    def to_raw(cls, df: pd.DataFrame, /, *, engine: None | Engine = None) -> int:
        """Write the given dataframe to the raw feature table.

        Args:
            df: Dataframe to store as rows in a local SQL table
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the SQL table.

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.prices.name):
            sql.prices.create(engine)
        with engine.begin() as conn:
            conn.execute(sql.prices.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)
