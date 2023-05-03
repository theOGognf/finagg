"""Refined Yahoo! Finance features (features aggregated from raw tables)."""

import logging

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from ... import backend, utils
from .. import api, sql
from . import _raw

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Daily:
    """Methods for gathering daily stock data features from Yahoo! finance.

    The module variable :data:`finagg.yfinance.feat.daily` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.yfinance.feat.daily.from_api("AAPL").head(5)
        >>> df2 = finagg.yfinance.feat.daily.from_raw("AAPL").head(5)
        >>> df3 = finagg.yfinance.feat.daily.from_refined("AAPL").head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
        >>> pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)

    """

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize daily features columns."""
        df = df.drop(columns=["ticker"]).set_index("date").sort_index()
        df = df.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")
        df = utils.resolve_func_cols(sql.daily, df, drop=True, inplace=True)
        return df.dropna()

    @classmethod
    def from_api(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get daily features directly from :meth:`finagg.yfinance.api.get`.

        Args:
            ticker: Company ticker.
            start: The start date of the stock history. Defaults to the
                first recorded date.
            end: The end date of the stock history. Defaults to the
                last recorded date.

        Returns:
            Daily stock price dataframe sorted by date.

        Examples:
            >>> finagg.yfinance.feat.daily.from_api("AAPL").head(5)  # doctest: +SKIP
                        LOG_CHANGE(open)  LOG_CHANGE(high)  LOG_CHANGE(low)  LOG_CHANGE(close) ...
            date                                                                               ...
            1980-12-15         -0.049005         -0.053343        -0.053581          -0.053581 ...
            1980-12-16         -0.075870         -0.075870        -0.076231          -0.076231 ...
            1980-12-17          0.019512          0.024331         0.024450           0.024450 ...
            1980-12-18          0.028580          0.028445         0.028580           0.028580 ...
            1980-12-19          0.059239          0.058970         0.059239           0.059239 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        df = api.get(ticker, start=start, end=end)
        return cls._normalize(df)

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
        """Get daily features from local SQL tables.

        Args:
            ticker: Company ticker.
            start: The start date of the stock history. Defaults to the
                first recorded date.
            end: The end date of the stock history. Defaults to the
                last recorded date.
            engine: Raw store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Daily stock price dataframe sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                raw SQL table.

        Examples:
            >>> finagg.yfinance.feat.daily.from_raw("AAPL").head(5)  # doctest: +SKIP
                        LOG_CHANGE(open)  LOG_CHANGE(high)  LOG_CHANGE(low)  LOG_CHANGE(close) ...
            date                                                                               ...
            1980-12-15         -0.049005         -0.053343        -0.053581          -0.053581 ...
            1980-12-16         -0.075870         -0.075870        -0.076231          -0.076231 ...
            1980-12-17          0.019512          0.024331         0.024450           0.024450 ...
            1980-12-18          0.028580          0.028445         0.028580           0.028580 ...
            1980-12-19          0.059239          0.058970         0.059239           0.059239 ...

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
            raise NoResultFound(f"No daily rows found for {ticker}.")
        return cls._normalize(df)

    @classmethod
    def from_refined(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from the feature-dedicated local SQL tables.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL tables
        is installed and is up-to-date).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Daily stock price dataframe sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.yfinance.feat.daily.from_refined("AAPL").head(5)  # doctest: +SKIP
                        LOG_CHANGE(open)  LOG_CHANGE(high)  LOG_CHANGE(low)  LOG_CHANGE(close) ...
            date                                                                               ...
            1980-12-15         -0.049005         -0.053343        -0.053581          -0.053581 ...
            1980-12-16         -0.075870         -0.075870        -0.076231          -0.076231 ...
            1980-12-17          0.019512          0.024331         0.024450           0.024450 ...
            1980-12-18          0.028580          0.028445         0.028580           0.028580 ...
            1980-12-19          0.059239          0.058970         0.059239           0.059239 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.daily.name):
            sql.daily.create(engine)
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.daily.select().where(
                        sql.daily.c.ticker == ticker,
                        sql.daily.c.date >= start,
                        sql.daily.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No daily rows found for {ticker}.")
        return df.drop(columns=["ticker"]).set_index("date").sort_index()

    @classmethod
    def get_candidate_ticker_set(
        cls, lb: int = 1, *, engine: None | Engine = None
    ) -> set[str]:
        """Get all unique tickers in the raw SQL table that MAY BE ELIGIBLE
        to be in the feature's refined SQL table.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            All unique tickers that may be valid for creating daily features
            that also have at least ``lb`` rows used for constructing the
            features.

        Examples:
            >>> "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set()  # doctest: +SKIP
            True

        """
        return _raw.Prices.get_ticker_set(lb=lb, engine=engine)

    @classmethod
    def get_ticker_set(cls, lb: int = 1, *, engine: None | Engine = None) -> set[str]:
        """Get all unique tickers in the feature's SQL table.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            All unique tickers that contain all the columns for creating
            daily features that also have at least ``lb`` rows.

        Examples:
            >>> "AAPL" in finagg.yfinance.feat.daily.get_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.daily.name):
            sql.daily.create(engine)
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.daily.c.ticker)
                    .group_by(sql.daily.c.ticker)
                    .having(sa.func.count(sql.daily.c.date) >= lb)
                )
                .scalars()
                .all()
            )
        return set(tickers)

    @classmethod
    def install(
        cls,
        tickers: None | set[str] = None,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install data associated with ``tickers`` by pulling data from the
        raw SQL tables, transforming them into daily features, and then writing
        to the refined daily SQL table.

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
            Number of rows written to the feature's refined SQL table.

        """
        tickers = tickers or cls.get_candidate_ticker_set()
        if not tickers:
            logger.info(
                "Skipping finagg.yfinance.feat.daily installation because no tickers"
                " were provided or no tickers were found with prerequisite data (i.e.,"
                " finagg.yfinance.feat.prices data)"
            )
            return 0

        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.daily.name):
            sql.daily.drop(engine, checkfirst=True)
            sql.daily.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing refined Yahoo! Finance daily data",
            position=0,
            leave=True,
        ):
            try:
                df = cls.from_raw(ticker, engine=engine)
                rowcount = len(df.index)
                if rowcount:
                    cls.to_refined(ticker, df, engine=engine)
                    total_rows += rowcount
                    logger.debug(f"{rowcount} rows inserted for {ticker}")
                else:
                    logger.debug(f"Skipping {ticker} due to missing data")
            except Exception as e:
                logger.debug(f"Skipping {ticker}", exc_info=e)
        return total_rows

    @classmethod
    def to_refined(
        cls,
        ticker: str,
        df: pd.DataFrame,
        /,
        *,
        engine: None | Engine = None,
    ) -> int:
        """Write the given dataframe to the refined feature table
        while using the ticker ``ticker``.

        Args:
            ticker: Company ticker.
            df: Dataframe to store as rows in a local SQL table.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the SQL table.

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.daily.name):
            sql.daily.create(engine)
        df = df.reset_index("date")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.daily.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)
