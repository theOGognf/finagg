"""Features from :mod:`yfinance` sources."""

import logging

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from .. import backend, feat, indices, utils
from . import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class RefinedDaily(feat.Features):
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

    #: Columns within this feature set. Dataframes returned by this class's
    #: methods will always contain these columns. The refined data SQL table
    #: corresponding to these features will also have rows that have these
    #: names.
    columns = [
        "price",
        "open_pct_change",
        "high_pct_change",
        "low_pct_change",
        "close_pct_change",
        "volume_pct_change",
    ]

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize daily features columns."""
        df = df.drop(columns=["ticker"]).set_index("date").astype(float).sort_index()
        df["price"] = df["close"]
        df = df.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")
        df[cls.pct_change_target_columns()] = df[cls.pct_change_source_columns()].apply(
            utils.safe_pct_change
        )
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
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
            >>> finagg.yfinance.feat.daily.from_api("AAPL").head(5)  # doctest: +NORMALIZE_WHITESPACE
                         price  open_pct_change  high_pct_change  low_pct_change  close_pct_change  volume_pct_change
            date
            1980-12-15  0.0945          -0.0478          -0.0519         -0.0522           -0.0522            -0.6250
            1980-12-16  0.0876          -0.0731          -0.0731         -0.0734           -0.0734            -0.3989
            1980-12-17  0.0897           0.0197           0.0246          0.0248            0.0248            -0.1824
            1980-12-18  0.0924           0.0290           0.0289          0.0290            0.0290            -0.1503
            1980-12-19  0.0980           0.0610           0.0607          0.0610            0.0610            -0.3379

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
            >>> finagg.yfinance.feat.daily.from_raw("AAPL").head(5)  # doctest: +NORMALIZE_WHITESPACE
                         price  open_pct_change  high_pct_change  low_pct_change  close_pct_change  volume_pct_change
            date
            1980-12-15  0.0945          -0.0478          -0.0519         -0.0522           -0.0522            -0.6250
            1980-12-16  0.0876          -0.0731          -0.0731         -0.0734           -0.0734            -0.3989
            1980-12-17  0.0897           0.0197           0.0246          0.0248            0.0248            -0.1824
            1980-12-18  0.0924           0.0290           0.0289          0.0290            0.0290            -0.1503
            1980-12-19  0.0980           0.0610           0.0607          0.0610            0.0610            -0.3379

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
            >>> finagg.yfinance.feat.daily.from_refined("AAPL").head(5)  # doctest: +NORMALIZE_WHITESPACE
                         price  open_pct_change  high_pct_change  low_pct_change  close_pct_change  volume_pct_change
            date
            1980-12-15  0.0945          -0.0478          -0.0519         -0.0522           -0.0522            -0.6250
            1980-12-16  0.0876          -0.0731          -0.0731         -0.0734           -0.0734            -0.3989
            1980-12-17  0.0897           0.0197           0.0246          0.0248            0.0248            -0.1824
            1980-12-18  0.0924           0.0290           0.0289          0.0290            0.0290            -0.1503
            1980-12-19  0.0980           0.0610           0.0607          0.0610            0.0610            -0.3379

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        df = df.pivot(index="date", columns="name", values="value").sort_index()
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df

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
            >>> "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set()
            True

        """
        return sql.get_ticker_set(lb=lb, engine=engine)

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
            >>> "AAPL" in finagg.yfinance.feat.daily.get_ticker_set()
            True

        """
        engine = engine or backend.engine
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.daily.c.ticker)
                    .group_by(sql.daily.c.ticker)
                    .having(
                        *[
                            sa.func.count(sql.daily.c.name == col) >= lb
                            for col in cls.columns
                        ]
                    )
                )
                .scalars()
                .all()
            )
        return set(tickers)

    @classmethod
    def install(
        cls, tickers: None | set[str] = None, *, engine: None | Engine = None
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from the raw SQL table.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.indices.api.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's refined SQL table.

        """
        tickers = tickers or cls.get_candidate_ticker_set()
        engine = engine or backend.engine
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

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        engine = engine or backend.engine
        df = df.reset_index("date")
        if set(df.columns) < set(cls.columns):
            raise ValueError(
                f"Dataframe must have columns {cls.columns} but got {df.columns}"
            )
        df = df.melt("date", var_name="name", value_name="value")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.daily.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class RawPrices:
    """Get a single company's daily stock history as-is from raw Yahoo! Finance
    data.

    The module variable :data:`finagg.yfinance.feat.prices` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def install(
        cls,
        tickers: None | set[str] = None,
        *,
        engine: None | Engine = None,
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        as-is using Yahoo! Finance.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.indices.api.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or indices.api.get_ticker_set()
        engine = engine or backend.engine
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
            >>> finagg.yfinance.feat.prices.from_raw("AAPL").head(5)  # doctest: +NORMALIZE_WHITESPACE
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
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sa.select(
                        sql.prices.c.date,
                        sql.prices.c.open,
                        sql.prices.c.high,
                        sql.prices.c.low,
                        sql.prices.c.close,
                        sql.prices.c.volume,
                    ).where(
                        sql.prices.c.ticker == ticker,
                        sql.prices.c.date >= start,
                        sql.prices.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No rows found for {ticker}.")
        return df.set_index(["date"]).sort_index()

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
        with engine.begin() as conn:
            conn.execute(sql.prices.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)


daily = RefinedDaily()
"""The most popular way for accessing :class:`RefinedDaily`.

:meta hide-value:
"""

prices = RawPrices()
"""The most popular way for accessing :class:`RawPrices`.

:meta hide-value:
"""
