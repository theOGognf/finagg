"""Features from :mod:`yfinance` sources."""

import multiprocessing as mp
from functools import cache, partial

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from .. import backend, feat, utils
from . import api, sql


def _refined_daily_helper(ticker: str, /) -> tuple[str, pd.DataFrame]:
    """Helper for getting daily Yahoo! Finance data in a
    multiprocessing pool.

    Args:
        ticker: Ticker to create features for.

    Returns:
        The ticker and the returned feature dataframe.

    """
    df = DailyFeatures.from_raw(ticker)
    return ticker, df


class DailyFeatures(feat.Features):
    """Methods for gathering daily stock data features from Yahoo! finance.

    The module variable :data:`finagg.yfinance.feat.daily` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.yfinance.feat.daily.from_api("AAPL")
        >>> df2 = finagg.yfinance.feat.daily.from_raw("AAPL")
        >>> df3 = finagg.yfinance.feat.daily.from_refined("AAPL")
        >>> df1.equals(df2)
        True
        >>> df1.equals(df3)
        True

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
        cls, ticker: str, /, *, start: str = "1776-07-04", end: str = utils.today
    ) -> pd.DataFrame:
        """Get daily features directly from :meth:`finagg.yfinance.api.get`.

        Args:
            ticker: Company ticker.
            start: The start date of the stock history.
                Defaults to the first recorded date.
            end: The end date of the stock history.
                Defaults to the last recorded date.

        Returns:
            Daily stock price dataframe sorted by date.

        Examples:
            >>> finagg.yfinance.feat.daily.from_api("AAPL").head(5)
                           price  open_pct_change  high_pct_change  low_pct_change ...
            date                                                                   ...
            1980-12-15  0.094519        -0.047823        -0.051945       -0.052171 ...
            1980-12-16  0.087582        -0.073063        -0.073063       -0.073398 ...
            1980-12-17  0.089749         0.019703         0.024629        0.024751 ...
            1980-12-18  0.092351         0.028993         0.028853        0.028993 ...
            1980-12-19  0.097987         0.061029         0.060744        0.061029 ...

        """
        df = api.get(ticker, start=start, end=end)
        return cls._normalize(df)

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get daily features from local SQL tables.

        Args:
            ticker: Company ticker.
            start: The start date of the stock history.
                Defaults to the first recorded date.
            end: The end date of the stock history.
                Defaults to the last recorded date.
            engine: Raw store database engine.

        Returns:
            Daily stock price dataframe sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                raw SQL table.

        Examples:
            >>> finagg.yfinance.feat.daily.from_raw("AAPL").head(5)
                           price  open_pct_change  high_pct_change  low_pct_change ...
            date                                                                   ...
            1980-12-15  0.094519        -0.047823        -0.051945       -0.052171 ...
            1980-12-16  0.087582        -0.073063        -0.073063       -0.073398 ...
            1980-12-17  0.089749         0.019703         0.024629        0.024751 ...
            1980-12-18  0.092351         0.028993         0.028853        0.028993 ...
            1980-12-19  0.097987         0.061029         0.060744        0.061029 ...

        """
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
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get features from the feature-dedicated local SQL tables.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL tables
        is installed and is up-to-date).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.
            engine: Feature store database engine.

        Returns:
            Daily stock price dataframe sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.yfinance.feat.daily.from_refined("AAPL").head(5)
                           price  open_pct_change  high_pct_change  low_pct_change ...
            date                                                                   ...
            1980-12-15  0.094519        -0.047823        -0.051945       -0.052171 ...
            1980-12-16  0.087582        -0.073063        -0.073063       -0.073398 ...
            1980-12-17  0.089749         0.019703         0.024629        0.024751 ...
            1980-12-18  0.092351         0.028993         0.028853        0.028993 ...
            1980-12-19  0.097987         0.061029         0.060744        0.061029 ...

        """
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
        cls,
        lb: int = 1,
    ) -> set[str]:
        """Get all unique tickers in the raw SQL table that MAY BE ELIGIBLE
        to be in the feature's refined SQL table.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.

        Returns:
            All unique tickers that may be valid for creating daily features
            that also have at least ``lb`` rows used for constructing the
            features.

        Examples:
            >>> "AAPL" in finagg.yfinance.feat.daily.get_candidate_ticker_set()
            True

        """
        return sql.get_ticker_set(lb=lb)

    @classmethod
    @cache
    def get_ticker_set(
        cls,
        lb: int = 1,
    ) -> set[str]:
        """Get all unique tickers in the feature's SQL table.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.

        Returns:
            All unique tickers that contain all the columns for creating
            daily features that also have at least ``lb`` rows.

        Examples:
            >>> "AAPL" in finagg.yfinance.feat.daily.get_ticker_set()
            True

        """
        with backend.engine.begin() as conn:
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
    def install(cls, *, processes: int = mp.cpu_count() - 1) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from the raw SQL table.

        Args:
            processes: Number of background processes to use for installation.

        Returns:
            Number of rows written to the feature's refined SQL table.

        """
        sql.daily.drop(backend.engine, checkfirst=True)
        sql.daily.create(backend.engine)

        tickers = cls.get_candidate_ticker_set()
        total_rows = 0
        with (
            tqdm(
                total=len(tickers),
                desc="Installing refined Yahoo! Finance daily data",
                position=0,
                leave=True,
            ) as pbar,
            mp.Pool(
                processes=processes,
                initializer=partial(backend.engine.dispose, close=False),
            ) as pool,
        ):
            for ticker, df in pool.imap_unordered(_refined_daily_helper, tickers):
                rowcount = len(df.index)
                if rowcount:
                    cls.to_refined(ticker, df)
                    total_rows += rowcount
                pbar.update()
        return total_rows

    @classmethod
    def to_refined(
        cls,
        ticker: str,
        df: pd.DataFrame,
        /,
        *,
        engine: Engine = backend.engine,
    ) -> int:
        """Write the given dataframe to the refined feature table
        while using the ticker ``ticker``.

        Args:
            ticker: Company ticker.
            df: Dataframe to store as rows in a local SQL table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        df = df.reset_index("date")
        if set(df.columns) < set(cls.columns):
            raise ValueError(f"Dataframe must have columns {cls.columns}")
        df = df.melt("date", var_name="name", value_name="value")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.daily.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


daily = DailyFeatures()
"""The most popular way for accessing :class:`DailyFeatures`.

:meta hide-value:
"""
