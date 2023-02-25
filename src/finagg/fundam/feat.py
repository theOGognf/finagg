"""Features from several sources."""

import multiprocessing as mp
from functools import cache, partial

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .. import backend, sec, utils, yfinance
from . import sql


def _refined_fundam_helper(ticker: str, /) -> tuple[str, pd.DataFrame]:
    """Helper for getting fundamental data in a multiprocessing pool.

    Args:
        ticker: Ticker to create features for.

    Returns:
        The ticker and the returned feature dataframe.

    """
    df = FundamentalFeatures.from_raw(ticker)
    return ticker, df


class FundamentalFeatures:
    """Method for gathering fundamental data on a stock using several sources."""

    #: Columns within this feature set.
    columns = (
        yfinance.feat.daily.columns
        + sec.feat.quarterly.columns
        + ["PriceEarningsRatio"]
    )

    @classmethod
    def _normalize(
        cls,
        quarterly: pd.DataFrame,
        daily: pd.DataFrame,
        /,
    ) -> pd.DataFrame:
        """Normalize the feature columns."""
        quarterly = quarterly.reset_index()
        abs_cols = [
            col
            for col in sec.feat.QuarterlyFeatures.columns
            if not col.endswith("pct_change")
        ]
        quarterly_abs = quarterly.groupby(["filed"], as_index=False)[abs_cols].last()
        pct_change_cols = [
            col
            for col in sec.feat.QuarterlyFeatures.columns
            if col.endswith("pct_change")
        ]
        quarterly_pct_change = quarterly.groupby(["filed"], as_index=False).agg(
            {col: np.prod for col in pct_change_cols}
        )
        quarterly = pd.merge(
            quarterly_abs,
            quarterly_pct_change,
            how="inner",
            left_on="filed",
            right_on="filed",
        )
        df = pd.merge(quarterly, daily, how="outer", left_index=True, right_index=True)
        pct_change_cols = [col for col in cls.columns if col.endswith("pct_change")]
        df[pct_change_cols] = df[pct_change_cols].fillna(method="pad")
        df = df.fillna(method="ffill").dropna()
        df["PriceEarningsRatio"] = df["price"] / df["EarningsPerShare"]
        df = utils.quantile_clip(df)
        df.index.names = ["date"]
        df = df[cls.columns]
        return df.dropna()

    @classmethod
    def from_api(
        cls,
        ticker: str,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
    ) -> pd.DataFrame:
        """Get features directly from APIs.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        quarterly = sec.feat.quarterly.from_api(
            ticker,
            start=start,
            end=end,
        ).reset_index(["fy", "fp"], drop=True)
        start = str(quarterly.index[0])
        daily = yfinance.feat.daily.from_api(ticker, start=start, end=end)
        return cls._normalize(quarterly, daily)

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
        """Get features directly from local SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.
            engine: Raw data and feature store database engine.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        quarterly_features = sec.feat.quarterly.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        ).reset_index(["fy", "fp"], drop=True)
        start = str(quarterly_features.index[0])
        daily_features = yfinance.feat.daily.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        )
        return cls._normalize(quarterly_features, daily_features)

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
        is current).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.
            engine: Feature store database engine.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        """
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.fundam.select().where(
                        sql.fundam.c.ticker == ticker,
                        sql.fundam.c.date >= start,
                        sql.fundam.c.date <= end,
                    )
                )
            )
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df

    @classmethod
    def get_candidate_ticker_set(
        cls,
        lb: int = 1,
    ) -> set[str]:
        """Get all unique tickers in the raw SQL table that MAY BE ELIGIBLE
        to be in the feature's SQL table.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.

        Returns:
            All unique tickers that may be valid for both quarterly and daily
            features.

        """
        return sec.feat.QuarterlyFeatures.get_ticker_set(
            lb=lb
        ) & yfinance.feat.DailyFeatures.get_ticker_set(lb=lb)

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
            fundamental features that also have at least `lb` rows.

        """
        with backend.engine.begin() as conn:
            tickers = set()
            for row in conn.execute(
                sa.select(sql.fundam.c.ticker)
                .distinct()
                .group_by(sql.fundam.c.ticker)
                .having(
                    *[
                        sa.func.count(sql.fundam.c.name == col) >= lb
                        for col in cls.columns
                    ]
                )
            ):
                (ticker,) = row
                tickers.add(str(ticker))
        return tickers

    @classmethod
    def install(cls, *, processes: int = mp.cpu_count() - 1) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Args:
            processes: Number of background processes to use for installation.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        sql.fundam.drop(backend.engine, checkfirst=True)
        sql.fundam.create(backend.engine)

        tickers = cls.get_candidate_ticker_set()
        total_rows = 0
        with (
            tqdm(
                total=len(tickers),
                desc="Installing refined fundamental data",
                position=0,
                leave=True,
            ) as pbar,
            mp.Pool(
                processes=processes,
                initializer=partial(backend.engine.dispose, close=False),
            ) as pool,
        ):
            for ticker, df in pool.imap_unordered(_refined_fundam_helper, tickers):
                rowcount = len(df.index)
                if not rowcount:
                    cls.to_refined(ticker, df)
                total_rows += rowcount
                pbar.update()
        return total_rows

    @classmethod
    def pct_change_columns_source_names(cls) -> list[str]:
        """Return the names of columns used for computed percent change
        columns.

        """
        return [
            col.removesuffix("_pct_change")
            for col in cls.columns
            if col.endswith("_pct_change")
        ]

    @classmethod
    def to_refined(
        cls,
        ticker: str,
        df: pd.DataFrame,
        /,
        *,
        engine: Engine = backend.engine,
    ) -> int:
        """Write the dataframe to the feature store for `ticker`.

        Does the necessary handling to transform columns to
        prepare the dataframe to be written to a dynamically-defined
        local SQL table.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index("date")
        df = df.melt("date", var_name="name", value_name="value")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Module variable intended for fully qualified name usage.
fundam = FundamentalFeatures()
