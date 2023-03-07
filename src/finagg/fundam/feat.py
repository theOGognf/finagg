"""Features from several sources."""

import multiprocessing as mp
from datetime import datetime, timedelta
from functools import cache, partial
from typing import Literal

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from .. import backend, feat, sec, utils, yfinance
from . import sql


def _refined_fundam_helper(ticker: str, /) -> tuple[str, pd.DataFrame]:
    """Helper for getting fundamental data in a multiprocessing pool.

    Args:
        ticker: Ticker to create features for.

    Returns:
        The ticker and the returned feature dataframe.

    """
    df = FundamentalFeatures.from_other_refined(ticker)
    return ticker, df


def _refined_normalized_fundam_helper(ticker: str, /) -> tuple[str, pd.DataFrame]:
    """Helper for getting industry-normalized fundamental data in a
    multiprocessing pool.

    Args:
        ticker: Ticker to create features for.

    Returns:
        The ticker and the returned feature dataframe.

    """
    df = NormalizedFundamentalFeatures.from_other_refined(ticker)
    return ticker, df


class IndustryFundamentalFeatures:
    """Methods for gathering industry-averaged fundamental data."""

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        ticker: None | str = None,
        code: None | str = None,
        level: Literal[2, 3, 4] = 2,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get fundamental features from the feature store,
        aggregated for an entire industry.

        The industry can be chosen according to a company or
        by an industry code directly. If a company is provided,
        then the first `level` digits of the company's SIC code
        is used for the industry code.

        Args:
            ticker: Company ticker. Lookup the industry associated
                with this company. Mutually exclusive with `code`.
            code: Industry SIC code to use for industry lookup.
                Mutually exclusive with `ticker`.
            level: Industry level to aggregate features at.
                The industry used according to `ticker` or `code`
                is subsampled according to this value. Options include:

                    - 2 = major group (e.g., furniture and fixtures)
                    - 3 = industry group (e.g., office furnitures)
                    - 4 = industry (e.g., wood office furniture)

            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Raw data and feature data SQL database engine.

        Returns:
            Fundamental data dataframe with each feature as a
            separate column. Sorted by date.

        Raises:
            `ValueError`: If neither a ``ticker`` nor ``code`` are provided.
            `NoResultFound`: If there are no rows for ``ticker`` or ``code``
                in the refined SQL table.

        """
        with engine.begin() as conn:
            if ticker:
                (sic,) = conn.execute(
                    sa.select(sec.sql.submissions.c.sic).where(
                        sec.sql.submissions.c.ticker == ticker
                    )
                ).one()
                code = str(sic)[:level]
            elif code:
                code = str(code)[:level]
            else:
                raise ValueError("Must provide a `ticker` or `code`.")

            df = pd.DataFrame(
                conn.execute(
                    sa.select(
                        sql.fundam.c.date,
                        sql.fundam.c.name,
                        sa.func.avg(sql.fundam.c.value).label("avg"),
                        sa.func.std(sql.fundam.c.value).label("std"),
                    )
                    .join(
                        sec.sql.submissions,
                        (sec.sql.submissions.c.ticker == sql.fundam.c.ticker)
                        & (sec.sql.submissions.c.sic.startswith(code)),
                    )
                    .group_by(
                        sql.fundam.c.date,
                        sql.fundam.c.name,
                    )
                    .where(
                        sql.fundam.c.date >= start,
                        sql.fundam.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No industry fundamental rows found for {code}.")
        df = df.pivot(
            index="date",
            columns="name",
            values=["avg", "std"],
        ).sort_index()
        return df


class NormalizedFundamentalFeatures:
    """Fundamental features from quarterly and daily data, normalized
    according to industry averages and standard deviations.

    """

    @classmethod
    def from_other_refined(
        cls,
        ticker: str,
        /,
        *,
        level: Literal[2, 3, 4] = 2,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get features from other feature SQL tables.

        Args:
            ticker: Company ticker.
            level: Industry level to aggregate relative features at.
                The industry used according to `ticker` is subsampled
                according to this value. Options include:

                    - 2 = major group (e.g., furniture and fixtures)
                    - 3 = industry group (e.g., office furnitures)
                    - 4 = industry (e.g., wood office furniture)

            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Feature store database engine.

        Returns:
            Relative fundamental data dataframe with each feature as a
            separate column. Sorted by filing date.

        """
        company_df = FundamentalFeatures.from_refined(
            ticker, start=start, end=end, engine=engine
        )
        industry_df = IndustryFundamentalFeatures.from_refined(
            ticker=ticker, level=level, start=start, end=end, engine=engine
        )
        company_df = (company_df - industry_df["avg"]) / industry_df["std"]
        company_df = company_df.sort_index()
        pct_change_columns = FundamentalFeatures.pct_change_target_columns()
        company_df[pct_change_columns] = company_df[pct_change_columns].fillna(
            value=0.0
        )
        return (
            company_df.fillna(method="ffill")
            .dropna()
            .reset_index()
            .drop_duplicates("date")
            .set_index("date")
        )

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
        """Get features from the features SQL table.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL table
        is current).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Feature store database engine.

        Returns:
            Fundamental data dataframe with each feature as a
            separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        """
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.normalized_fundam.select().where(
                        sql.normalized_fundam.c.ticker == ticker,
                        sql.normalized_fundam.c.date >= start,
                        sql.normalized_fundam.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(
                f"No industry-normalized fundamental rows found for {ticker}."
            )
        df = df.pivot(index="date", columns="name", values="value").sort_index()
        df.columns = df.columns.rename(None)
        df = df[FundamentalFeatures.columns]
        return df

    @classmethod
    def get_candidate_ticker_set(cls, lb: int = 1) -> set[str]:
        """The candidate ticker set is just the `fundam` ticker set."""
        return FundamentalFeatures.get_ticker_set(lb=lb)

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
            tickers = (
                conn.execute(
                    sa.select(sql.normalized_fundam.c.ticker)
                    .group_by(sql.normalized_fundam.c.ticker)
                    .having(
                        *[
                            sa.func.count(sql.normalized_fundam.c.name == col) >= lb
                            for col in FundamentalFeatures.columns
                        ]
                    )
                )
                .scalars()
                .all()
            )
        return set(tickers)

    @classmethod
    def get_tickers_sorted_by(
        cls,
        column: str,
        /,
        *,
        ascending: bool = True,
        date: int | str = 0,
    ) -> list[str]:
        """Get all tickers in the feature's SQL table sorted by a particular
        column for a date.

        Args:
            column: Feature column to sort by.
            ascending: Whether to return results in ascending order according
                to the values in `column`.
            date: Date to select from. Defaults to the most recent date with
                available data. If a string, should be in "%Y-%m-%d" format.
                If an integer, should be days before the current date (e.g,
                `-2` indicates two days before today).

        Returns:
            Tickers sorted by a feature column.

        Raises:
            `ValueError`: If the integer date is positive.

        """
        with backend.engine.begin() as conn:
            if isinstance(date, int):
                if date > 0:
                    raise ValueError("Date should be non-positive")

                (max_date,) = conn.execute(
                    sa.select(sa.func.max(sql.normalized_fundam.c.date))
                ).one()
                if date == 0:
                    date = str(max_date)
                else:
                    date = (
                        datetime.fromisoformat(str(max_date)) - timedelta(days=date)
                    ).strftime("%Y-%m-%d")

            tickers = (
                conn.execute(
                    sa.select(sql.normalized_fundam.c.ticker)
                    .where(
                        sql.normalized_fundam.c.name == column,
                        sql.normalized_fundam.c.date == date,
                    )
                    .order_by(sql.normalized_fundam.c.value)
                )
                .scalars()
                .all()
            )
        if not ascending:
            return list(reversed(tickers))
        return list(tickers)

    @classmethod
    def install(cls, *, processes: int = mp.cpu_count() - 1) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Args:
            processes: Number of background processes to use for installation.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        sql.normalized_fundam.drop(backend.engine, checkfirst=True)
        sql.normalized_fundam.create(backend.engine)

        tickers = cls.get_candidate_ticker_set()
        total_rows = 0
        with (
            tqdm(
                total=len(tickers),
                desc="Installing refined industry-normalized fundamental data",
                position=0,
                leave=True,
            ) as pbar,
            mp.Pool(
                processes=processes,
                initializer=partial(backend.engine.dispose, close=False),
            ) as pool,
        ):
            for ticker, df in pool.imap_unordered(
                _refined_normalized_fundam_helper, tickers
            ):
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
        """Write the dataframe to the feature store for `ticker`.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        df = df.reset_index("date")
        if set(df.columns) < set(FundamentalFeatures.columns):
            raise ValueError(
                f"Dataframe must have columns {FundamentalFeatures.columns}"
            )
        df = df.melt("date", var_name="name", value_name="value")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.normalized_fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class FundamentalFeatures(feat.Features):
    """Method for gathering fundamental data on a stock using several sources."""

    #: Columns within this feature set.
    columns = (
        yfinance.feat.daily.columns
        + sec.feat.quarterly.columns
        + ["PriceEarningsRatio"]
    )

    #: Fundamental features aggregated by industry.
    industry = IndustryFundamentalFeatures()

    #: A company's fundamental features normalized by its industry.
    normalized = NormalizedFundamentalFeatures()

    @classmethod
    def _normalize(
        cls,
        quarterly: pd.DataFrame,
        daily: pd.DataFrame,
        /,
    ) -> pd.DataFrame:
        """Normalize the feature columns."""
        quarterly = quarterly.reset_index()
        quarterly_abs = quarterly.groupby(["filed"], as_index=False)[
            [
                col
                for col in sec.feat.QuarterlyFeatures.columns
                if not col.endswith("pct_change")
            ]
        ].last()
        quarterly_pct_change_cols = (
            sec.feat.QuarterlyFeatures.pct_change_target_columns()
        )
        quarterly[quarterly_pct_change_cols] += 1
        quarterly_pct_change = quarterly.groupby(["filed"], as_index=False).agg(
            {col: np.prod for col in quarterly_pct_change_cols}
        )
        quarterly = pd.merge(
            quarterly_abs,
            quarterly_pct_change,
            how="inner",
            left_on="filed",
            right_on="filed",
        )
        quarterly[quarterly_pct_change_cols] -= 1
        quarterly = quarterly.set_index("filed")
        df = pd.merge(
            quarterly, daily, how="outer", left_index=True, right_index=True
        ).sort_index()
        pct_change_cols = cls.pct_change_target_columns()
        df[pct_change_cols] = df[pct_change_cols].fillna(value=0)
        df = df.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")
        df["PriceEarningsRatio"] = df["price"] / df["EarningsPerShare"]
        df["PriceEarningsRatio"] = (
            df["PriceEarningsRatio"]
            .replace([-np.inf, np.inf], np.nan)
            .fillna(method="ffill")
        )
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
    def from_other_refined(
        cls,
        ticker: str,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get features directly from other refined SQL tables.

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
        quarterly = sec.feat.quarterly.from_refined(
            ticker,
            start=start,
            end=end,
            engine=engine,
        ).reset_index(["fy", "fp"], drop=True)
        start = str(quarterly.index[0])
        daily = yfinance.feat.daily.from_refined(
            ticker,
            start=start,
            end=end,
            engine=engine,
        )
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
        """Get features directly from other raw SQL tables.

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
        quarterly = sec.feat.quarterly.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        ).reset_index(["fy", "fp"], drop=True)
        start = str(quarterly.index[0])
        daily = yfinance.feat.daily.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        )
        return cls._normalize(quarterly, daily)

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

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

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
        if not len(df.index):
            raise NoResultFound(f"No fundamental rows found for {ticker}.")
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
            tickers = (
                conn.execute(
                    sa.select(sql.fundam.c.ticker)
                    .group_by(sql.fundam.c.ticker)
                    .having(
                        *[
                            sa.func.count(sql.fundam.c.name == col) >= lb
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
        """Write the dataframe to the feature store for `ticker`.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
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
            conn.execute(sql.fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Module variable intended for fully qualified name usage.
fundam = FundamentalFeatures()
