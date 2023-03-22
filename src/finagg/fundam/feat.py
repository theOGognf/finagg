"""Fundamental features aggregated from several sources."""

import logging
from datetime import datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from .. import backend, feat, sec, utils, yfinance
from . import sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class RefinedIndustryFundamental:
    """Methods for gathering industry-averaged fundamental data.

    The class variable :data:`finagg.fundam.feat.fundam.industry` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        You can aggregate this feature set using a ticker or an industry code
        directly.

        >>> df1 = finagg.fundam.feat.fundam.industry.from_refined(ticker="MSFT").head(5)
        >>> df2 = finagg.fundam.feat.fundam.industry.from_refined(code=73).head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)

    """

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        ticker: None | str = None,
        code: None | str = None,
        level: Literal[2, 3, 4] = 2,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get fundamental features from the feature store,
        aggregated for an entire industry.

        The industry can be chosen according to a company or
        by an industry code directly. If a company is provided,
        then the first ``level`` digits of the company's SIC code
        is used for the industry code.

        Args:
            ticker: Company ticker. Lookup the industry associated
                with this company. Mutually exclusive with ``code``.
            code: Industry SIC code to use for industry lookup.
                Mutually exclusive with ``ticker``.
            level: Industry level to aggregate features at.
                The industry used according to ``ticker`` or ``code``
                is subsampled according to this value. Options include:

                    - 2 = major group (e.g., furniture and fixtures)
                    - 3 = industry group (e.g., office furnitures)
                    - 4 = industry (e.g., wood office furniture)

            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Fundamental data dataframe with each feature as a
            separate column. Sorted by date.

        Raises:
            `ValueError`: If neither a ``ticker`` nor ``code`` are provided.
            `NoResultFound`: If there are no rows for ``ticker`` or ``code``
                in the refined SQL table.

        Examples:
            >>> df = finagg.fundam.feat.fundam.industry.from_refined(ticker="AAPL").head(5)
            >>> df["avg"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name        AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            date                                                                    ...
            2009-10-23                       0.0           0.3305              2.48 ...
            2009-10-26                       0.0           0.3305              2.48 ...
            2009-10-27                       0.0           0.3305              2.48 ...
            2009-10-28                       0.0           0.3305              2.48 ...
            2009-10-29                       0.0           0.3305              2.48 ...
            >>> df["std"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name        AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            date                                                                    ...
            2009-10-23                       0.0              0.0               0.0 ...
            2009-10-26                       0.0              0.0               0.0 ...
            2009-10-27                       0.0              0.0               0.0 ...
            2009-10-28                       0.0              0.0               0.0 ...
            2009-10-29                       0.0              0.0               0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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


class RefinedNormalizedFundamental:
    """Fundamental features from quarterly and daily data, normalized
    according to industry averages and standard deviations.

    The class variable :data:`finagg.fundam.feat.fundam.normalized` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.fundam.feat.fundam.normalized.from_other_refined("AAPL").head(5)
        >>> df2 = finagg.fundam.feat.fundam.from_refined("AAPL").head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)

    """

    @classmethod
    def from_other_refined(
        cls,
        ticker: str,
        /,
        *,
        level: Literal[2, 3, 4] = 2,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from other feature SQL tables.

        Args:
            ticker: Company ticker.
            level: Industry level to aggregate relative features at.
                The industry used according to ``ticker`` is subsampled
                according to this value. Options include:

                    - 2 = major group (e.g., furniture and fixtures)
                    - 3 = industry group (e.g., office furnitures)
                    - 4 = industry (e.g., wood office furniture)

            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Relative fundamental data dataframe with each feature as a
            separate column. Sorted by filing date.

        Examples:
            >>> finagg.fundam.feat.fundam.normalized.from_other_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            date                                                                    ...
            2010-01-25                   -1.4142          -0.6309           -0.6506 ...
            2010-01-26                    0.0000          -0.6309           -0.6506 ...
            2010-01-27                    0.0000          -0.6309           -0.6506 ...
            2010-01-28                    0.5774          -0.5223            0.2046 ...
            2010-01-29                    0.0000          -0.5940            0.4365 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        company_df = RefinedFundamental.from_refined(
            ticker, start=start, end=end, engine=engine
        )
        industry_df = RefinedIndustryFundamental.from_refined(
            ticker=ticker, level=level, start=start, end=end, engine=engine
        )
        company_df = (company_df - industry_df["avg"]) / industry_df["std"]
        company_df = company_df.sort_index()
        pct_change_columns = RefinedFundamental.pct_change_target_columns()
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
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from the features SQL table.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL table
        is current).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Fundamental data dataframe with each feature as a
            separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.fundam.feat.fundam.normalized.from_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            date                                                                    ...
            2010-01-25                   -1.4142          -0.6309           -0.6506 ...
            2010-01-26                    0.0000          -0.6309           -0.6506 ...
            2010-01-27                    0.0000          -0.6309           -0.6506 ...
            2010-01-28                    0.5774          -0.5223            0.2046 ...
            2010-01-29                    0.0000          -0.5940            0.4365 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        df = df[RefinedFundamental.columns]
        return df

    @classmethod
    def get_candidate_ticker_set(
        cls, lb: int = 1, *, engine: None | Engine = None
    ) -> set[str]:
        """Get all unique tickers in the fundamental SQL table that MAY BE
        ELIGIBLE to be in the feature's SQL table.

        This is just an alias for :meth:`finagg.fundam.feat.fundam.get_ticker_set`.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            All unique tickers that may be valid for creating
            industry-normalized fundamental features that also have at least
            ``lb`` rows for each tag used for constructing the features.

        Examples:
            >>> "AAPL" in finagg.fundam.feat.fundam.normalized.get_candidate_ticker_set()
            True

        """
        return RefinedFundamental.get_ticker_set(lb=lb, engine=engine)

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
            fundamental features that also have at least ``lb`` rows.

        Examples:
            >>> "AAPL" in finagg.fundam.feat.fundam.normalized.get_ticker_set()
            True

        """
        engine = engine or backend.engine
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.normalized_fundam.c.ticker)
                    .group_by(sql.normalized_fundam.c.ticker)
                    .having(
                        *[
                            sa.func.count(sql.normalized_fundam.c.name == col) >= lb
                            for col in RefinedFundamental.columns
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
        engine: None | Engine = None,
    ) -> list[str]:
        """Get all tickers in the feature's SQL table sorted by a particular
        column for a date.

        Args:
            column: Feature column to sort by.
            ascending: Whether to return results in ascending order according
                to the values in ``column``.
            date: Date to select from. Defaults to the most recent date with
                available data. If a string, should be in "%Y-%m-%d" format.
                If an integer, should be days before the current date (e.g,
                ``-2`` indicates two days before today).
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Tickers sorted by a feature column for a particular date.

        Raises:
            `ValueError`: If the integer date is positive.

        Examples:
            >>> ts = finagg.fundam.feat.fundam.normalized.get_tickers_sorted_by(
            ...         "PriceEarningsRatio",
            ...         date="2019-01-04"
            ... )
            >>> "AMD" == ts[0]
            True

        """
        engine = engine or backend.engine
        with engine.begin() as conn:
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
    def install(
        cls, tickers: None | set[str] = None, *, engine: None | Engine = None
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.fundam.feat.fundam.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        sql.normalized_fundam.drop(engine, checkfirst=True)
        sql.normalized_fundam.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing refined industry-normalized fundamental data",
            position=0,
            leave=True,
        ):
            try:
                df = cls.from_other_refined(ticker, engine=engine)
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
        """Write the dataframe to the feature store for ``ticker``.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
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
        if set(df.columns) < set(RefinedFundamental.columns):
            raise ValueError(
                f"Dataframe must have columns {RefinedFundamental.columns} but got {df.columns}"
            )
        df = df.melt("date", var_name="name", value_name="value")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.normalized_fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class RefinedFundamental(feat.Features):
    """Method for gathering fundamental data on a stock using several sources.

    The module variable :data:`finagg.fundam.feat.fundam` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.fundam.feat.fundam.from_api("AAPL").head(5)
        >>> df2 = finagg.fundam.feat.fundam.from_raw("AAPL").head(5)
        >>> df3 = finagg.fundam.feat.fundam.from_refined("AAPL").head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
        >>> pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)

    """

    #: Columns within this feature set. Dataframes returned by this class's
    #: methods will always contain these columns.
    columns = (
        yfinance.feat.daily.columns
        + sec.feat.quarterly.columns
        + ["PriceEarningsRatio"]
    )

    industry = RefinedIndustryFundamental()
    """Fundamental features aggregated for an entire industry.
    The most popular way for accessing the :class:`RefinedIndustryFundamental`
    feature set.

    :meta hide-value:
    """

    normalized = RefinedNormalizedFundamental()
    """A company's fundamental features normalized by its industry.
    The most popular way for accessing the :class:`RefinedNormalizedFundamental`
    feature set.

    :meta hide-value:
    """

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
                for col in sec.feat.RefinedQuarterly.columns
                if not col.endswith("pct_change")
            ]
        ].last()
        quarterly_pct_change_cols = (
            sec.feat.RefinedQuarterly.pct_change_target_columns()
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
        start: None | str = None,
        end: None | str = None,
    ) -> pd.DataFrame:
        """Get features directly from APIs.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        Examples:
            >>> finagg.fundam.feat.fundam.from_api("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                         price  open_pct_change ... PriceEarningsRatio
            date                                ...
            2010-01-25  6.1727          -0.0207 ...             2.4302
            2010-01-26  6.2600           0.0170 ...             2.4646
            2010-01-27  6.3189           0.0044 ...             2.4878
            2010-01-28  6.0578          -0.0093 ...             2.3850
            2010-01-29  5.8381          -0.0188 ...             2.2984

        """
        start = start or "1776-07-04"
        end = end or utils.today
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
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features directly from other refined SQL tables.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        Examples:
            >>> finagg.fundam.feat.fundam.from_other_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                         price  open_pct_change ... PriceEarningsRatio
            date                                ...
            2010-01-25  6.1727          -0.0207 ...             2.4302
            2010-01-26  6.2600           0.0170 ...             2.4646
            2010-01-27  6.3189           0.0044 ...             2.4878
            2010-01-28  6.0578          -0.0093 ...             2.3850
            2010-01-29  5.8381          -0.0188 ...             2.2984

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features directly from other raw SQL tables.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        Examples:
            >>> finagg.fundam.feat.fundam.from_raw("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                         price  open_pct_change ... PriceEarningsRatio
            date                                ...
            2010-01-25  6.1727          -0.0207 ...             2.4302
            2010-01-26  6.2600           0.0170 ...             2.4646
            2010-01-27  6.3189           0.0044 ...             2.4878
            2010-01-28  6.0578          -0.0093 ...             2.3850
            2010-01-29  5.8381          -0.0188 ...             2.2984

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from the feature-dedicated local SQL tables.

        This is the preferred method for accessing features for
        offline analysis (assuming data in the local SQL tables
        is current).

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Combined quarterly and daily feature dataframe.
            Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.fundam.feat.fundam.from_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                         price  open_pct_change ... PriceEarningsRatio
            date                                ...
            2010-01-25  6.1727          -0.0207 ...             2.4302
            2010-01-26  6.2600           0.0170 ...             2.4646
            2010-01-27  6.3189           0.0044 ...             2.4878
            2010-01-28  6.0578          -0.0093 ...             2.3850
            2010-01-29  5.8381          -0.0188 ...             2.2984

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        cls, lb: int = 1, *, engine: None | Engine = None
    ) -> set[str]:
        """Get all unique tickers in the raw SQL table that MAY BE ELIGIBLE
        to be in the feature's SQL table.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            All unique tickers that may be valid for both quarterly and daily
            features that also have at least ``lb`` rows used for constructing
            the features.

        Examples:
            >>> "AAPL" in finagg.fundam.feat.fundam.get_candidate_ticker_set()
            True

        """
        return sec.feat.RefinedQuarterly.get_ticker_set(
            lb=lb, engine=engine
        ) & yfinance.feat.RefinedDaily.get_ticker_set(lb=lb, engine=engine)

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
            fundamental features that also have at least ``lb`` rows.

        Examples:
            >>> "AAPL" in finagg.fundam.feat.fundam.get_ticker_set()
            True

        """
        engine = engine or backend.engine
        with engine.begin() as conn:
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
    def install(
        cls, tickers: None | set[str] = None, *, engine: None | Engine = None
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.sec.feat.quarterly.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        sql.fundam.drop(engine, checkfirst=True)
        sql.fundam.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing refined fundamental data",
            position=0,
            leave=True,
        ):
            try:
                df = cls.from_other_refined(ticker, engine=engine)
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
        """Write the dataframe to the feature store for ``ticker``.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
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
            conn.execute(sql.fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


fundam = RefinedFundamental()
"""The most popular way for accessing :class:`RefinedFundamental`.

:meta hide-value:
"""
