"""Quarterly features from SEC sources."""

import logging
from typing import Literal

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from .... import backend, utils
from ... import api, sql
from .. import _raw

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class IndustryQuarterly:
    """Methods for gathering industry-averaged quarterly data from SEC
    features.

    The class variable :attr:`finagg.sec.feat.Quarterly.industry` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        You can aggregate this feature set using a ticker or an industry code
        directly.

        >>> df1 = finagg.sec.feat.quarterly.industry.from_refined(ticker="MSFT").head(5)
        >>> df2 = finagg.sec.feat.quarterly.industry.from_refined(code=73).head(5)
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
        """Get quarterly features from the feature store,
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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Raises:
            `ValueError`: If neither a ``ticker`` nor ``code`` are provided.
            `NoResultFound`: If there are no rows for ``ticker`` or ``code``
                in the refined SQL table.

        Examples:
            >>> df = finagg.sec.feat.quarterly.industry.from_refined(ticker="AAPL").head(5)
            >>> df["mean"]  # doctest: +SKIP
            name                AssetCoverageRatio  DebtEquityRatio  EarningsPerShareBasic ...
            fy   fp filed                                                                  ...
            2010 Q1 2010-04-29            1.718796         0.956111               0.865000 ...
                 Q2 2010-07-30            1.753165         0.874013               0.380000 ...
                 Q3 2010-11-04            1.830832         0.842663               1.062857 ...
            2011 Q1 2011-05-05            2.164743         0.757230               1.022500 ...
                 Q2 2011-08-08            2.222168         0.718292               2.016667 ...
            >>> df["std"]  # doctest: +SKIP
            name                AssetCoverageRatio  DebtEquityRatio  EarningsPerShareBasic ...
            fy   fp filed                                                                  ...
            2010 Q1 2010-04-29            0.394667         0.327296               1.139079 ...
                 Q2 2010-07-30            0.310804         0.285554               2.954901 ...
                 Q3 2010-11-04            0.351052         0.273552               3.075227 ...
            2011 Q1 2011-05-05            1.166501         0.311317               1.137287 ...
                 Q2 2011-08-08            1.104677         0.313766               2.075428 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.quarterly.name):
            sql.quarterly.create(engine)
        with engine.begin() as conn:
            if ticker:
                (sic,) = conn.execute(
                    sa.select(sql.submissions.c.sic).where(
                        sql.submissions.c.ticker == ticker
                    )
                ).one()
                code = str(sic)[:level]
            elif code:
                code = str(code)[:level]
            else:
                raise ValueError("Must provide a `ticker` or `code`.")

            df = pd.DataFrame(
                conn.execute(
                    sql.quarterly.select()
                    .join(
                        sql.submissions,
                        (sql.submissions.c.cik == sql.quarterly.c.cik)
                        & (sql.submissions.c.sic.startswith(code)),
                    )
                    .where(sql.quarterly.c.filed >= start, sql.quarterly.c.filed <= end)
                )
            )
        if not len(df.index):
            raise NoResultFound(
                f"No industry quarterly rows found for industry {code}."
            )
        df = df.drop(columns=["cik"])
        df = df.melt(
            ["fy", "fp", "filed"], var_name="name", value_name="value"
        ).set_index(["fy", "fp"])
        df["filed"] = df.groupby(["fy", "fp"])["filed"].max()
        return (
            df.reset_index()  # type: ignore[return-value]
            .set_index(["fy", "fp", "filed"])
            .groupby(["fy", "fp", "filed", "name"])
            .agg([np.mean, np.std])
            .reset_index()
            .pivot(index=["fy", "fp", "filed"], columns="name")["value"]
            .sort_index()
            .dropna()
        )


class NormalizedQuarterly:
    """Quarterly features from SEC EDGAR data normalized according to industry
    averages and standard deviations.

    The class variable :data:`finagg.sec.feat.Quarterly.normalized` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They both return equivalent dataframes.

        >>> df1 = finagg.sec.feat.quarterly.normalized.from_other_refined("AAPL").head(5)
        >>> df2 = finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)
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
            Relative quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Examples:
            >>> finagg.sec.feat.quarterly.normalized.from_other_refined("AAPL").head(5)  # doctest: +SKIP
                                NORM(LOG_CHANGE(Assets))  NORM(LOG_CHANGE(AssetsCurrent))  NORM(LOG_CHANGE(CommonStockSharesOutstanding)) ...
            fy   fp filed                                                                                                                 ...
            2010 Q2 2010-04-21                  0.000000                         0.000000                                        0.000000 ...
                 Q3 2010-07-21                  0.000000                         0.000000                                        0.000000 ...
            2011 Q1 2011-01-19                  0.978816                         0.074032                                        0.407282 ...
                 Q2 2011-04-21                  0.000000                         0.000000                                        0.000000 ...
                 Q3 2011-07-20                 -0.353553                        -0.353553                                        0.051602 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        company_df = Quarterly.from_refined(
            ticker, start=start, end=end, engine=engine
        ).reset_index(["filed"])
        filed = company_df["filed"]
        industry_df = IndustryQuarterly.from_refined(
            ticker=ticker, level=level, start=start, end=end, engine=engine
        ).reset_index(["filed"])
        company_df = (company_df - industry_df["mean"]) / industry_df["std"]
        company_df["filed"] = filed
        func_cols = utils.get_func_cols(sql.quarterly)
        company_df[func_cols] = company_df[func_cols].fillna(value=0.0)
        company_df = (
            company_df.reset_index()
            .set_index(["fy", "fp", "filed"])
            .rename(lambda x: f"NORM({x})", axis=1)
        )
        company_df = (
            company_df.fillna(method="ffill")
            .reset_index()
            .dropna()
            .drop_duplicates("filed")
            .set_index(["fy", "fp", "filed"])
            .sort_index()
        )
        company_df = utils.resolve_col_order(
            sql.normalized_quarterly, company_df, extra_ignore=["filed"]
        )
        return company_df

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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)  # doctest: +SKIP
                                NORM(LOG_CHANGE(Assets))  NORM(LOG_CHANGE(AssetsCurrent))  NORM(LOG_CHANGE(CommonStockSharesOutstanding)) ...
            fy   fp filed                                                                                                                 ...
            2010 Q2 2010-04-21                  0.000000                         0.000000                                        0.000000 ...
                 Q3 2010-07-21                  0.000000                         0.000000                                        0.000000 ...
            2011 Q1 2011-01-19                  0.978816                         0.074032                                        0.407282 ...
                 Q2 2011-04-21                  0.000000                         0.000000                                        0.000000 ...
                 Q3 2011-07-20                 -0.353553                        -0.353553                                        0.051602 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.normalized_quarterly.name):
            sql.normalized_quarterly.create(engine)
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.normalized_quarterly.select()
                    .join(
                        sql.submissions,
                        (sql.submissions.c.cik == sql.normalized_quarterly.c.cik)
                        & (sql.submissions.c.ticker == ticker),
                    )
                    .where(
                        sql.normalized_quarterly.c.filed >= start,
                        sql.normalized_quarterly.c.filed <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(
                f"No industry-normalized quarterly rows found for {ticker}."
            )
        df = df.drop(columns=["cik"]).set_index(["fy", "fp", "filed"]).sort_index()
        return df

    @classmethod
    def get_candidate_ticker_set(
        cls, lb: int = 1, *, engine: None | Engine = None
    ) -> set[str]:
        """Get all unique tickers in the quarterly SQL table that MAY BE
        ELIGIBLE to be in the feature's SQL table.

        This is just an alias for :meth:`finagg.sec.feat.Quarterly.get_ticker_set`.

        Args:
            lb: Minimum number of rows required to include a ticker in the
                returned set.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            All unique tickers that may be valid for creating
            industry-normalized quarterly features that also have at least
            ``lb`` rows for each tag used for constructing the features.

        Examples:
            >>> "AAPL" in finagg.sec.feat.quarterly.normalized.get_candidate_ticker_set()  # doctest: +SKIP
            True

        """
        return Quarterly.get_ticker_set(lb=lb, engine=engine)

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
            industry-normalized quarterly features that also have at least
            ``lb`` rows.

        Examples:
            >>> "AAPL" in finagg.sec.feat.quarterly.normalized.get_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.normalized_quarterly.name):
            sql.normalized_quarterly.create(engine)
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.submissions.c.ticker)
                    .join(
                        sql.normalized_quarterly,
                        sql.normalized_quarterly.c.cik == sql.submissions.c.cik,
                    )
                    .group_by(sql.normalized_quarterly.c.cik)
                    .having(sa.func.count(sql.normalized_quarterly.c.filed) >= lb)
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
        year: int = -1,
        quarter: int = -1,
        engine: None | Engine = None,
    ) -> list[str]:
        """Get all tickers in the feature's SQL table sorted by a particular
        column.

        Args:
            column: Feature column to sort by.
            ascending: Whether to return results in ascending order according
                to the values in ``column``.
            year: Year to select from. Defaults to the most recent year that
                has data available.
            quarter: Quarter to select from. Defaults to the most recent quarter
                that has data available.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Tickers sorted by a feature column for a particular year and quarter.

        Examples:
            >>> ts = finagg.sec.feat.quarterly.normalized.get_tickers_sorted_by(
            ...         "EarningsPerShareBasic",
            ...         year=2020,
            ...         quarter=3
            ... )
            >>> "AMD" == ts[0]  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.normalized_quarterly.name):
            sql.normalized_quarterly.create(engine)
        with engine.begin() as conn:
            if year == -1:
                (max_year,) = conn.execute(
                    sa.select(sa.func.max(sql.normalized_quarterly.c.fy))
                ).one()
                year = int(max_year)

            if quarter == -1:
                (max_quarter,) = conn.execute(
                    sa.select(sa.func.max(sql.normalized_quarterly.c.fp))
                ).one()
                fp = str(max_quarter)
            else:
                fp = f"Q{quarter}"

            tickers = (
                conn.execute(
                    sa.select(sql.submissions.c.ticker)
                    .join(
                        sql.normalized_quarterly,
                        sql.normalized_quarterly.c.cik == sql.submissions.c.cik,
                    )
                    .where(
                        sql.normalized_quarterly.c.fy == year,
                        sql.normalized_quarterly.c.fp == fp,
                    )
                    .order_by(sql.normalized_quarterly.c[column])
                )
                .scalars()
                .all()
            )
        if not ascending:
            return list(reversed(tickers))
        return list(tickers)

    @classmethod
    def install(
        cls,
        tickers: None | set[str] = None,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install data associated with ``tickers`` by pulling data from the
        quarterly SQL tables, transforming them into normalized features, and
        then writing to the refined quarterly normalized SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the candidate tickers from
                :meth:`NormalizedQuarterly.get_candidate_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        if not tickers:
            logger.info(
                "Skipping finagg.sec.feat.quarterly.normalized installation because no"
                " tickers were provided or no tickers were found with prerequisite data"
                " (i.e., finagg.sec.feat.quarterly data)"
            )
            return 0

        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(
            sql.normalized_quarterly.name
        ):
            sql.normalized_quarterly.drop(engine, checkfirst=True)
            sql.normalized_quarterly.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing refined SEC industry-normalized quarterly data",
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

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.normalized_quarterly.name):
            sql.normalized_quarterly.create(engine)
        df = df.reset_index(["fy", "fp", "filed"])
        df["cik"] = sql.get_cik(ticker, engine=engine)
        with engine.begin() as conn:
            conn.execute(sql.normalized_quarterly.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class Quarterly:
    """Methods for gathering quarterly features from SEC EDGAR data.

    The module variable :data:`finagg.sec.feat.quarterly` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.sec.feat.quarterly.from_api("AAPL").head(5)
        >>> df2 = finagg.sec.feat.quarterly.from_raw("AAPL").head(5)
        >>> df3 = finagg.sec.feat.quarterly.from_refined("AAPL").head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
        >>> pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)

    """

    industry = IndustryQuarterly()
    """Quarterly features aggregated for an entire industry.
    The most popular way for accessing the :class:`IndustryQuarterly`
    feature set.

    :meta hide-value:
    """

    normalized = NormalizedQuarterly()
    """A company's quarterly features normalized by its industry.
    The most popular way for accessing the :class:`NormalizedQuarterly`
    feature set.

    :meta hide-value:
    """

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize quarterly features columns."""
        df = api.get_financial_ratios(df)
        df = df.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")
        df = utils.resolve_func_cols(sql.quarterly, df, drop=True, inplace=True)
        df.columns = df.columns.rename(None)
        df = utils.resolve_col_order(sql.quarterly, df, extra_ignore=["filed"])
        return df.dropna()

    @classmethod
    def from_api(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get quarterly features directly from the SEC API.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent quarterly publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Examples:
            >>> finagg.sec.feat.quarterly.from_api("AAPL").head(5)  # doctest: +SKIP
                                LOG_CHANGE(Assets)  LOG_CHANGE(AssetsCurrent)  LOG_CHANGE(CommonStockSharesOutstanding) ...
            fy   fp filed                                                                                               ...
            2010 Q1 2010-01-25            0.182629                  -0.023676                                  0.012840 ...
                 Q2 2010-04-21            0.000000                   0.000000                                  0.000000 ...
                 Q3 2010-07-21            0.000000                   0.000000                                  0.000000 ...
            2011 Q1 2011-01-19            0.459174                   0.278241                                  0.017805 ...
                 Q2 2011-04-21            0.000000                   0.000000                                  0.000000 ...

        """
        df = api.company_concept.join_get(
            api.popular_concepts, ticker=ticker, form="10-Q", start=start, end=end
        )
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
        """Get quarterly features from a local SEC SQL table.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent quarterly publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Examples:
            >>> finagg.sec.feat.quarterly.from_raw("AAPL").head(5)  # doctest: +SKIP
                                LOG_CHANGE(Assets)  LOG_CHANGE(AssetsCurrent)  LOG_CHANGE(CommonStockSharesOutstanding) ...
            fy   fp filed                                                                                               ...
            2010 Q1 2010-01-25            0.182629                  -0.023676                                  0.012840 ...
                 Q2 2010-04-21            0.000000                   0.000000                                  0.000000 ...
                 Q3 2010-07-21            0.000000                   0.000000                                  0.000000 ...
            2011 Q1 2011-01-19            0.459174                   0.278241                                  0.017805 ...
                 Q2 2011-04-21            0.000000                   0.000000                                  0.000000 ...

        """
        df = _raw.Tags.join_from_raw(
            ticker,
            [concept["tag"] for concept in api.popular_concepts],
            form="10-Q",
            start=start,
            end=end,
            engine=engine,
        )
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
        """Get features from the refined SQL table.

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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.sec.feat.quarterly.from_refined("AAPL").head(5)  # doctest: +SKIP
                                LOG_CHANGE(Assets)  LOG_CHANGE(AssetsCurrent)  LOG_CHANGE(CommonStockSharesOutstanding) ...
            fy   fp filed                                                                                               ...
            2010 Q1 2010-01-25            0.182629                  -0.023676                                  0.012840 ...
                 Q2 2010-04-21            0.000000                   0.000000                                  0.000000 ...
                 Q3 2010-07-21            0.000000                   0.000000                                  0.000000 ...
            2011 Q1 2011-01-19            0.459174                   0.278241                                  0.017805 ...
                 Q2 2011-04-21            0.000000                   0.000000                                  0.000000 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.quarterly.name):
            sql.quarterly.create(engine)
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.quarterly.select()
                    .join(
                        sql.submissions,
                        (sql.submissions.c.cik == sql.quarterly.c.cik)
                        & (sql.submissions.c.ticker == ticker),
                    )
                    .where(
                        sql.quarterly.c.filed >= start,
                        sql.quarterly.c.filed <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No quarterly rows found for {ticker}.")
        df = df.drop(columns=["cik"]).set_index(["fy", "fp", "filed"]).sort_index()
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
            All unique tickers that may be valid for creating quarterly features
            that also have at least ``lb`` rows for each tag used for
            constructing the features.

        Examples:
            >>> "AAPL" in finagg.sec.feat.quarterly.get_candidate_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.tags.name):
            sql.tags.create(engine)
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(
                        sql.submissions.c.ticker,
                        *[
                            sa.func.sum(
                                sa.case(
                                    {concept["tag"]: 1}, value=sql.tags.c.tag, else_=0
                                )
                            ).label(concept["tag"])
                            for concept in api.popular_concepts
                        ],
                    )
                    .join(sql.tags, sql.tags.c.cik == sql.submissions.c.cik)
                    .where(sql.tags.c.form == "10-Q")
                    .group_by(sql.tags.c.cik)
                    .having(
                        *[
                            sa.text(f"{concept['tag']} >= {lb}")
                            for concept in api.popular_concepts
                        ]
                    )
                )
                .scalars()
                .all()
            )
        return set(tickers)

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
            quarterly features that also have at least ``lb`` rows.

        Examples:
            >>> "AAPL" in finagg.sec.feat.quarterly.get_ticker_set()
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.quarterly.name):
            sql.quarterly.create(engine)
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.submissions.c.ticker)
                    .join(sql.quarterly, sql.quarterly.c.cik == sql.submissions.c.cik)
                    .group_by(sql.quarterly.c.cik)
                    .having(sa.func.count(sql.quarterly.c.filed) >= lb)
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
        raw SQL tables, transforming them into quarterly features, and then
        writing to the refined quarterly SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the candidate tickers from
                :meth:`Quarterly.get_candidate_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        if not tickers:
            logger.info(
                "Skipping finagg.sec.feat.quarterly installation because no tickers"
                " were provided or no tickers were found with prerequisite data (i.e.,"
                " finagg.sec.feat.submissions and finagg.sec.feat.raw data)"
            )
            return 0

        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.quarterly.name):
            sql.quarterly.drop(engine, checkfirst=True)
            sql.quarterly.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing refined SEC quarterly data",
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
            df: Dataframe to store as rows in a local SQL table
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the SQL table.

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.quarterly.name):
            sql.quarterly.create(engine)
        df = df.reset_index(["fy", "fp", "filed"])
        df["cik"] = sql.get_cik(ticker, engine=engine)
        with engine.begin() as conn:
            conn.execute(sql.quarterly.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)
