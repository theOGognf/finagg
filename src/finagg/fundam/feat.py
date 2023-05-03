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

from .. import backend, sec, utils, yfinance
from . import sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class IndustryFundamental:
    """Methods for gathering industry-averaged fundamental data.

    The class variable :attr:`finagg.fundam.feat.Fundamental.industry` is an
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
            >>> df["mean"]  # doctest: +SKIP
            name        PriceBookRatio  PriceEarningsRatio
            date
            2010-01-29        1.213018           17.020706
            2010-02-01        1.166246           16.364503
            2010-02-02        1.213047           17.021558
            2010-02-03        1.222630           17.156233
            2010-02-04        1.213401           17.026466
            >>> df["std"]  # doctest: +SKIP
            name        PriceBookRatio  PriceEarningsRatio
            date
            2010-01-29        1.469641           20.667755
            2010-02-01        1.414144           19.887156
            2010-02-02        1.476001           20.756459
            2010-02-03        1.490459           20.959443
            2010-02-04        1.475500           20.749523

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sec.sql.submissions.name):
            sec.sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.fundam.name):
            sql.fundam.create(engine)
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
                    sql.fundam.select()
                    .join(
                        sec.sql.submissions,
                        (sec.sql.submissions.c.ticker == sql.fundam.c.ticker)
                        & (sec.sql.submissions.c.sic.startswith(code)),
                    )
                    .where(sql.fundam.c.date >= start, sql.fundam.c.date <= end)
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No industry fundamental rows found for {code}.")
        df = df.drop(columns=["ticker"])
        df = df.melt(["date"], var_name="name", value_name="value").set_index("date")
        return (
            df.groupby(["date", "name"])  # type: ignore[return-value]
            .agg([np.mean, np.std])
            .reset_index()
            .pivot(index=["date"], columns="name")["value"]
            .sort_index()
            .dropna()
        )


class NormalizedFundamental:
    """Fundamental features from quarterly and daily data, normalized
    according to industry averages and standard deviations.

    The class variable :attr:`finagg.fundam.feat.Fundamental.normalized` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.fundam.feat.fundam.normalized.from_other_refined("AAPL").head(5)
        >>> df2 = finagg.fundam.feat.fundam.normalized.from_refined("AAPL").head(5)
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
            >>> finagg.fundam.feat.fundam.normalized.from_other_refined("AAPL").head(5)  # doctest: +SKIP
                        NORM(PriceBookRatio)  NORM(PriceEarningsRatio)
            date
            2010-01-25             -0.706266                 -0.706279
            2010-01-26             -0.698805                 -0.698935
            2010-01-27             -0.700699                 -0.700799
            2010-01-28             -0.701446                 -0.701534
            2010-01-29             -0.704558                 -0.704598

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        company_df = Fundamental.from_refined(
            ticker, start=start, end=end, engine=engine
        ).reset_index(["date"])
        date = company_df["date"]
        industry_df = IndustryFundamental.from_refined(
            ticker=ticker, level=level, start=start, end=end, engine=engine
        ).reset_index(["date"])
        company_df = (company_df - industry_df["mean"]) / industry_df["std"]
        company_df["date"] = date
        company_df = (
            company_df.reset_index()
            .set_index("date")
            .rename(lambda x: f"NORM({x})", axis=1)
        )
        company_df = (
            company_df.fillna(method="ffill")
            .reset_index()
            .dropna()
            .drop_duplicates("date")
            .set_index("date")
            .sort_index()
        )
        company_df = utils.resolve_col_order(sql.normalized_fundam, company_df)
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
            Fundamental data dataframe with each feature as a
            separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.fundam.feat.fundam.normalized.from_refined("AAPL").head(5)  # doctest: +SKIP
                        NORM(PriceBookRatio)  NORM(PriceEarningsRatio)
            date
            2010-01-25             -0.706266                 -0.706279
            2010-01-26             -0.698805                 -0.698935
            2010-01-27             -0.700699                 -0.700799
            2010-01-28             -0.701446                 -0.701534
            2010-01-29             -0.704558                 -0.704598

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.normalized_fundam.name):
            sql.normalized_fundam.create(engine)
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
        df = df.drop(columns=["ticker"]).set_index("date").sort_index()
        return df

    @classmethod
    def get_candidate_ticker_set(
        cls, lb: int = 1, *, engine: None | Engine = None
    ) -> set[str]:
        """Get all unique tickers in the fundamental SQL table that MAY BE
        ELIGIBLE to be in the feature's SQL table.

        This is just an alias for
        :meth:`finagg.fundam.feat.Fundamental.get_ticker_set`.

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
            >>> "AAPL" in finagg.fundam.feat.fundam.normalized.get_candidate_ticker_set()  # doctest: +SKIP
            True

        """
        return Fundamental.get_ticker_set(lb=lb, engine=engine)

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
            >>> "AAPL" in finagg.fundam.feat.fundam.normalized.get_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.normalized_fundam.name):
            sql.normalized_fundam.create(engine)
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.normalized_fundam.c.ticker)
                    .group_by(sql.normalized_fundam.c.ticker)
                    .having(sa.func.count(sql.normalized_fundam.c.date) >= lb)
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
            >>> "AMD" == ts[0]  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.normalized_fundam.name):
            sql.normalized_fundam.create(engine)
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
                        sql.normalized_fundam.c.date == date,
                    )
                    .order_by(sql.normalized_fundam.c[column])
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
        refined SQL tables, transforming them into normalized features, and
        then writing to the refined normalized fundamental SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.fundam.feat.Fundamental.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        if not tickers:
            logger.info(
                "Skipping finagg.fundam.feat.fundam.normalized installation because no"
                " tickers were provided or no tickers were found with prerequisite data"
                " (i.e., finagg.fundam.feat.fundam data)"
            )
            return 0

        if recreate_tables or not sa.inspect(engine).has_table(
            sql.normalized_fundam.name
        ):
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
        if not sa.inspect(engine).has_table(sql.normalized_fundam.name):
            sql.normalized_fundam.create(engine)
        df = df.reset_index("date")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.normalized_fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class Fundamental:
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

    industry = IndustryFundamental()
    """Fundamental features aggregated for an entire industry.
    The most popular way for accessing the :class:`IndustryFundamental`
    feature set.

    :meta hide-value:
    """

    normalized = NormalizedFundamental()
    """A company's fundamental features normalized by its industry.
    The most popular way for accessing the :class:`NormalizedFundamental`
    feature set.

    :meta hide-value:
    """

    @classmethod
    def _normalize(
        cls,
        quarterly: pd.DataFrame,
        prices: pd.DataFrame,
        /,
    ) -> pd.DataFrame:
        """Normalize the feature columns."""
        df = pd.merge(quarterly, prices, how="outer", left_index=True, right_index=True)
        df = df.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")
        df["PriceBookRatio"] = df["open"] / df["BookRatio"]
        df["PriceEarningsRatio"] = df["open"] / df["EarningsPerShareBasic"]
        df.index.names = ["date"]
        df = utils.resolve_col_order(sql.fundam, df)
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
            >>> finagg.fundam.feat.fundam.from_api("AAPL").head(5)  # doctest: +SKIP
                        PriceBookRatio  PriceEarningsRatio
            date
            2010-01-25        0.175061            2.423509
            2010-01-26        0.178035            2.464677
            2010-01-27        0.178813            2.475447
            2010-01-28        0.177153            2.452471
            2010-01-29        0.173825            2.406396

        """
        start = start or "1776-07-04"
        end = end or utils.today
        quarterly = sec.feat.quarterly.from_api(
            ticker,
            start=start,
            end=end,
        ).reset_index(["fy", "fp"], drop=True)
        start = str(quarterly.index[0])
        prices = yfinance.api.get(
            ticker,
            start=start,
            end=end,
        ).set_index("date")
        return cls._normalize(quarterly, prices)

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
            >>> finagg.fundam.feat.fundam.from_raw("AAPL").head(5)  # doctest: +SKIP
                        PriceBookRatio  PriceEarningsRatio
            date
            2010-01-25        0.175061            2.423509
            2010-01-26        0.178035            2.464677
            2010-01-27        0.178813            2.475447
            2010-01-28        0.177153            2.452471
            2010-01-29        0.173825            2.406396

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
        prices = yfinance.feat.prices.from_raw(
            ticker,
            start=start,
            end=end,
            engine=engine,
        )
        return cls._normalize(quarterly, prices)

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
            >>> finagg.fundam.feat.fundam.from_refined("AAPL").head(5)  # doctest: +SKIP
                        PriceBookRatio  PriceEarningsRatio
            date
            2010-01-25        0.175061            2.423509
            2010-01-26        0.178035            2.464677
            2010-01-27        0.178813            2.475447
            2010-01-28        0.177153            2.452471
            2010-01-29        0.173825            2.406396

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.fundam.name):
            sql.fundam.create(engine)
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
        df = df.drop(columns=["ticker"]).set_index("date").sort_index()
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
            >>> "AAPL" in finagg.fundam.feat.fundam.get_candidate_ticker_set()  # doctest: +SKIP
            True

        """
        return sec.feat.quarterly.get_ticker_set(
            lb=lb, engine=engine
        ) & yfinance.feat.daily.get_ticker_set(lb=lb, engine=engine)

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
            >>> "AAPL" in finagg.fundam.feat.fundam.get_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.fundam.name):
            sql.fundam.create(engine)
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.fundam.c.ticker)
                    .group_by(sql.fundam.c.ticker)
                    .having(sa.func.count(sql.fundam.c.date) >= lb)
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
        """Install data associated with ``tickers`` by pulling data from other
        refined SQL tables, transforming them into fundamental features, and
        then writing to the refined fundamental features SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.sec.feat.Quarterly.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        if not tickers:
            logger.info(
                "Skipping finagg.fundam.feat.fundam installation because no tickers"
                " were provided or no tickers were found with prerequisite data (i.e.,"
                " finagg.yfinance.feat.daily and finagg.sec.feat.quarterly data)"
            )
            return 0

        if recreate_tables or not sa.inspect(engine).has_table(sql.fundam.name):
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
        if not sa.inspect(engine).has_table(sql.fundam.name):
            sql.fundam.create(engine)
        df = df.reset_index("date")
        df["ticker"] = ticker
        with engine.begin() as conn:
            conn.execute(sql.fundam.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


fundam = Fundamental()
"""The most popular way for accessing :class:`finagg.fundam.feat.Fundamental`.

:meta hide-value:
"""
