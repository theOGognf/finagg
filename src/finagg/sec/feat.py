"""Features from SEC sources."""

import logging
from typing import Literal

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


def get_unique_filings(
    df: pd.DataFrame, /, *, form: str = "10-Q", units: None | str = None
) -> pd.DataFrame:
    """Get all unique rows as determined by the filing date and tag for a
    period.

    Args:
        df: Dataframe without unique rows.
        form: Only keep rows with form type ``form``. Most popular choices
            include ``"10-K"`` for annual and ``"10-Q"`` for quarterly.
        units: Only keep rows with units ``units`` if not ``None``.

    Returns:
        Dataframe with unique rows.

    Examples:
        Only get a company's original quarterly earnings-per-share filings.

        >>> df = finagg.sec.api.company_concept.get(
        ...     "EarningsPerShareBasic",
        ...     ticker="AAPL",
        ...     taxonomy="us-gaap",
        ...     units="USD/shares",
        ... )
        >>> finagg.sec.feat.get_unique_filings(
        ...     df,
        ...     form="10-Q",
        ...     units="USD/shares"
        ... ).head(5)  # doctest: +ELLIPSIS
             fy  fp  ...
        0  2009  Q3  ...
        1  2010  Q1  ...
        2  2010  Q2  ...
        3  2010  Q3  ...
        4  2011  Q1  ...

    """
    mask = df["form"] == form
    match form:
        case "10-K":
            mask &= df["fp"] == "FY"
        case "10-Q":
            mask &= df["fp"].str.startswith("Q")
    if units:
        mask &= df["units"] == units
    df = df[mask]
    return (
        df.sort_values(["fy", "fp", "filed"])
        .groupby(["fy", "fp", "tag"], as_index=False)
        .first()
    )


class RefinedIndustryQuarterly:
    """Methods for gathering industry-averaged quarterly data from SEC
    features.

    The class variable :data:`finagg.sec.feat.quarterly.industry` is an
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
            >>> df["avg"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2009 Q3 2009-10-30                    0.0000           0.5733            3.0650 ...
            2010 Q1 2010-04-29                   -0.0122           0.4025            0.8650 ...
                 Q2 2010-07-30                    0.0000           0.5003            0.5386 ...
                 Q3 2010-11-04                    0.0011           0.4568            1.2038 ...
            2011 Q1 2011-05-05                    0.2716           0.4652            0.9920 ...
            >>> df["std"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2009 Q3 2009-10-30                    0.0000           0.2428            0.5850 ...
            2010 Q1 2010-04-29                    0.0434           0.1490            0.9865 ...
                 Q2 2010-07-30                    0.0000           0.2575            2.5274 ...
                 Q3 2010-11-04                    0.0030           0.2672            2.6892 ...
            2011 Q1 2011-05-05                    0.1827           0.2859            0.9541 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
                    sa.select(
                        sql.quarterly.c.fy,
                        sql.quarterly.c.fp,
                        sa.func.max(sql.quarterly.c.filed).label("filed"),
                        sql.quarterly.c.name,
                        sa.func.avg(sql.quarterly.c.value).label("avg"),
                        sa.func.std(sql.quarterly.c.value).label("std"),
                    )
                    .join(
                        sql.submissions,
                        (sql.submissions.c.cik == sql.quarterly.c.cik)
                        & (sql.submissions.c.sic.startswith(code)),
                    )
                    .group_by(
                        sql.quarterly.c.fy,
                        sql.quarterly.c.fp,
                        sql.quarterly.c.name,
                    )
                    .where(
                        sql.quarterly.c.filed >= start,
                        sql.quarterly.c.filed <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(
                f"No industry quarterly rows found for industry {code}."
            )
        df = df.pivot(
            index=["fy", "fp", "filed"],
            columns="name",
            values=["avg", "std"],
        ).sort_index()
        return df


class RefinedNormalizedQuarterly:
    """Quarterly features from SEC EDGAR data normalized according to industry
    averages and standard deviations.

    The class variable :data:`finagg.sec.feat.quarterly.normalized` is an
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
            >>> finagg.sec.feat.quarterly.normalized.from_other_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2010 Q1 2010-01-25                   -0.2573          -0.2606            1.6980 ...
                 Q2 2010-04-21                    0.0000          -0.5309            1.5081 ...
                 Q3 2010-07-21                   -0.3780          -0.3485            1.9323 ...
            2011 Q1 2011-01-19                    0.2693          -0.1107            2.8801 ...
                 Q2 2011-04-21                    0.0000          -0.0655            2.8997 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        company_df = RefinedQuarterly.from_refined(
            ticker, start=start, end=end, engine=engine
        ).reset_index(["filed"])
        filed = company_df["filed"]
        industry_df = RefinedIndustryQuarterly.from_refined(
            ticker=ticker, level=level, start=start, end=end, engine=engine
        ).reset_index(["filed"])
        company_df = (company_df - industry_df["avg"]) / industry_df["std"]
        company_df["filed"] = filed
        pct_change_columns = RefinedQuarterly.pct_change_target_columns()
        company_df[pct_change_columns] = company_df[pct_change_columns].fillna(
            value=0.0
        )
        return (
            company_df.fillna(method="ffill")
            .dropna()
            .reset_index()
            .drop_duplicates("filed")
            .set_index(["fy", "fp", "filed"])
            .sort_index()
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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                refined SQL table.

        Examples:
            >>> finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2010 Q1 2010-01-25                   -0.2573          -0.2606            1.6980 ...
                 Q2 2010-04-21                    0.0000          -0.5309            1.5081 ...
                 Q3 2010-07-21                   -0.3780          -0.3485            1.9323 ...
            2011 Q1 2011-01-19                    0.2693          -0.1107            2.8801 ...
                 Q2 2011-04-21                    0.0000          -0.0655            2.8997 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        df = df.pivot(
            index=["fy", "fp", "filed"], columns="name", values="value"
        ).sort_index()
        df.columns = df.columns.rename(None)
        df = df[RefinedQuarterly.columns]
        return df

    @classmethod
    def get_candidate_ticker_set(
        cls, lb: int = 1, *, engine: None | Engine = None
    ) -> set[str]:
        """Get all unique tickers in the quarterly SQL table that MAY BE
        ELIGIBLE to be in the feature's SQL table.

        This is just an alias for :meth:`finagg.sec.feat.quarterly.get_ticker_set`.

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
            >>> "AAPL" in finagg.sec.feat.quarterly.normalized.get_candidate_ticker_set()
            True

        """
        return RefinedQuarterly.get_ticker_set(lb=lb, engine=engine)

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
            >>> "AAPL" in finagg.sec.feat.quarterly.normalized.get_ticker_set()
            True

        """
        engine = engine or backend.engine
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.submissions.c.ticker)
                    .join(
                        sql.normalized_quarterly,
                        sql.normalized_quarterly.c.cik == sql.submissions.c.cik,
                    )
                    .group_by(sql.normalized_quarterly.c.cik)
                    .having(
                        *[
                            sa.func.count(sql.normalized_quarterly.c.name == col) >= lb
                            for col in RefinedQuarterly.columns
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
            ...         "EarningsPerShare",
            ...         year=2020,
            ...         quarter=3
            ... )
            >>> "PCGU" == ts[0]
            True

        """
        engine = engine or backend.engine
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
                        sql.normalized_quarterly.c.name == column,
                        sql.normalized_quarterly.c.fy == year,
                        sql.normalized_quarterly.c.fp == fp,
                    )
                    .order_by(sql.normalized_quarterly.c.value)
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
                the candidate tickers from
                :meth:`RefinedNormalizedQuarterly.get_candidate_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        engine = engine or backend.engine
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

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        engine = engine or backend.engine
        df = df.reset_index(["fy", "fp", "filed"])
        if set(df.columns) < set(RefinedQuarterly.columns):
            raise ValueError(
                f"Dataframe must have columns {RefinedQuarterly.columns} but got {df.columns}"
            )
        df = df.melt(["fy", "fp", "filed"], var_name="name", value_name="value")
        df["cik"] = sql.get_cik(ticker, engine=engine)
        with engine.begin() as conn:
            conn.execute(sql.normalized_quarterly.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class RefinedQuarterly(feat.Features):
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

    #: Columns within this feature set. Dataframes returned by this class's
    #: methods will always contain these columns. The refined data SQL table
    #: corresponding to these features will also have rows that have these
    #: names.
    columns = [
        "AssetsCurrent_pct_change",
        "DebtEquityRatio",
        "EarningsPerShare",
        "InventoryNet_pct_change",
        "LiabilitiesCurrent_pct_change",
        "NetIncomeLoss_pct_change",
        "OperatingIncomeLoss_pct_change",
        "PriceBookRatio",
        "QuickRatio",
        "ReturnOnEquity",
        "StockholdersEquity_pct_change",
        "WorkingCapitalRatio",
    ]

    concepts = api.popular_concepts
    """XBRL disclosure concepts to pull to construct the columns in this
    feature set.

    :meta hide-value:
    """

    industry = RefinedIndustryQuarterly()
    """Quarterly features aggregated for an entire industry.
    The most popular way for accessing the :class:`RefinedIndustryQuarterly`
    feature set.

    :meta hide-value:
    """

    normalized = RefinedNormalizedQuarterly()
    """A company's quarterly features normalized by its industry.
    The most popular way for accessing the :class:`RefinedNormalizedQuarterly`
    feature set.

    :meta hide-value:
    """

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize quarterly features columns."""
        df = df.set_index(["fy", "fp"]).sort_index()
        df["filed"] = df.groupby(["fy", "fp"])["filed"].max()
        df = df.reset_index()
        df = df.pivot(
            index=["fy", "fp", "filed"],
            columns="tag",
            values="value",
        ).astype(float)
        df["EarningsPerShare"] = df["EarningsPerShareBasic"]
        df["DebtEquityRatio"] = df["LiabilitiesCurrent"] / df["StockholdersEquity"]
        df["PriceBookRatio"] = df["StockholdersEquity"] / (
            df["AssetsCurrent"] - df["LiabilitiesCurrent"]
        )
        df["QuickRatio"] = (df["AssetsCurrent"] - df["InventoryNet"]) / df[
            "LiabilitiesCurrent"
        ]
        df["ReturnOnEquity"] = df["NetIncomeLoss"] / df["StockholdersEquity"]
        df["WorkingCapitalRatio"] = df["AssetsCurrent"] / df["LiabilitiesCurrent"]
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
            >>> finagg.sec.feat.quarterly.from_api("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2010 Q1 2010-01-25                   -0.0234           0.3637              2.54 ...
                 Q2 2010-04-21                    0.0000           0.3637              4.35 ...
                 Q3 2010-07-21                    0.0000           0.3637              6.40 ...
            2011 Q1 2011-01-19                    0.3208           0.4336              3.74 ...
                 Q2 2011-04-21                    0.0000           0.4336              7.12 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        dfs = []
        for concept in cls.concepts:
            tag = concept["tag"]
            taxonomy = concept["taxonomy"]
            units = concept["units"]
            df = api.company_concept.get(
                tag, ticker=ticker, taxonomy=taxonomy, units=units
            )
            df = get_unique_filings(df, units=units)
            df = df[(df["filed"] >= start) & (df["filed"] <= end)]
            dfs.append(df)
        df = pd.concat(dfs)
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

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the
                raw SQL table.

        Examples:
            >>> finagg.sec.feat.quarterly.from_raw("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2010 Q1 2010-01-25                   -0.0234           0.3637              2.54 ...
                 Q2 2010-04-21                    0.0000           0.3637              4.35 ...
                 Q3 2010-07-21                    0.0000           0.3637              6.40 ...
            2011 Q1 2011-01-19                    0.3208           0.4336              3.74 ...
                 Q2 2011-04-21                    0.0000           0.4336              7.12 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.tags.select()
                    .join(
                        sql.submissions,
                        (sql.submissions.c.cik == sql.tags.c.cik)
                        & (sql.submissions.c.ticker == ticker),
                    )
                    .where(
                        sql.tags.c.tag.in_(
                            [concept["tag"] for concept in cls.concepts]
                        ),
                        sql.tags.c.filed >= start,
                        sql.tags.c.filed <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No quarterly rows found for {ticker}.")
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
            >>> finagg.sec.feat.quarterly.from_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
            fy   fp filed                                                                   ...
            2010 Q1 2010-01-25                   -0.0234           0.3637              2.54 ...
                 Q2 2010-04-21                    0.0000           0.3637              4.35 ...
                 Q3 2010-07-21                    0.0000           0.3637              6.40 ...
            2011 Q1 2011-01-19                    0.3208           0.4336              3.74 ...
                 Q2 2011-04-21                    0.0000           0.4336              7.12 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
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
        df = df.pivot(
            index=["fy", "fp", "filed"],
            columns="name",
            values="value",
        ).sort_index()
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
            All unique tickers that may be valid for creating quarterly features
            that also have at least ``lb`` rows for each tag used for
            constructing the features.

        Examples:
            >>> "AAPL" in finagg.sec.feat.quarterly.get_candidate_ticker_set()
            True

        """
        engine = engine or backend.engine
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
                            for concept in cls.concepts
                        ],
                    )
                    .join(sql.tags, sql.tags.c.cik == sql.submissions.c.cik)
                    .group_by(sql.tags.c.cik)
                    .having(
                        *[
                            sa.text(f"{concept['tag']} >= {lb}")
                            for concept in cls.concepts
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
        with engine.begin() as conn:
            tickers = (
                conn.execute(
                    sa.select(sql.submissions.c.ticker)
                    .join(sql.quarterly, sql.quarterly.c.cik == sql.submissions.c.cik)
                    .group_by(sql.quarterly.c.cik)
                    .having(
                        *[
                            sa.func.count(sql.quarterly.c.name == col) >= lb
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
                the candidate tickers from
                :meth:`RefinedQuarterly.get_candidate_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or cls.get_candidate_ticker_set(engine=engine)
        engine = engine or backend.engine
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

        Raises:
            `ValueError`: If the given dataframe's columns do not match this
                feature's columns.

        """
        engine = engine or backend.engine
        df = df.reset_index(["fy", "fp", "filed"])
        if set(df.columns) < set(cls.columns):
            raise ValueError(
                f"Dataframe must have columns {cls.columns} but got {df.columns}"
            )
        df = df.melt(["fy", "fp", "filed"], var_name="name", value_name="value")
        df["cik"] = sql.get_cik(ticker, engine=engine)
        with engine.begin() as conn:
            conn.execute(sql.quarterly.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class RawSubmissions:
    """Get a single company's metadata as-is from raw SEC data.

    The module variable :data:`finagg.sec.feat.submissions` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def install(
        cls, tickers: None | set[str] = None, *, engine: None | Engine = None
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        as-is from the SEC API.

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
        sql.submissions.drop(engine, checkfirst=True)
        sql.submissions.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing raw SEC submissions data",
            position=0,
            leave=True,
        ):
            try:
                metadata = api.submissions.get(ticker=ticker)["metadata"]
                df = pd.DataFrame(metadata, index=[0])
                rowcount = len(df.index)
                if rowcount:
                    cls.to_raw(df, engine=engine)
                    total_rows += rowcount
                    logger.debug(f"{rowcount} rows inserted for {ticker}")
                else:
                    logger.debug(f"Skipping {ticker} due to missing submissions")
            except Exception as e:
                logger.debug(f"Skipping {ticker}", exc_info=e)
        return total_rows

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        /,
        *,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get a single company's metadata as-is from raw SEC data.

        The metadata provided for each company varies and each company's
        metadata may be incomplete. Only their SEC CIK and SIC industry
        code are guaranteed to be provided.

        Args:
            ticker: Company ticker.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            A dataframe containing the company's metadata.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` in the raw
                SQL table.

        Examples:
            >>> finagg.sec.feat.submissions.from_raw("AAPL")  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                      cik ticker entity_type   sic sic_description ...
            0  0000320193   AAPL        None  3571            None ...

        """
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.submissions.select().where(sql.submissions.c.ticker == ticker)
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No rows found for {ticker}.")
        return df

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
            conn.execute(sql.submissions.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)


class RawTags:
    """Get a single company concept tag as-is from raw SEC data.

    The module variable :data:`finagg.sec.feat.tags` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def install(
        cls, tickers: None | set[str] = None, *, engine: None | Engine = None
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        as-is from the SEC API.

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
        sql.tags.drop(engine, checkfirst=True)
        sql.tags.create(engine)

        total_rows = 0
        for ticker in tqdm(
            tickers,
            desc="Installing raw SEC tags data",
            position=0,
            leave=True,
        ):
            for concept in api.popular_concepts:
                tag = concept["tag"]
                taxonomy = concept["taxonomy"]
                units = concept["units"]
                try:
                    df = api.company_concept.get(
                        tag,
                        ticker=ticker,
                        taxonomy=taxonomy,
                        units=units,
                    )
                    df = get_unique_filings(df, form="10-Q", units=units)
                    rowcount = len(df.index)
                    if rowcount:
                        cls.to_raw(df, engine=engine)
                        total_rows += rowcount
                        logger.debug(f"{rowcount} rows inserted for {ticker} tag {tag}")
                    else:
                        logger.debug(f"Skipping {ticker} due to missing filings")
                except Exception as e:
                    logger.debug(f"Skipping {ticker}", exc_info=e)
        return total_rows

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        tag: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get a single company concept tag as-is from raw SEC data.

        This is the preferred method for accessing raw SEC data without
        using the SEC API.

        Args:
            ticker: Company ticker.
            tag: Company concept tag to retreive.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            A dataframe containing the company concept tag values
            across the specified period.

        Raises:
            `NoResultFound`: If there are no rows for ``ticker`` and ``tag``
                in the raw SQL table.

        Examples:
            >>> finagg.sec.feat.tags.from_raw("AAPL", "EarningsPerShareBasic").head(5)  # doctest: +NORMALIZE_WHITESPACE
                                     units  value
            fy   fp filed
            2009 Q3 2009-07-22  USD/shares   4.20
            2010 Q1 2010-01-25  USD/shares   2.54
                 Q2 2010-04-21  USD/shares   4.35
                 Q3 2010-07-21  USD/shares   6.40
            2011 Q1 2011-01-19  USD/shares   3.74

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sa.select(
                        sql.tags.c.fy,
                        sql.tags.c.fp,
                        sql.tags.c.filed,
                        sql.tags.c.units,
                        sql.tags.c.value,
                    )
                    .join(
                        sql.submissions,
                        (sql.submissions.c.cik == sql.tags.c.cik)
                        & (sql.submissions.c.ticker == ticker),
                    )
                    .where(
                        sql.tags.c.tag == tag,
                        sql.tags.c.filed >= start,
                        sql.tags.c.filed <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No {tag} rows found for {ticker}.")
        return df.set_index(["fy", "fp", "filed"]).sort_index()

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
            conn.execute(sql.tags.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)


quarterly = RefinedQuarterly()
"""The most popular way for accessing :class:`RefinedQuarterly`.

:meta hide-value:
"""

submissions = RawSubmissions()
"""The most popular way for accessing :class:`RawSubmissions`.

:meta hide-value:
"""

tags = RawTags()
"""The most popular way for accessing :class:`RawTags`.

:meta hide-value:
"""
