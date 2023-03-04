"""Features from SEC sources."""

import multiprocessing as mp
from functools import cache, partial
from typing import Literal

import numpy as np
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from tqdm import tqdm

from .. import backend, feat, utils
from . import api, sql


def _refined_quarterly_helper(ticker: str, /) -> tuple[str, pd.DataFrame]:
    """Helper for getting quarterly SEC data in a multiprocessing pool.

    Args:
        ticker: Ticker to create features for.

    Returns:
        The ticker and the returned feature dataframe.

    """
    df = QuarterlyFeatures.from_raw(ticker)
    return ticker, df


def _refined_normalized_quarterly_helper(ticker: str, /) -> tuple[str, pd.DataFrame]:
    """Helper for getting industry-normalized quarterly SEC data in a
    multiprocessing pool.

    Args:
        ticker: Ticker to create features for.

    Returns:
        The ticker and the returned feature dataframe.

    """
    df = NormalizedQuarterlyFeatures.from_other_refined(ticker)
    return ticker, df


def get_unique_filings(
    df: pd.DataFrame, /, *, form: str = "10-Q", units: None | str = None
) -> pd.DataFrame:
    """Get all unique rows as determined by the filing date
    and tag for a period.

    Args:
        df: Dataframe without unique rows.
        form: Only keep rows with form type `form`.
        units: Only keep rows with units `units` if not `None`.

    Returns:
        Dataframe with unique rows.

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


class IndustryQuarterlyFeatures:
    """Methods for gathering industry-averaged quarterly data from SEC
    features.

    """

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
        """Get quarterly features from the feature store,
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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        Raises:
            ValueError if neither a `ticker` nor `code` are provided.

        """
        with engine.begin() as conn:
            if ticker:
                (row,) = conn.execute(
                    sa.select(sql.submissions.c.sic).where(
                        sql.submissions.c.ticker == ticker
                    )
                ).fetchall()
                (sic,) = row
                code = str(sic)[:level]
            elif code:
                code = code[:level]
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
        df = df.pivot(
            index=["fy", "fp", "filed"],
            columns="name",
            values=["avg", "std"],
        ).sort_index()
        return df


class NormalizedQuarterlyFeatures:
    """Quarterly features from SEC EDGAR data normalized according to industry
    averages and standard deviations.

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
            Relative quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        company_df = QuarterlyFeatures.from_refined(
            ticker, start=start, end=end, engine=engine
        ).reset_index(["filed"])
        filed = company_df["filed"]
        industry_df = IndustryQuarterlyFeatures.from_refined(
            ticker=ticker, level=level, start=start, end=end, engine=engine
        ).reset_index(["filed"])
        company_df = (company_df - industry_df["avg"]) / industry_df["std"]
        company_df["filed"] = filed
        pct_change_columns = QuarterlyFeatures.pct_change_target_columns()
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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
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
        df = df.pivot(
            index=["fy", "fp", "filed"], columns="name", values="value"
        ).sort_index()
        df.columns = df.columns.rename(None)
        df = df[QuarterlyFeatures.columns]
        return df

    @classmethod
    def get_candidate_ticker_set(cls, lb: int = 1) -> set[str]:
        """The candidate ticker set is just the `quarterly` ticker set."""
        return QuarterlyFeatures.get_ticker_set(lb=lb)

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
            quarterly features that also have at least `lb` rows.

        """
        with backend.engine.begin() as conn:
            tickers = set()
            for row in conn.execute(
                sa.select(sql.submissions.c.ticker)
                .join(
                    sql.normalized_quarterly,
                    sql.normalized_quarterly.c.cik == sql.submissions.c.cik,
                )
                .group_by(sql.normalized_quarterly.c.cik)
                .having(
                    *[
                        sa.func.count(sql.normalized_quarterly.c.name == col) >= lb
                        for col in QuarterlyFeatures.columns
                    ]
                )
            ):
                (ticker,) = row
                tickers.add(str(ticker))
        return tickers

    @classmethod
    def get_tickers_sorted_by(
        cls,
        column: str,
        /,
        *,
        ascending: bool = True,
        year: int = -1,
        quarter: int = -1,
    ) -> list[str]:
        """Get all tickers in the feature's SQL table sorted by a particular
        column.

        Args:
            column: Feature column to sort by.
            ascending: Whether to return results in ascending order according
                to the values in `column`.
            year: Year to select from. Defaults to the most recent year that
                has data available.
            quarter: Quarter to select from. Defaults to the most recent quarter
                that has data available.

        Returns:
            Tickers sorted by a feature column for a particular year and quarter.

        """
        with backend.engine.begin() as conn:
            if year == -1:
                (row,) = conn.execute(
                    sa.select(sa.func.max(sql.normalized_quarterly.c.fy))
                ).fetchall()
                (max_year,) = row
                year = int(max_year)

            if quarter == -1:
                (row,) = conn.execute(
                    sa.select(sa.func.max(sql.normalized_quarterly.c.fp))
                ).fetchall()
                (max_quarter,) = row
                fp = str(max_quarter)
            else:
                fp = f"Q{quarter}"

            tickers = []
            for row in conn.execute(
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
            ):
                (ticker,) = row
                tickers.append(str(ticker))
        if not ascending:
            tickers = list(reversed(tickers))
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
        sql.normalized_quarterly.drop(backend.engine, checkfirst=True)
        sql.normalized_quarterly.create(backend.engine)

        tickers = cls.get_candidate_ticker_set()
        total_rows = 0
        with (
            tqdm(
                total=len(tickers),
                desc="Installing refined SEC industry-normalized quarterly data",
                position=0,
                leave=True,
            ) as pbar,
            mp.Pool(
                processes=processes,
                initializer=partial(backend.engine.dispose, close=False),
            ) as pool,
        ):
            for ticker, df in pool.imap_unordered(
                _refined_normalized_quarterly_helper, tickers
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

        """
        df = df.reset_index(["fy", "fp", "filed"])
        df = df.melt(["fy", "fp", "filed"], var_name="name", value_name="value")
        df["cik"] = sql.get_cik(ticker)
        with engine.begin() as conn:
            conn.execute(sql.normalized_quarterly.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class QuarterlyFeatures(feat.Features):
    """Quarterly features from SEC EDGAR data."""

    #: Columns within this feature set.
    columns: list[str] = [
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

    #: XBRL disclosure concepts to pull for a company.
    concepts: list[api.Concept] = api.common_concepts

    #: Quarterly features aggregated by industry.
    industry = IndustryQuarterlyFeatures()

    #: A company's quarterly features normalized by its industry.
    normalized = NormalizedQuarterlyFeatures()

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
        cls, ticker: str, /, *, start: str = "1776-07-04", end: str = utils.today
    ) -> pd.DataFrame:
        """Get quarterly features directly from the SEC API.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent quarterly publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
            end: The end date of the observation period.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
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
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get quarterly features from a local SEC SQL table.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent quarterly publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Raw store database engine.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
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
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
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
        df = df.pivot(
            index=["fy", "fp", "filed"],
            columns="name",
            values="value",
        ).sort_index()
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df

    @classmethod
    @cache
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
            All unique tickers that may be valid for creating quarterly features
            that also have at least `lb` rows for each tag used for
            constructing the features.

        """
        with backend.engine.begin() as conn:
            tickers = set()
            for row in conn.execute(
                sa.select(
                    sql.submissions.c.ticker,
                    *[
                        sa.func.sum(
                            sa.case({concept["tag"]: 1}, value=sql.tags.c.tag, else_=0)
                        ).label(concept["tag"])
                        for concept in cls.concepts
                    ],
                )
                .join(sql.tags, sql.tags.c.cik == sql.submissions.c.cik)
                .group_by(sql.tags.c.cik)
                .having(
                    *[sa.text(f"{concept['tag']} >= {lb}") for concept in cls.concepts]
                )
            ):
                ticker = row[0]
                tickers.add(str(ticker))
        return tickers

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
            quarterly features that also have at least `lb` rows.

        """
        with backend.engine.begin() as conn:
            tickers = set()
            for row in conn.execute(
                sa.select(sql.submissions.c.ticker)
                .join(sql.quarterly, sql.quarterly.c.cik == sql.submissions.c.cik)
                .group_by(sql.quarterly.c.cik)
                .having(
                    *[
                        sa.func.count(sql.quarterly.c.name == col) >= lb
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
        sql.quarterly.drop(backend.engine, checkfirst=True)
        sql.quarterly.create(backend.engine)

        tickers = cls.get_candidate_ticker_set()
        total_rows = 0
        with (
            tqdm(
                total=len(tickers),
                desc="Installing refined SEC quarterly data",
                position=0,
                leave=True,
            ) as pbar,
            mp.Pool(
                processes=processes,
                initializer=partial(backend.engine.dispose, close=False),
            ) as pool,
        ):
            for ticker, df in pool.imap_unordered(_refined_quarterly_helper, tickers):
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
        """Write the dataframe to the feature store for ``ticker``.

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
        df = df.reset_index(["fy", "fp", "filed"])
        if set(df.columns) != set(cls.columns):
            raise ValueError(f"Dataframe must have columns {cls.columns}")
        df = df.melt(["fy", "fp", "filed"], var_name="name", value_name="value")
        df["cik"] = sql.get_cik(ticker)
        with engine.begin() as conn:
            conn.execute(sql.quarterly.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class TagFeatures:
    """Get a single company concept tag as-is from raw SEC data."""

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        tag: str,
        /,
        *,
        start: str = "1776-07-04",
        end: str = utils.today,
        engine: Engine = backend.engine,
    ) -> pd.DataFrame:
        """Get a single company concept tag as-is from raw SEC data.

        The tag is in the units it was in when it was originally retrieved
        from the SEC API prior to being stored (this is usually USD).
        This is the preferred method for accessing raw SEC data without
        using the SEC API.

        Args:
            ticker: Company ticker.
            tag: Company concept tag to retreive.
            start: The start date of the observation period.
            end: The end date of the observation period.
            engine: Feature store database engine.

        Returns:
            A dataframe containing the company concept tag values
            across the specified period.

        """
        with engine.begin() as conn:
            df = (
                pd.DataFrame(
                    conn.execute(
                        sa.select(
                            sql.tags.c.fy,
                            sql.tags.c.fp,
                            sql.tags.c.filed,
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
                .set_index(["fy", "fp", "filed"])
                .sort_index()
            )
        return df


#: Module variable intended for fully qualified name usage.
quarterly = QuarterlyFeatures()
tags = TagFeatures()
