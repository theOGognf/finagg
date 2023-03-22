"""Features from FRED sources."""


import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from .. import backend, feat, utils
from . import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class RefinedTimeSummarizedEconomic:
    """Methods for gathering time-averaged economic data from FRED
    features.

    The class variable :data:`finagg.fred.feat.economic.summary` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    """

    @classmethod
    def from_refined(
        cls,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get the average and standard deviation of each series's
        feature across its history.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Average and standard deviation of each economic series
            across its respective history.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        Examples:
            >>> df = finagg.fred.economic.summary.from_refined().head(5)
            >>> df["avg"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06         -6.0827e-06               0.0003 ...
            >>> df["std"]  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            name        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06              0.0017               0.0015 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            stmt = sa.select(
                sql.economic.c.date,
                sql.economic.c.name,
                sa.func.avg(sql.economic.c.value).label("avg"),
                sa.func.std(sql.economic.c.value).label("std"),
            ).group_by(
                sql.economic.c.name,
            )
            df = pd.DataFrame(
                conn.execute(
                    stmt.where(
                        sql.economic.c.date >= start,
                        sql.economic.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No time-summarized economic rows found.")
        df = df.pivot(
            index=["date"],
            columns="name",
            values=["avg", "std"],
        ).sort_index()
        return df


class RefinedNormalizedEconomic:
    """Economic features from FRED data normalized according to historical
    averages.

    The class variable :data:`finagg.fred.feat.economic.normalized` is an
    instance of this feature set implementation and is the most popular
    interface for calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They both return equivalent dataframes.

        >>> df1 = finagg.fred.feat.economic.normalized.from_other_refined().head(5)
        >>> df2 = finagg.fred.feat.economic.normalized.from_refined().head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)

    """

    @classmethod
    def from_other_refined(
        cls,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get features from other feature SQL tables.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Relative economic data dataframe with each series ID as a
            separate column. Sorted by date.

        Examples:
            >>> finagg.fred.feat.economic.normalized.from_other_refined().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06              0.0036              -0.1837 ...
            2014-10-08              0.0000               0.0000 ...
            2014-10-13              0.0000               0.0000 ...
            2014-10-15              0.0000               0.0000 ...
            2014-10-20              0.0000               0.0000 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        economic_df = RefinedEconomic.from_refined(start=start, end=end, engine=engine)
        summarized_df = RefinedTimeSummarizedEconomic.from_refined(
            start=start, end=end, engine=engine
        )
        economic_df = (economic_df - summarized_df["avg"]) / summarized_df["std"]
        economic_df = economic_df.sort_index()
        pct_change_cols = RefinedEconomic.pct_change_target_columns()
        economic_df[pct_change_cols] = economic_df[pct_change_cols].fillna(value=0.0)
        return economic_df.fillna(method="ffill").dropna()

    @classmethod
    def from_refined(
        cls,
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
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        Examples:
            >>> finagg.fred.feat.economic.normalized.from_refined().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06              0.0036              -0.1837 ...
            2014-10-08              0.0000               0.0000 ...
            2014-10-13              0.0000               0.0000 ...
            2014-10-15              0.0000               0.0000 ...
            2014-10-20              0.0000               0.0000 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.normalized_economic.select().where(
                        sql.normalized_economic.c.date >= start,
                        sql.normalized_economic.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No normalized economic rows found.")
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[RefinedEconomic.columns]
        return df

    @classmethod
    def install(cls, *, engine: None | Engine = None) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Args:
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        sql.normalized_economic.drop(engine, checkfirst=True)
        sql.normalized_economic.create(engine)

        df = cls.from_other_refined(engine=engine)
        rowcount = len(df.index)
        if rowcount:
            cls.to_refined(df, engine=engine)
            rowcount += rowcount
            logger.debug(f"{rowcount} normalized economic feature rows inserted")
        else:
            logger.debug("Skipping normalized economic features due to missing data")
        return rowcount

    @classmethod
    def to_refined(
        cls,
        df: pd.DataFrame,
        /,
        *,
        engine: None | Engine = None,
    ) -> int:
        """Write the dataframe to the feature store.

        Args:
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
        if set(df.columns) < set(RefinedEconomic.columns):
            raise ValueError(
                f"Dataframe must have columns {RefinedEconomic.columns} but got {df.columns}"
            )
        df = df.melt("date", var_name="name", value_name="value")
        with engine.begin() as conn:
            conn.execute(sql.normalized_economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class RefinedEconomic(feat.Features):
    """Methods for gathering economic data series from FRED sources.

    The module variable :data:`finagg.fred.feat.economic` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    Examples:
        It doesn't matter which data source you use to gather features.
        They all return equivalent dataframes.

        >>> df1 = finagg.fred.feat.economic.from_api().head(5)
        >>> df2 = finagg.fred.feat.economic.from_raw().head(5)
        >>> df3 = finagg.fred.feat.economic.from_refined().head(5)
        >>> pd.testing.assert_frame_equal(df1, df2, rtol=1e-4)
        >>> pd.testing.assert_frame_equal(df1, df3, rtol=1e-4)

    """

    #: Economic series IDs (typical economic indicators) used for constructing
    #: this feature set.
    series_ids = [
        "CIVPART",  # Labor force participation rate
        "CPIAUCNS",  # Consumer price index
        "CSUSHPINSA",  # S&P/Case-Shiller national home price index
        "FEDFUNDS",  # Federal funds interest rate
        "GDP",  # Gross domestic product
        "GDPC1",  # Real gross domestic product
        "GS10",  # 10-Year treasury yield
        "M2",  # Money stock measures (i.e., savings and related balances)
        "MICH",  # University of Michigan: inflation expectation
        "PSAVERT",  # Personal savings rate
        "UMCSENT",  # University of Michigan: consumer sentiment
        "UNRATE",  # Unemployment rate
        "WALCL",  # US assets, total assets (less eliminations from consolidation)
    ]

    #: Columns within this feature set. Dataframes returned by this class's
    #: methods will always contain these columns. The refined data SQL table
    #: corresponding to these features will also have rows that have these
    #: names.
    columns = [
        "CIVPART_pct_change",
        "CPIAUCNS_pct_change",
        "CSUSHPINSA_pct_change",
        "FEDFUNDS",
        "GDP_pct_change",
        "GDPC1_pct_change",
        "GS10",
        "M2_pct_change",
        "MICH",
        "PSAVERT",
        "UMCSENT_pct_change",
        "UNRATE",
        "WALCL_pct_change",
    ]

    normalized = RefinedNormalizedEconomic()
    """Economic features normalized over time.
    The most popular way for accessing the :class:`RefinedNormalizedEconomic`
    feature set.

    :meta hide-value:
    """

    summary = RefinedTimeSummarizedEconomic()
    """Economic features aggregated over time.
    The most popular way for accessing the :class:`RefinedTimeSummarizedEconomic`
    feature set.

    :meta hide-value:
    """

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize economic features columns."""
        df = (
            df.pivot(index="date", values="value", columns="series_id")
            .sort_index()
            .fillna(method="ffill")
            .dropna()
            .astype(float)
        )
        df[cls.pct_change_target_columns()] = df[cls.pct_change_source_columns()].apply(
            utils.safe_pct_change
        )
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df.dropna()

    @classmethod
    def from_api(
        cls, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get economic features directly from the FRED API.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent economic series
        are forward filled.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Examples:
            >>> finagg.fred.feat.economic.from_api().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06                 0.0                  0.0 ...
            2014-10-08                 0.0                  0.0 ...
            2014-10-13                 0.0                  0.0 ...
            2014-10-15                 0.0                  0.0 ...
            2014-10-20                 0.0                  0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        dfs = []
        for series_id in cls.series_ids:
            df = api.series.observations.get(
                series_id,
                realtime_start=0,
                realtime_end=-1,
                observation_start=start,
                observation_end=end,
                output_type=4,
            )
            dfs.append(df)
        df = pd.concat(dfs)
        return cls._normalize(df)

    @classmethod
    def from_raw(
        cls,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get economic features from local FRED SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent economic series
        are forward filled.

        Args:
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows in the raw SQL table.

        Examples:
            >>> finagg.fred.feat.economic.from_raw().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06                 0.0                  0.0 ...
            2014-10-08                 0.0                  0.0 ...
            2014-10-13                 0.0                  0.0 ...
            2014-10-15                 0.0                  0.0 ...
            2014-10-20                 0.0                  0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.series.select().where(
                        sql.series.c.series_id.in_(cls.series_ids),
                        sql.series.c.date >= start,
                        sql.series.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No economic rows found.")
        return cls._normalize(df)

    @classmethod
    def from_refined(
        cls,
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
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Economic data series dataframe with each series
            as a separate column. Sorted by date.

        Raises:
            `NoResultFound`: If there are no rows in the refined SQL table.

        Examples:
            >>> finagg.fred.feat.economic.from_refined().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        CIVPART_pct_change  CPIAUCNS_pct_change ...
            date                                                ...
            2014-10-06                 0.0                  0.0 ...
            2014-10-08                 0.0                  0.0 ...
            2014-10-13                 0.0                  0.0 ...
            2014-10-15                 0.0                  0.0 ...
            2014-10-20                 0.0                  0.0 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.economic.select().where(
                        sql.economic.c.date >= start, sql.economic.c.date <= end
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No economic rows found.")
        df = df.pivot(index="date", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[cls.columns]
        return df

    @classmethod
    def install(cls, *, engine: None | Engine = None) -> int:
        """Drop the feature's table, create a new one, and insert data
        transformed from another raw SQL table.

        Args:
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        sql.economic.drop(engine, checkfirst=True)
        sql.economic.create(engine)

        df = cls.from_raw(engine=engine)
        rowcount = len(df.index)
        if rowcount:
            cls.to_refined(df, engine=engine)
            rowcount += rowcount
            logger.debug(f"{rowcount} economic feature rows inserted")
        else:
            logger.debug("Skipping economic features due to missing data")
        return rowcount

    @classmethod
    def to_refined(
        cls,
        df: pd.DataFrame,
        /,
        *,
        engine: None | Engine = None,
    ) -> int:
        """Write the dataframe to the feature store.

        Args:
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
        with engine.begin() as conn:
            conn.execute(sql.economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


class RawSeries:
    """Get a single economic series as-is from raw FRED data.

    The module variable :data:`finagg.fred.feat.series` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def from_raw(
        cls,
        series_id: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get a single economic data series as-is from raw FRED data.

        This is the preferred method for accessing raw FRED data without
        using the FRED API.

        Args:
            series_id: Economic data series ID.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            A dataframe containing the economic data series values
            across the specified period.

        Raises:
            `NoResultFound`: If there are no rows for ``series_id`` in the
                raw SQL table.

        Examples:
            >>> finagg.fred.feat.series.from_raw("CPIAUCNS").head(5)  # doctest: +NORMALIZE_WHITESPACE
                        value
            date
            1949-03-01  169.5
            1949-04-01  169.7
            1949-05-01  169.2
            1949-06-01  169.6
            1949-07-01  168.5

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sa.select(sql.series.c.date, sql.series.c.value).where(
                        sql.series.c.series_id == series_id,
                        sql.series.c.date >= start,
                        sql.series.c.date <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No series rows found for {series_id}.")
        return df.set_index(["date"]).sort_index()

    @classmethod
    def install(
        cls, series_ids: None | set[str] = None, *, engine: None | Engine = None
    ) -> int:
        """Drop the feature's table, create a new one, and insert data
        as-is from the FRED API.

        Args:
            series_ids: Set of series to install features for. Defaults to all
                the series from :data:`finagg.fred.feat.economic.series_ids`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        series_ids = series_ids or set(economic.series_ids)
        engine = engine or backend.engine
        sql.series.drop(engine, checkfirst=True)
        sql.series.create(engine)

        total_rows = 0
        for series_id in tqdm(
            series_ids,
            desc="Installing raw FRED economic series data",
            position=0,
            leave=True,
        ):
            try:
                df = api.series.observations.get(
                    series_id,
                    realtime_start=0,
                    realtime_end=-1,
                    output_type=4,
                )
                rowcount = len(df.index)
                if rowcount:
                    cls.to_raw(df, engine=engine)
                    total_rows += rowcount
                    logger.debug(f"{rowcount} rows inserted for {series_id}")
                else:
                    logger.debug(f"Skipping {series_id} due to missing data")
            except Exception as e:
                logger.debug(f"Skipping {series_id}", exc_info=e)
        return total_rows

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
            conn.execute(sql.series.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)


economic = RefinedEconomic()
"""The most popular way for accessing :class:`RefinedEconomic`.

:meta hide-value:
"""

series = RawSeries()
"""The most popular way for accessing :class:`RawSeries`.

:meta hide-value:
"""
