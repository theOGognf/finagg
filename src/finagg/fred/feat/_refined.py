"""Refined FRED features (features aggregated from raw tables)."""


import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound

from ... import backend, utils
from .. import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Economic:
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

    @classmethod
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize economic features columns."""
        df = (
            df.pivot(index="date", values="value", columns="series_id")
            .sort_index()
            .astype(float)
            .fillna(method="ffill")
            .dropna()
        )
        df = utils.resolve_func_cols(sql.economic, df, drop=True, inplace=True)
        df.columns = df.columns.rename(None)
        df = utils.resolve_col_order(sql.economic, df)
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
            >>> finagg.fred.feat.economic.from_api().head(5)  # doctest: +SKIP
                        CIVPART  LOG_CHANGE(CPIAUCNS)  LOG_CHANGE(CSUSHPINSA)  FEDFUNDS ...
            date                                                                        ...
            2014-10-06     62.8                   0.0                     0.0      0.09 ...
            2014-10-08     62.8                   0.0                     0.0      0.09 ...
            2014-10-13     62.8                   0.0                     0.0      0.09 ...
            2014-10-15     62.8                   0.0                     0.0      0.09 ...
            2014-10-20     62.8                   0.0                     0.0      0.09 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        dfs = []
        for series_id in api.popular_series:
            df = api.series.observations.get_first_observations(
                series_id,
                observation_start=start,
                observation_end=end,
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
            >>> finagg.fred.feat.economic.from_raw().head(5)  # doctest: +SKIP
                        CIVPART  LOG_CHANGE(CPIAUCNS)  LOG_CHANGE(CSUSHPINSA)  FEDFUNDS ...
            date                                                                        ...
            2014-10-06     62.8                   0.0                     0.0      0.09 ...
            2014-10-08     62.8                   0.0                     0.0      0.09 ...
            2014-10-13     62.8                   0.0                     0.0      0.09 ...
            2014-10-15     62.8                   0.0                     0.0      0.09 ...
            2014-10-20     62.8                   0.0                     0.0      0.09 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.series.name):
            sql.series.create(engine)
        with engine.begin() as conn:
            df = pd.DataFrame(
                conn.execute(
                    sql.series.select().where(
                        sql.series.c.series_id.in_(api.popular_series),
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
            >>> finagg.fred.feat.economic.from_refined().head(5)  # doctest: +SKIP
                        CIVPART  LOG_CHANGE(CPIAUCNS)  LOG_CHANGE(CSUSHPINSA)  FEDFUNDS ...
            date                                                                        ...
            2014-10-06     62.8                   0.0                     0.0      0.09 ...
            2014-10-08     62.8                   0.0                     0.0      0.09 ...
            2014-10-13     62.8                   0.0                     0.0      0.09 ...
            2014-10-15     62.8                   0.0                     0.0      0.09 ...
            2014-10-20     62.8                   0.0                     0.0      0.09 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.economic.name):
            sql.economic.create(engine)
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
        df = df.set_index("date").sort_index()
        return df

    @classmethod
    def install(
        cls,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install economic data by pulling data from the raw SQL tables,
        transforming them into economic features, and then writing to the
        refined economic data SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.economic.name):
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

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.economic.name):
            sql.economic.create(engine)
        df = df.reset_index("date")
        with engine.begin() as conn:
            conn.execute(sql.economic.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)
