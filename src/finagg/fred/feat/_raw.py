"""Raw features from FRED sources."""

import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from ... import backend, utils
from .. import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Series:
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
            >>> finagg.fred.feat.series.from_raw("CPIAUCNS").head(5)  # doctest: +SKIP
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
        if not sa.inspect(engine).has_table(sql.series.name):
            sql.series.create(engine)
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
        return df.set_index("date").sort_index()

    @classmethod
    def get_id_set(cls, lb: int = 1, *, engine: None | Engine = None) -> set[str]:
        """Get all unique economic series IDs in the raw SQL tables that have at least
        ``lb`` rows.

        Args:
            lb: Lower bound number of rows that a series must have for its ID
                to be included in the set returned by this method.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Examples:
            >>> "FEDFUNDS" in finagg.fred.feat.series.get_id_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.series.name):
            sql.series.create(engine)
        with engine.begin() as conn:
            series_ids = (
                conn.execute(
                    sa.select(sql.series.c.series_id)
                    .group_by(sql.series.c.series_id)
                    .having(sa.func.count(sql.series.c.date) >= lb)
                )
                .scalars()
                .all()
            )
        return set(series_ids)

    @classmethod
    def install(
        cls,
        series_ids: None | set[str] = None,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install data associated with by pulling data from the FRED API and
        then writing the data to the raw series SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            series_ids: Set of series to install features for. Defaults to all
                the series from :attr:`finagg.fred.feat.Series.get_id_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        series_ids = series_ids or set(api.popular_series)
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.series.name):
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
                df = api.series.observations.get_first_observations(
                    series_id,
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
        if not sa.inspect(engine).has_table(sql.series.name):
            sql.series.create(engine)
        with engine.begin() as conn:
            conn.execute(sql.series.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)
