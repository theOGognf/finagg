"""Raw features from SEC sources."""

import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoResultFound
from tqdm import tqdm

from ... import backend, indices, utils
from .. import api, sql

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Submissions:
    """Get a single company's metadata as-is from raw SEC data.

    The module variable :data:`finagg.sec.feat.submissions` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def install(
        cls,
        tickers: None | set[str] = None,
        *,
        engine: None | Engine = None,
        recreate_tables: bool = False,
    ) -> int:
        """Install data associated with ``tickers`` by pulling data from the
        API, and then writing the data to the raw submissions SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.indices.api.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or indices.api.get_ticker_set()
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.submissions.name):
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
            >>> finagg.sec.feat.submissions.from_raw("AAPL")  # doctest: +SKIP
                      cik ticker entity_type   sic sic_description ...
            0  0000320193   AAPL        None  3571            None ...

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
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
    def get_ticker_set(
        cls,
        *,
        engine: None | Engine = None,
    ) -> set[str]:
        """Get all unique ticker symbols in the raw SQL submissions table.

        This method is convenient for accessing the tickers that have raw SQL data
        associated with them so the data associated with those tickers can be
        further refined. A common pattern is to use this method and other
        ``get_ticker_set`` methods (such as those found in :mod:`finagg.sec.feat`)
        to determine which tickers are missing data from other tables or features.

        Args:
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Examples:
            >>> "AAPL" in finagg.sec.feat.submissions.get_ticker_set()  # doctest: +SKIP
            True

        """
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        with engine.begin() as conn:
            tickers = conn.execute(sa.select(sql.submissions.c.ticker)).scalars().all()
        return set(tickers)

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
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        with engine.begin() as conn:
            conn.execute(sql.submissions.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)


class Tags:
    """Get a single company concept tag as-is from raw SEC data.

    The module variable :data:`finagg.sec.feat.tags` is an instance of
    this feature set implementation and is the most popular interface for
    calling feature methods.

    """

    @classmethod
    def from_raw(
        cls,
        ticker: str,
        tag: str,
        /,
        *,
        form: str = "10-Q",
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
            form: SEC filing form to retrieve rows for. Options include:

                - "10-Q" = quarterly filings
                - "10-K" = annual filings

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
            >>> finagg.sec.feat.tags.from_raw("AAPL", "EarningsPerShareBasic").head(5)  # doctest: +SKIP
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
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.tags.name):
            sql.tags.create(engine)
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
                        sql.tags.c.form == form,
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
    def get_ticker_set(
        cls,
        lb: int = 1,
        *,
        engine: None | Engine = None,
    ) -> set[str]:
        """Get all unique ticker symbols in the raw SQL tables that have at least
        ``lb`` rows.

        This method is convenient for accessing the tickers that have raw SQL data
        associated with them so the data associated with those tickers can be
        further refined. A common pattern is to use this method and other
        ``get_ticker_set`` methods (such as those found in :mod:`finagg.sec.feat`)
        to determine which tickers are missing data from other tables or features.

        Args:
            lb: Lower bound number of rows that a company must have for its ticker
                to be included in the set returned by this method.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.

        Examples:
            >>> "AAPL" in finagg.sec.feat.tags.get_ticker_set()  # doctest: +SKIP
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
                    sa.select(sql.submissions.c.ticker)
                    .join(sql.tags, sql.tags.c.cik == sql.submissions.c.cik)
                    .group_by(sql.tags.c.cik)
                    .having(sa.func.count(sql.tags.c.filed) >= lb)
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
        API, and then writing the data to the raw tags SQL table.

        Tables associated with this method are created if they don't already
        exist.

        Args:
            tickers: Set of tickers to install features for. Defaults to all
                the tickers from :meth:`finagg.indices.api.get_ticker_set`.
            engine: Feature store database engine. Defaults to the engine
                at :data:`finagg.backend.engine`.
            recreate_tables: Whether to drop and recreate tables, wiping all
                previously installed data.

        Returns:
            Number of rows written to the feature's SQL table.

        """
        tickers = tickers or indices.api.get_ticker_set()
        engine = engine or backend.engine
        if recreate_tables or not sa.inspect(engine).has_table(sql.tags.name):
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
                    for form in ("10-K", "10-Q"):
                        df_unique = api.get_unique_filings(df, form=form, units=units)
                        rowcount = len(df_unique.index)
                        if rowcount:
                            cls.to_raw(df_unique, engine=engine)
                            total_rows += rowcount
                            logger.debug(
                                f"{rowcount} rows inserted for"
                                f" {ticker} {tag} {form} filings"
                            )
                        else:
                            logger.debug(
                                f"Skipping {ticker} due to missing {tag} {form} filings"
                            )
                except Exception as e:
                    logger.debug(f"Skipping {ticker}", exc_info=e)
        return total_rows

    @classmethod
    def join_from_raw(
        cls,
        ticker: str,
        tags: list[str],
        /,
        *,
        form: str = "10-Q",
        start: None | str = None,
        end: None | str = None,
        engine: None | Engine = None,
    ) -> pd.DataFrame:
        """Get one or more company concept tags from raw SEC data.

        Joins all the tags into one table, pivoting the columns such that
        each tag is in its own column. Tags are forward-filled to fill
        gaps.

        Args:
            ticker: Company ticker.
            tags: Company concept tags to retreive.
            form: SEC filing form to retrieve rows for. Options include:

                - "10-Q" = quarterly filings
                - "10-K" = annual filings

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
            `NoResultFound`: If there are no rows for ``ticker`` or any of
                the tags in ``tags`` for ``ticker`` in the raw SQL table.

        Examples:
            >>> finagg.sec.feat.tags.join_from_raw(
            ...     "AAPL",
            ...     ["Assets", "EarningsPerShareBasic"],
            ...     form="10-Q"
            ... ).head(5)  # doctest: +SKIP
                                      Assets  EarningsPerShareBasic
            fy   fp filed
            2009 Q3 2009-07-22  3.957200e+10                   4.20
            2010 Q1 2010-01-25  4.750100e+10                   2.54
                 Q2 2010-04-21  4.750100e+10                   4.35
                 Q3 2010-07-21  4.750100e+10                   6.40
            2011 Q1 2011-01-19  7.518300e+10                   3.74

        """
        start = start or "1776-07-04"
        end = end or utils.today
        engine = engine or backend.engine
        if not sa.inspect(engine).has_table(sql.submissions.name):
            sql.submissions.create(engine)
        if not sa.inspect(engine).has_table(sql.tags.name):
            sql.tags.create(engine)
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
                        sql.tags.c.tag.in_(tags),
                        sql.tags.c.form == form,
                        sql.tags.c.filed >= start,
                        sql.tags.c.filed <= end,
                    )
                )
            )
        if not len(df.index):
            raise NoResultFound(f"No rows found for {ticker}.")
        df = api.join_filings(df, form=form)
        for tag in tags:
            if tag not in df.columns:
                raise NoResultFound(f"No {tag} rows found for {ticker}.")
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
        if not sa.inspect(engine).has_table(sql.tags.name):
            sql.tags.create(engine)
        with engine.begin() as conn:
            conn.execute(sql.tags.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df)
