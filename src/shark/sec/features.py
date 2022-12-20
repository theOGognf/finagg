"""Features from SEC sources."""

from functools import cache
from typing import Sequence

import pandas as pd
from sqlalchemy import Column, Float, String, Table
from sqlalchemy.sql import and_, distinct, select

from .. import utils
from . import api, sql, store


@cache
def get_ticker_set() -> set[str]:
    """Get all unique tickers in the SQL tables."""
    with sql.engine.connect() as conn:
        ciks = conn.execute(select(distinct(sql.tags.c.cik)))
        tickers = set()
        for cik in ciks:
            (cik,) = cik
            ticker = api.get_ticker(cik)
            tickers.add(ticker)
    return tickers


def get_unique_10q(df: pd.DataFrame, /, *, units: str = "USD") -> pd.DataFrame:
    """Get all unique rows as determined by the
    accession number (`accn`) and tag for each quarter.

    Args:
        df: Dataframe without unique rows.
        units: Only keep rows with units `units` if not `None`.

    Returns:
        Dataframe with unique rows.

    """
    df = df[
        (df["form"] == "10-Q") & (df["units"] == units) & (df["fp"].str.startswith("Q"))
    ]
    return df.drop_duplicates(["accn", "tag"])


class _QuarterlyFeatures:
    """Methods for gathering quarterly data from SEC sources."""

    #: XBRL disclosure concepts to pull for a company.
    concepts = (
        {"tag": "AssetsCurrent", "taxonomy": "us-gaap", "units": "USD"},
        {
            "tag": "EarningsPerShareBasic",
            "taxonomy": "us-gaap",
            "units": "USD/shares",
        },
        {"tag": "InventoryNet", "taxonomy": "us-gaap", "units": "USD"},
        {"tag": "LiabilitiesCurrent", "taxonomy": "us-gaap", "units": "USD"},
        {"tag": "NetIncomeLoss", "taxonomy": "us-gaap", "units": "USD"},
        {"tag": "OperatingIncomeLoss", "taxonomy": "us-gaap", "units": "USD"},
        {"tag": "StockholdersEquity", "taxonomy": "us-gaap", "units": "USD"},
    )

    #: Name of feature store SQL table.
    table_name = "quarterly_features"

    @classmethod
    def _create_table(cls, column_names: Sequence[str]) -> None:
        """Create the feature store SQL table."""
        primary_keys = {"ticker", "filed"}
        table_columns = [
            Column("ticker", String, primary_key=True, doc="Unique company ticker."),
            Column("filed", String, primary_key=True, doc="Filing date."),
        ]

        for name in column_names:
            if name not in primary_keys:
                column = Column(name, Float)
                table_columns.append(column)

        quarterly_features = Table(
            cls.table_name,
            store.metadata,
            *table_columns,
        )
        quarterly_features.create(bind=store.engine)
        store.quarterly_features = quarterly_features

    @classmethod
    def _normalize(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize quarterly features columns."""
        df = (
            df.pivot(index="filed", values="value", columns="tag")
            .fillna(method="ffill")
            .dropna()
            .astype(float)
            .sort_index()
        )
        df["EarningsPerShare"] = df["EarningsPerShareBasic"]
        df["WorkingCapitalRatio"] = df["AssetsCurrent"] / df["LiabilitiesCurrent"]
        df["QuickRatio"] = (df["AssetsCurrent"] - df["InventoryNet"]) / df[
            "LiabilitiesCurrent"
        ]
        df["DebtEquityRatio"] = df["LiabilitiesCurrent"] / df["StockholdersEquity"]
        df["ReturnOnEquity"] = df["NetIncomeLoss"] / df["StockholdersEquity"]
        df["PriceBookRatio"] = df["StockholdersEquity"] / (
            df["AssetsCurrent"] - df["LiabilitiesCurrent"]
        )
        df = utils.quantile_clip(df)
        pct_change_columns = [concept["tag"] for concept in cls.concepts]
        df[pct_change_columns] = df[pct_change_columns].apply(utils.safe_pct_change)
        return df.dropna()

    @classmethod
    @cache
    def from_api(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get quarterly features directly from the SEC API.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent quarterly publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

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
            df = get_unique_10q(df, units=units)
            if start:
                df = df[df["filed"] >= start]
            if end:
                df = df[df["filed"] <= end]
            dfs.append(df)
        df = pd.concat(dfs)
        return cls._normalize(df)

    @classmethod
    @cache
    def from_sql(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
    ) -> pd.DataFrame:
        """Get quarterly features from local SEC SQL tables.

        Not all data series are published at the same rate or
        time. Missing rows for less-frequent quarterly publications
        are forward filled.

        Args:
            ticker: Company ticker.
            start: The start date of the observation period.
                Defaults to the first recorded date.
            end: The end date of the observation period.
                Defaults to the last recorded date.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        with sql.engine.connect() as conn:
            stmt = sql.tags.c.cik == api.get_cik(ticker)
            stmt = and_(
                stmt, sql.tags.c.tag.in_([concept["tag"] for concept in cls.concepts])
            )
            if start:
                stmt = and_(stmt, sql.tags.c.filed >= start)
            if end:
                stmt = and_(stmt, sql.tags.c.filed <= end)
            df = pd.DataFrame(conn.execute(sql.tags.select(stmt)))
        return cls._normalize(df)

    @classmethod
    def from_store(
        cls, ticker: str, /, *, start: None | str = None, end: None | str = None
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

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        table = store.metadata.tables[cls.table_name]
        with store.engine.connect() as conn:
            stmt = table.c.ticker == ticker
            if start:
                stmt = and_(stmt, table.c.filed >= start)
            if end:
                stmt = and_(stmt, table.c.filed <= end)
            df = pd.DataFrame(conn.execute(table.select(stmt)))
        df = df.set_index("filed").drop(columns="ticker")
        return df

    @classmethod
    def to_store(cls, ticker: str, df: pd.DataFrame, /) -> None | int:
        """Write the dataframe to the feature store for `ticker`.

        Does the necessary handling to transform columns to
        prepare the dataframe to be written to a dynamically-defined
        local SQL table.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="filed")
        df["ticker"] = ticker
        if not store.inspector.has_table(cls.table_name):
            cls._create_table(df.columns)
        with store.engine.connect() as conn:
            conn.execute(
                store.quarterly_features.insert(), df.to_dict(orient="records")
            )
        return len(df.index)


#: Public-facing API.
quarterly_features = _QuarterlyFeatures
