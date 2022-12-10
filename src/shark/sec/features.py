"""Features from SEC sources."""

import pandas as pd
from sqlalchemy.sql import and_

from .api import api
from .sql import engine
from .sql import tags as tags_table


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


class QuarterlyFeatures:
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
            df = get_unique_10q(df)
            if start:
                df = df[df["filed"] >= start]
            if end:
                df = df[df["filed"] <= end]
            dfs.append(df)
        dfs = pd.concat(dfs)
        return (
            dfs.pivot(index="filed", values="value", columns="tag")
            .sort_index()
            .fillna(method="ffill")
            .dropna()
        )

    @classmethod
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
        with engine.connect() as conn:
            stmt = tags_table.c.cik == api.get_cik(ticker)
            stmt = and_(
                stmt, tags_table.c.tag.in_([concept["tag"] for concept in cls.concepts])
            )
            if start:
                stmt = and_(stmt, tags_table.c.filed >= start)
            if end:
                stmt = and_(stmt, tags_table.c.filed <= end)
            df = pd.DataFrame(conn.execute(tags_table.select(stmt)))
        return (
            df.pivot(index="filed", values="value", columns="tag")
            .sort_index()
            .fillna(method="ffill")
            .dropna()
        )
