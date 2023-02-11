"""Features from SEC sources."""

import pandas as pd
from sqlalchemy.engine import Engine

from .. import utils
from . import api, sql, store


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
    return df.drop_duplicates(["tag", "filed"])


class _QuarterlyFeatures:
    """Methods for gathering quarterly data from SEC sources."""

    #: Columns within this feature set.
    columns = (
        "AssetsCurrent",
        "DebtEquityRatio",
        "EarningsPerShareBasic",
        "InventoryNet",
        "LiabilitiesCurrent",
        "NetIncomeLoss",
        "OperatingIncomeLoss",
        "PriceBookRatio",
        "QuickRatio",
        "ReturnOnEquity",
        "StockholdersEquity",
        "WorkingCapitalRatio",
    )

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
    def _normalize(cls, df: pd.DataFrame, /) -> pd.DataFrame:
        """Normalize quarterly features columns."""
        df = (
            df.pivot(index="filed", values="value", columns="tag")
            .fillna(method="ffill")
            .dropna()
            .astype(float)
            .sort_index()
        )
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
        df = utils.quantile_clip(df)
        pct_change_columns = [concept["tag"] for concept in cls.concepts]
        df[pct_change_columns] = df[pct_change_columns].apply(utils.safe_pct_change)
        df.columns = df.columns.rename(None)
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
            df = get_unique_filings(df, units=units)
            if start:
                df = df[df["filed"] >= start]
            if end:
                df = df[df["filed"] <= end]
            dfs.append(df)
        df = pd.concat(dfs)
        return cls._normalize(df)

    @classmethod
    def from_sql(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = sql.engine,
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
            engine: Raw store database engine.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        table = sql.tags
        with engine.begin() as conn:
            stmt = table.c.cik == api.get_cik(ticker)
            stmt &= table.c.tag.in_([concept["tag"] for concept in cls.concepts])
            if start:
                stmt &= table.c.filed >= start
            if end:
                stmt &= table.c.filed <= end
            df = pd.DataFrame(conn.execute(table.select().where(stmt)))
        return cls._normalize(df)

    @classmethod
    def from_store(
        cls,
        ticker: str,
        /,
        *,
        start: None | str = None,
        end: None | str = None,
        engine: Engine = store.engine,
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
            engine: Feature store database engine.

        Returns:
            Quarterly data dataframe with each tag as a
            separate column. Sorted by filing date.

        """
        table = store.quarterly_features
        with engine.begin() as conn:
            stmt = table.c.ticker == ticker
            if start:
                stmt &= table.c.filed >= start
            if end:
                stmt &= table.c.filed <= end
            df = pd.DataFrame(conn.execute(table.select().where(stmt)))
        df = df.pivot(index="filed", values="value", columns="name").sort_index()
        df.columns = df.columns.rename(None)
        df = df[list(cls.columns)]
        return df

    @classmethod
    def to_store(
        cls,
        ticker: str,
        df: pd.DataFrame,
        /,
        *,
        engine: Engine = store.engine,
    ) -> int:
        """Write the dataframe to the feature store for `ticker`.

        Does the necessary handling to transform columns to
        prepare the dataframe to be written to a dynamically-defined
        local SQL table.

        Args:
            ticker: Company ticker.
            df: Dataframe to store completely as rows in a local SQL
                table.
            engine: Feature store database engine.

        Returns:
            Number of rows written to the SQL table.

        """
        df = df.reset_index(names="filed")
        df = df.melt("filed", var_name="name", value_name="value")
        df["ticker"] = ticker
        table = store.quarterly_features
        with engine.begin() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))  # type: ignore[arg-type]
        return len(df.index)


#: Public-facing API.
quarterly_features = _QuarterlyFeatures()
