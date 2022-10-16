"""BEA SQLite interface."""

import sqlite3
from abc import ABC, abstractmethod
from typing import ClassVar

import pandas as pd


class _Table(ABC):
    """Interface for BEA SQLite tables."""

    @classmethod
    @property
    @abstractmethod
    def COLUMNS(cls) -> dict[str, str]:
        """Table columns."""

    @classmethod
    @property
    @abstractmethod
    def NAME(cls) -> str:
        """Table name."""

    @classmethod
    @property
    @abstractmethod
    def PRIMARY_KEYS(cls) -> tuple[str, ...]:
        """Primary keys in `COLUMNS`."""

    @classmethod
    def _column_def(cls) -> str:
        """SQLite column definition (that goes inside parentheticals)."""
        return ", ".join([f"{k} {v}" for k, v in cls.COLUMNS.items()])

    @classmethod
    def _create_table_stmt(cls) -> str:
        """SQLite CREATE TABLE statement."""
        return (
            f"CREATE TABLE IF NOT EXISTS "
            f"{cls.NAME}({cls._column_def()}, "
            f"{cls._table_constraint()}"
            ");"
        )

    @classmethod
    def _handle_args(
        cls, data: str | pd.DataFrame, con: str | sqlite3.Connection
    ) -> tuple[pd.DataFrame, sqlite3.Connection, bool]:
        """Convert args to dataframes and connections if they aren't.

        Args:
            data: Path to CSV or a dataframe.
            con: Path to SQLite database or a connection.

        Returns:
            `data` as a dataframe
            `con` as a SQLite database connection
            and a boolean indicating whether to close `con` after usage.

        """
        if isinstance(data, str):
            with open(data, "r") as f:
                data = pd.read_csv(f)

        close_connection = False
        if isinstance(con, str):
            con = sqlite3.connect(con)
            close_connection = True
        return data, con, close_connection

    @classmethod
    def _insert_stmt(cls) -> str:
        """SQLite INSERT INTO statement."""
        return f"INSERT INTO {cls.NAME} " f"VALUES({', '.join('?' * len(cls.COLUMNS))})"

    @classmethod
    def _table_constraint(cls) -> str:
        """SQLite PRIMARY KEY definition."""
        return f"PRIMARY KEY({', '.join(cls.PRIMARY_KEYS)})"

    @classmethod
    def create(cls, data: str | pd.DataFrame, con: str | sqlite3.Connection) -> None:
        """Create a table at `con` and initialize it with `data`.

        Args:
            data: Path to CSV or a dataframe.
            con: Path to SQLite database or a connection.

        """
        data, con, close_connection = cls._handle_args(data, con)

        with con:
            cur = con.cursor()
            cur.execute(cls._create_table_stmt())
            cur.executemany(cls._insert_stmt(), data.values.tolist())

        if close_connection:
            con.close()

    @classmethod
    def insert(cls, data: str | pd.DataFrame, con: str | sqlite3.Connection) -> None:
        """Insert `data` into `con`.

        Args:
            data: Path to CSV or a dataframe.
            con: Path to SQLite database or a connection.

        """
        data, con, close_connection = cls._handle_args(data, con)

        with con:
            cur = con.cursor()
            cur.executemany(cls._insert_stmt(), data.values.tolist())

        if close_connection:
            con.close()

    @classmethod
    @abstractmethod
    def select(cls) -> None:
        ...


class _FixedAssets(_Table):
    """US fixed assets (assets for long-term use)."""

    #: Table name.
    NAME: ClassVar[str] = "fixed_assets"

    #: Mapping of column name to SQLite types.
    COLUMNS: ClassVar[dict[str, str]] = {
        "table_id": "TEXT",
        "series_code": "TEXT",
        "line": "INT",
        "line_description": "TEXT",
        "year": "INT",
        "metric": "TEXT",
        "units": "TEXT",
        "e": "INT",
        "value": "REAL",
    }

    #: Columns used for the table's primary keys.
    PRIMARY_KEYS: ClassVar[tuple[str, ...]] = ("table_id", "line", "year")


class _SQL:
    """Collection of BEA SQLite tables."""

    fixed_assets: ClassVar[type[_FixedAssets]] = _FixedAssets


#: Public-facing BEA SQLite interface.
sql = _SQL

if __name__ == "__main__":
    import shark

    df = shark.bea.api.fixed_assets.get("FAAt101")
    shark.bea.sql.fixed_assets.create(df, "data/bea.sqlite")
