"""BEA SQLAlchemy interfaces."""

import os
import pathlib

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent / "data" / "bea.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "BEA_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all datasets.
metadata = MetaData()

#: BEA dataset APIs as SQL tables.
fixed_assets = Table(
    "fixed_assets",
    metadata,
    Column("table_id", String, primary_key=True),
    Column("series_code", String),
    Column("line", Integer, primary_key=True),
    Column("line_description", String),
    Column("year", Integer, primary_key=True),
    Column("metric", String),
    Column("units", String),
    Column("e", Integer),
    Column("value", Float),
)

gdp_by_industry = Table(
    "gdp_by_industry",
    metadata,
    Column("table_id", Integer, primary_key=True),
    Column("freq", String),
    Column("year", Integer, primary_key=True),
    Column("quarter", Integer, primary_key=True),
    Column("industry", String, primary_key=True),
    Column("industry_description", String),
    Column("value", Float),
)

input_output = Table(
    "input_output",
    metadata,
    Column("table_id", Integer, primary_key=True),
    Column("year", Integer, primary_key=True),
    Column("row_code", String, primary_key=True),
    Column("row_description", String),
    Column("row_type", String),
    Column("col_code", String, primary_key=True),
    Column("col_description", String),
    Column("col_type", String),
    Column("value", Float),
)

nipa = Table(
    "nipa",
    metadata,
    Column("table_id", String, primary_key=True),
    Column("series_code", String),
    Column("line", Integer, primary_key=True),
    Column("line_description", String),
    Column("year", Integer, primary_key=True),
    Column("quarter", Integer, primary_key=True),
    Column("metric", String),
    Column("units", String),
    Column("e", Integer),
    Column("value", Float),
)
