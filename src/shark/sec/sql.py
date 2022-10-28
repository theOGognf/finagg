"""SEC SQLAlchemy interfaces."""

import os
import pathlib

from sqlalchemy import Column, Float, Integer, MetaData, String, Table, create_engine

_SQL_DB_PATH = (
    pathlib.Path(__file__).resolve().parent.parent.parent.parent / "data" / "sec.sqlite"
)

_SQL_DB_URL = os.environ.get(
    "SEC_SQL_DB_URL",
    f"sqlite:///{_SQL_DB_PATH}",
)

#: SQLAlchemy engine used for all operations.
engine = create_engine(_SQL_DB_URL)

#: SQLAlchemy metadata used by all datasets.
metadata = MetaData()

#: SEC dataset APIs as SQL tables.
submissions = Table(
    "submissions",
    metadata,
    Column("cik", String, primary_key=True),
    Column("entity_type", String),
    Column("sic", String),
    Column("sic_description", String),
    Column("insider_transaction_for_owner_exists", Integer),
    Column("insider_transaction_for_issuer_exists", Integer),
    Column("name", String),
    Column("ticker", String),
    Column("exchange", String),
    Column("ein", String),
    Column("description", String),
    Column("website", String),
    Column("investor_website", String),
    Column("category", String),
    Column("fiscal_year_end", String),
    Column("state_of_incorporation", String),
    Column("state_of_incorporation_description", String),
)

tags = Table(
    "tags",
    metadata,
    Column("cik", String),
    Column("accn", String, primary_key=True),
    Column("taxonomy", String),
    Column("tag", String, primary_key=True),
    Column("form", String),
    Column("units", String),
    Column("fy", Integer),
    Column("fp", String),
    Column("start", String, nullable=True),
    Column("end", String),
    Column("filed", String),
    Column("frame", String, nullable=True),
    Column("label", String),
    Column("description", String),
    Column("entity", String),
    Column("value", Float),
)
