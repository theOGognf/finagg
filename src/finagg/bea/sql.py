"""BEA SQLAlchemy interfaces."""


from sqlalchemy import Column, Float, Integer, MetaData, String, Table

metadata = MetaData()

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
