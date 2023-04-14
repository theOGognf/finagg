"""BEA SQLAlchemy interfaces."""


from sqlalchemy import Column, Float, Integer, MetaData, String, Table

metadata = MetaData()
"""The metadata associated with all SQL tables defined in this module.

:meta hide-value:
"""

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
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.fixed_assets` (an alias for
:class:`finagg.bea.api.FixedAssets`).

:meta hide-value:
"""

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
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.gdp_by_industry` (an alias for
:class:`finagg.bea.api.GDPByIndustry`).

:meta hide-value:
"""

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
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.input_output` (an alias for
:class:`finagg.bea.api.InputOutput`).

:meta hide-value:
"""

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
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.nipa` (an alias for
:class:`finagg.bea.api.NIPA`).

:meta hide-value:
"""
