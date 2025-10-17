"""BEA SQLAlchemy interfaces."""


from sqlalchemy import Column, Float, Integer, MetaData, String, Table

metadata = MetaData()
"""The metadata associated with all SQL tables defined in this module.

:meta hide-value:
"""

fixed_assets = Table(
    "fixed_assets",
    metadata,
    Column("TableName", String, primary_key=True),
    Column("SeriesCode", String),
    Column("LineNumber", Integer, primary_key=True),
    Column("LineDescription", String),
    Column("TimePeriod", Integer, primary_key=True),
    Column("METRIC_NAME", String),
    Column("CL_UNIT", String),
    Column("UNIT_MULT", Integer),
    Column("DataValue", Float),
)
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.fixed_assets` (an alias for
:class:`finagg.bea.api.FixedAssets`).

:meta hide-value:
"""

gdp_by_industry = Table(
    "gdp_by_industry",
    metadata,
    Column("TableID", Integer, primary_key=True),
    Column("Frequency", String),
    Column("Year", Integer, primary_key=True),
    Column("Quarter", Integer, primary_key=True),
    Column("Industry", String, primary_key=True),
    Column("IndustrYDescription", String),
    Column("DataValue", Float),
)
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.gdp_by_industry` (an alias for
:class:`finagg.bea.api.GDPByIndustry`).

:meta hide-value:
"""

input_output = Table(
    "input_output",
    metadata,
    Column("TableID", Integer, primary_key=True),
    Column("Year", Integer, primary_key=True),
    Column("RowCode", String, primary_key=True),
    Column("RowDescr", String),
    Column("RowType", String),
    Column("ColCode", String, primary_key=True),
    Column("ColDescr", String),
    Column("ColType", String),
    Column("DataValue", Float),
)
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.input_output` (an alias for
:class:`finagg.bea.api.InputOutput`).

:meta hide-value:
"""

nipa = Table(
    "nipa",
    metadata,
    Column("TableName", String, primary_key=True),
    Column("SeriesCode", String),
    Column("LineNumber", Integer, primary_key=True),
    Column("LineDescription", String),
    Column("Year", Integer, primary_key=True),
    Column("Quarter", Integer, primary_key=True),
    Column("METRIC_NAME", String),
    Column("CL_UNIT", String),
    Column("UNIT_MULT", Integer),
    Column("DataValue", Float),
)
"""SQL table for storing raw data as returned by
:data:`finagg.bea.api.nipa` (an alias for
:class:`finagg.bea.api.NIPA`).

:meta hide-value:
"""
