Conventions
===========

**finagg** has a number of conventions around package organization,
data organization, and data normalization. Understanding these conventions
makes **finagg** a bit more ergonomic. This page covers those conventions.

Import conventions
------------------

Following **finagg**'s import conventions guarantees updates to **finagg**
won't break your code. Although definitions may shift around during or
**finagg**'s organization may change slightly, your code won't be affected
so long as you follow the import conventions. On top of this benefit,
**finagg**'s import conventions just simplify **finagg**'s usage.

**finagg** is designed to be imported once at the highest module:

>>> import finagg  # doctest: +SKIP

Subpackages and submodules are usually accessed through their fully qualified
names from the top-level module:

>>> finagg.bea.api  # doctest: +SKIP
>>> finagg.fred.api  # doctest: +SKIP
>>> finagg.sec.api  # doctest: +SKIP

It's also common for subpackages to be imported using their name as an alias:

>>> import finagg.bea as bea
>>> import finagg.fred as fred
>>> import finagg.sec as sec

Package organization
--------------------

**finagg** is organized according to API implementations, SQL table
definitions, and feature definitions. As such, each subpackage has up to
three submodules within it:

* an ``api`` module that implements the subpackage's API (if one exists)
* a ``sql`` module that defines SQL tables for organizing aggregated data
  along with utility functions for common SQL queries (if there's enough
  data that deems SQL tables necessary)
* a ``feat`` module that defines features aggregated from the ``api`` and
  ``sql`` submodules (if the data would benefit from special queries or
  normalization)

As an example, the :mod:`finagg.sec` contains all three submodules because
of the complexity of its API, its data, and its features:

* :mod:`finagg.sec.api` for implementing the SEC EDGAR API
* :mod:`finagg.sec.sql` for defining SQL tables around raw SEC EDGAR API data,
  refined SEC EDGAR API data, and helper functions that replicate some SEC
  EDGAR API methods using data from the SQL tables
* :mod:`finagg.sec.feat` for defining features and helper methods for
  constructing those features

However, not all subpackages contain all three submodules. As another example,
the :mod:`finagg.indices` subpackage only contains ``api`` and ``sql``
submodules. No ``feat`` submodule is necessary for :mod:`finagg.indices`
because there is no use for further transforming or normalizing the data
available through :mod:`finagg.indices.api` or defined by
:mod:`finagg.indices.sql`.

API implementations
-------------------

APIs are implemented as singleton class instances within ``api`` submodules
Each singleton has a ``get`` method for accessing data from API endpoints.
Some API implementations include class attributes that define API metadata
(such as URLs or endpoint names), while other API implementations include
helper methods for navigating the APIs. The design of each API implementation
is based on the reference API that's being implemented.

As an example, the BEA API, implemented by :mod:`finagg.bea.api`, contains
a singleton :data:`finagg.bea.api.gdp_by_industry` with an attribute
:attr:`~finagg.bea.api.API.name` that describes the BEA API database
that the singleton refers to. In addition, the singleton has methods
:meth:`~finagg.bea.api.API.get_parameter_list` and
:meth:`~finagg.bea.api.API.get_parameter_values`
for getting API parameters and API parameter value options, respectively,
while :meth:`~finagg.bea.api.GDPByIndustry.get` is the actual method for
retrieving data from the API is implemented by.

>>> finagg.bea.api.gdp_by_industry.name
'GdpByIndustry'
>>> finagg.bea.api.gdp_by_industry.get_parameter_list()  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
  ParameterName ParameterDataType                               ParameterDescription ...
0     Frequency            string                            A - Annual, Q-Quarterly ...
1      Industry            string       List of industries to retrieve (ALL for All) ...
2       TableID           integer  The unique GDP by Industry table identifier (A... ...
3          Year           integer  List of year(s) of data to retrieve (ALL for All) ...
>>> finagg.bea.api.gdp_by_industry.get_parameter_values("TableID").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
  Key                                               Desc
0   1                    Value Added by Industry (A) (Q)
1   5  Value added by Industry as a Percentage of Gro...
2   6          Components of Value Added by Industry (A)
3   7  Components of Value Added by Industry as a Per...
4   8  Chain-Type Quantity Indexes for Value Added by...

Other implemented APIs, such as the SEC EDGAR API implemented by
:mod:`finagg.sec.api`, don't have as many helper methods and are barebone
implementations.

Almost everything is a dataframe
--------------------------------

Dataframes are just too convenient to not use as the fundamental type within
**finagg**. Almost all objects returned by APIs and features are dataframes.

Helper methods for inspecting available data
--------------------------------------------

Most submodules and singletons contain helper methods for getting sets of
IDs available through other methods. These methods are useful for verifying
if data has been installed properly or for selecting a subset of data for
further refinement. Examples of these methods include:

* :meth:`finagg.fred.feat.Series.get_id_set` returns installed economic data
  series IDs
* :meth:`finagg.sec.api.get_ticker_set` returns all the tickers that have
  at least *some* data available through the SEC EDGAR API
* :meth:`finagg.sec.feat.Quarterly.get_ticker_set` returns all the tickers
  that have quarterly features available

Data organization
-----------------

There are only a handful of conventions regarding data organization:

* Data returned by API implementations that're used by features typically have
  their own SQL table definitions. This is convenient for querying API data
  offline and for customizing features without having to repeatedly get data
  from APIs.
* Feature SQL tables are typically "melted" and do not have a SQL table column
  per feature dataframe column. This makes it so features can be changed without
  breaking the SQL table schemas.
* Classes within ``feat`` submodules and SQL tables within ``sql`` submodules are
  named similarly to indicate their relationship.
* Unaltered data from APIs are typically referred to as "raw" data while
  features are referred to as "refined" data. Refined data SQL tables typically
  have foreign key constraints on raw data SQL tables such that refined rows
  are deleted when raw rows are deleted with the same primary key.

Data normalization
------------------

Data returned by API implementations is not normalized or standardized
beyond type casting and column renaming. However, data returned by feature
implementations is normalized depending on the nature of the data. The general
rules implemented for data normalization are as follows:

* Data whose scale drifts over time or is not easily normalizable through
  other means (e.g., gross domestic product, compony stock price, etc.) is
  converted to log changes. Since the log change of the first sample
  in a series cannot be computed and is NaN, it is dropped from the series.
* Data gaps and/or NaNs are forward-filled with the previous non-NaN value.
  If the series being forward-filled is a log change series then gaps
  and/or NaNs are replaced with zeros instead (indicating that no change
  occurs).
* Inf values are replaced with NaNs and forward-filled with the same logic
  as the previous bullet.
* Dataframe indices are always based on some time unit. When an index has
  multiple levels (e.g., features returned by
  :data:`finagg.sec.feat.quarterly`), the levels are ordered from least
  granular to most granular (e.g., year -> quarter -> date). Indices
  are always sorted.

Feature method naming
---------------------

Features are aggregations or collections of raw and/or refined data that're
ready for ingestion by another process. Features can be aggregated from
APIs, local SQL tables, or combinations of both. Features generally can be
aggregated by more than one method, and a method's name determines where the
feature is aggregated from. The feature's aggregation source(s) implies
properties associated with instantiating and maintaining the feature. For
example, if a feature is aggregated directly from an API, then that implies
the feature is likely not being stored locally, saving a bit of disk space.

Feature aggregation methods are named according to where the features are
being aggregated from to clarify the implications associated with the
methods:

* A ``from_api`` method implies the feature is aggregated directly from
  API calls. It's best to reserve ``from_api`` for experimentation.
* A ``from_raw`` method implies the feature is aggregated from local raw
  SQL tables. No extra storage space is being used to store the completely
  refined features; only already-stored raw data is being used to aggregate the
  features.
* A ``from_refined`` method implies the feature is aggregated from local
  refined SQL tables. This is likely the fastest method for accessing
  a feature, but at the cost of additional disk usage. Disk usage can be
  significant and adds up quickly depending on the number of time series
  being stored.
* A ``from_other_refined`` method implies the feature is aggregated from
  local refined SQL tables outside of the feature's subpackage. This is
  likely preferrable over ``from_refined`` when it's available as it uses
  significantly less storage with little loss in speed.
