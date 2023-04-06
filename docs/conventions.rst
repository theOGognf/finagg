Conventions
===========

**finagg** has a number of conventions around package organization,
data normalization, and data organization. Understanding these conventions
makes **finagg** a bit more ergonomic. This page covers those conventions.

Import conventions
------------------

**finagg** is designed to be imported once at the highest module:

>>> import finagg

Subpackages and submodules are usually accessed through their fully qualified
names from the top-level module:

>>> finagg.bea.api
>>> finagg.fred.api
>>> finagg.sec.api

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
:data:`finagg.bea.api.gdp_by_industry.name` that describes the BEA API database
that the singleton refers to. In addition, the singleton has methods
:data:`finagg.bea.api.gdp_by_industry.get_parameter_list` and
:data:`finagg.bea.api.gdp_by_industry.get_parameter_values`
for getting API parameters and API parameter value options, respectively,
while :data:`finagg.bea.api.gdp_by_industry.get` is the actual method for
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
