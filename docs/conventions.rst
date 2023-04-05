Conventions
===========

**finagg** has a number of conventions around package organization,
data normalization, and data organization. Understanding these conventions
makes **finagg** significantly easier to use and more ergonomic. This page
covers those conventions.

Package organization
--------------------

**finagg** is organized according to API implementations, SQL table
definitions, and features defined using APIs and SQL tables.
Each subpackage has up to three submodules within it:

* an ``api`` module that implements the subpackage's API (if one exists)
* a ``sql`` module that defines SQL tables for organizing aggregated data
  and defining utility functions for common SQL queries
* and a ``feat`` modules that defines feature classes that are aggregations
