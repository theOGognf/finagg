.. finagg documentation master file, created by
   sphinx-quickstart on Mon Feb 27 20:01:04 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

finagg: Financial Aggregation for Python
========================================

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating data from those APIs into SQL databases,
and tools for transforming aggregated data into features useful for analysis
and AI/ML.

**finagg** currently supports the following free APIs:

* The Bureau of Economic Analysis `(BEA) API`_.
* The Federal Reserve Economic Data `(FRED) API`_.
* The Securities and Exchange Commission’s `(SEC) EDGAR API`_.

In addition, **finagg** provides methods for:

* downloading data from these APIs
* inserting downloaded data into "raw" SQL tables
* aggregating raw data into "refined" SQL tables
* analyzing/inspecting raw and refined data

making **finagg** ideal for offline analysis.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   Installation <installation>
   Configuration <configuration>
   API <api/modules>
   References <references>
   FAQ <faq>

:ref:`genindex`
---------------

Alphabetically-ordered index of all package members.

.. _`(BEA) API`: https://apps.bea.gov/api/signup/
.. _`(FRED) API`: https://fred.stlouisfed.org/docs/api/fred/
.. _`(SEC) EDGAR API`: https://www.sec.gov/edgar/sec-api-documentation
