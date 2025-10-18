finagg: Financial Aggregation for Python
========================================

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating historical data from those APIs into SQL
databases, and tools for transforming aggregated data into features useful for
analysis.

**finagg** currently supports the following free APIs:

* The Bureau of Economic Analysis `(BEA) API`_. The BEA API provides methods
  for retrieving a subset of economic statistical data as published by the BEA
  along with metadata that describes that economic statistical data.
* The Federal Reserve Economic Data `(FRED) API`_. The FRED API provides
  methods for retrieving, searching, and describing economic data from a variety
  of sources. The FRED API is one of the most popular APIs in the finance
  industry.
* The Securities and Exchange Commission’s `(SEC) EDGAR API`_. The SEC EDGAR
  API provides methods for retrieving XBRL data (e.g., earnings per share) from
  financial statements and methods for retrieving SEC filing submission
  histories (e.g., 10-Q/10-K forms). The SEC EDGAR API is one of the few APIs
  that provides historical and current company financial data for free.

Methods for aggregating data from these APIs are organized according to their
API/subpackage and usage (i.e., ``finagg.<api/subpackage>.<usage>``). For
example, SEC EDGAR API methods are accesible under the subpackage
:mod:`finagg.sec.api` (e.g., the SEC company facts API is accessible as
:data:`finagg.sec.api.company_facts`) while features aggregated from the SEC
EDGAR API are accessible under the fully qualified name :mod:`finagg.sec.feat`
(e.g., quarterly SEC features are accessible as
:data:`finagg.sec.feat.quarterly`).

.. toctree::
   :maxdepth: 2
   :caption: Contents

   Conventions <conventions>
   Walkthroughs <walkthroughs>
   API <api/modules>
   CLI <cli>
   Release Notes <release_notes>

:ref:`genindex`
---------------

Alphabetically-ordered index of all package members.

.. _`(BEA) API`: https://apps.bea.gov/api/signup/
.. _`(FRED) API`: https://fred.stlouisfed.org/docs/api/fred/
.. _`(SEC) EDGAR API`: https://www.sec.gov/edgar/sec-api-documentation
