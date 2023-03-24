finagg: Financial Aggregation for Python
========================================

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating historical data from those APIs into SQL
databases, and tools for transforming aggregated data into features useful for
analysis and AI/ML.

Quick Start
===========

Installation
------------

Install **finagg** from GitHub directly.

.. code:: console

    git clone https://github.com/theOGognf/finagg.git
    pip install ./finagg/

Optionally install the recommended datasets from 3rd party APIs into a local
SQL database.

.. code:: console

    finagg install --help

The installation will point you where to get free API keys and write them to a
local ``.env`` file for storage.

Basic Usage
-----------

Explore the APIs directly
^^^^^^^^^^^^^^^^^^^^^^^^^

Get Bureau of Economic Analysis (BEA) data.

>>> finagg.bea.api.gdp_by_industry.get(year=[2019]).head(5)
   table_id freq  year quarter industry                         industry_description       value
0         1    Q  2019       1       11  Agriculture, forestry, fishing, and hunting  156.300003
1         1    Q  2019       1    111CA                                        Farms  117.599998
2         1    Q  2019       1    113FF    Forestry, fishing, and related activities   38.700001
3         1    Q  2019       1       21                                       Mining  305.700012
4         1    Q  2019       1      211                       Oil and gas extraction  190.199997

Get Federal Reserve Economic Data (FRED).

>>> finagg.fred.api.series.observations.get(
...   "CPIAUCNS",
...   realtime_start=0,
...   realtime_end=-1,
...   output_type=4
... ).head(5)
  realtime_start realtime_end        date  value series_id
0     1949-04-22   1953-02-26  1949-03-01  169.5  CPIAUCNS
1     1949-05-23   1953-02-26  1949-04-01  169.7  CPIAUCNS
2     1949-06-24   1953-02-26  1949-05-01  169.2  CPIAUCNS
3     1949-07-22   1953-02-26  1949-06-01  169.6  CPIAUCNS
4     1949-08-26   1953-02-26  1949-07-01  168.5  CPIAUCNS

Get Securities and Exchange Commission (SEC) filings.

>>> finagg.sec.api.company_facts.get(ticker="AAPL").head(5)
          end        value                  accn    fy  fp    form       filed ...
0  2009-06-27  895816758.0  0001193125-09-153165  2009  Q3    10-Q  2009-07-22 ...
1  2009-10-16  900678473.0  0001193125-09-214859  2009  FY    10-K  2009-10-27 ...
2  2009-10-16  900678473.0  0001193125-10-012091  2009  FY  10-K/A  2010-01-25 ...
3  2010-01-15  906794589.0  0001193125-10-012085  2010  Q1    10-Q  2010-01-25 ...
4  2010-04-09  909938383.0  0001193125-10-088957  2010  Q2    10-Q  2010-04-21 ...

Use installed raw data for exploring the most popular features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get the most popular FRED features all in one dataframe.

>>> finagg.fred.feat.economic.from_raw().head(5)
            CIVPART_pct_change  CPIAUCNS_pct_change  CSUSHPINSA_pct_change  FEDFUNDS ...
date                                                                                 ...
2014-10-06                 0.0                  0.0                    0.0      0.09 ...
2014-10-08                 0.0                  0.0                    0.0      0.09 ...
2014-10-13                 0.0                  0.0                    0.0      0.09 ...
2014-10-15                 0.0                  0.0                    0.0      0.09 ...
2014-10-20                 0.0                  0.0                    0.0      0.09 ...

Get quarterly report features from SEC data.

>>> finagg.sec.feat.quarterly.from_raw("AAPL").head(5)
                    AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
fy   fp filed                                                                   ...
2010 Q1 2010-01-25                 -0.023398         0.363654              2.54 ...
     Q2 2010-04-21                  0.000000         0.363654              4.35 ...
     Q3 2010-07-21                  0.000000         0.363654              6.40 ...
2011 Q1 2011-01-19                  0.320805         0.433596              3.74 ...
     Q2 2011-04-21                  0.000000         0.433596              7.12 ...

Get an aggregation of quarterly and daily features for a particular ticker.

>>> finagg.fundam.feat.fundam.from_raw("AAPL").head(5)

Use installed features for exploring refined aggregations of raw data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Get a ticker's industry's averaged quarterly report features.

>>> finagg.sec.feat.quarterly.industry.from_refined(ticker="AAPL").head(5)
                                        avg                                  ...
name               AssetsCurrent_pct_change DebtEquityRatio EarningsPerShare ...
fy   fp filed                                                                ...
2009 Q3 2009-10-30                 0.000000        0.573255         3.065000 ...
2010 Q1 2010-04-29                -0.012229        0.402497         0.865000 ...
     Q2 2010-07-30                 0.000000        0.500347         0.538571 ...
     Q3 2010-11-04                 0.001145        0.456791         1.203750 ...
2011 Q1 2011-05-05                 0.271624        0.465244         0.992000 ...

Get a ticker's industry-averaged quarterly report features.

>>> finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)
                    AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
fy   fp filed                                                                   ...
2010 Q1 2010-01-25                 -0.257265        -0.260642          1.697972 ...
     Q2 2010-04-21                  0.000000        -0.530932          1.508060 ...
     Q3 2010-07-21                 -0.377964        -0.348547          1.932276 ...
2011 Q1 2011-01-19                  0.269259        -0.110688          2.880060 ...
     Q2 2011-04-21                  0.000000        -0.065501          2.899716 ...

Get tickers sorted by an industry-averaged quarterly report feature.

>>> finagg.sec.feat.quarterly.normalized.get_tickers_sorted_by("EarningsPerShare", year=2019)[:5]
['XRAY', 'TSLA', 'SYY', 'WHR', 'KMB']

Get tickers sorted by an industry-averaged fundamental feature.

>>> finagg.fundam.feat.fundam.normalized.get_tickers_sorted_by(
...   "PriceEarningsRatio",
...   date="2019-01-04"
... )[:5]
['AMD', 'TRGP', 'HPE', 'CZR', 'TSLA']

Configuration
=============

API Keys and User Agents
------------------------

API keys and user agent declarations are required for most of the APIs.
You can set environment variables to expose your API keys and user agents
to **finagg**, or you can pass your API keys and user agents to the implemented
APIs programmatically. The following environment variables are used for
configuring API keys and user agents:

* ``BEA_API_KEY`` is for the Bureau of Economic Analysis's API key. You can get
  a free API key from the `BEA API site`_.
* ``FRED_API_KEY`` is for the Federal Reserve Economic Data API key. You can get
  a free API key from the `FRED API site`_.
* ``INDICES_API_USER_AGENT`` is for scraping popular indices' compositions from
  Wikipedia and should be equivalent to a browser's user agent declaration.
  This defaults to a hardcoded value, but it may not always work.
* ``SEC_API_USER_AGENT`` is for the Securities and Exchange Commission's API. This
  should be of the format ``FIRST_NAME LAST_NAME E_MAIL``.

Data Locations
--------------

**finagg**'s root path, HTTP cache path, and database path are all configurable
through environment variables. By default, all data related to **finagg** is put
in a ``./findata`` directory relative to a root directory. You can change these
locations by modifying the respective environment variables:

* ``FINAGG_ROOT_PATH`` points to the parent directory of the ``./findata`` directory.
  Defaults to your current working directory.
* ``FINAGG_HTTP_CACHE_PATH`` points to the HTTP requests cache SQLite storage.
  Defaults to ``./findata/http_cache.sqlite``.
* ``FINAGG_DATABASE_URL`` points to the **finagg** data storage. Defaults to
  ``./findata/finagg.sqlite``.

Dependencies
============

* `pandas`_ for fast, flexible, and expressive representations of relational data.
* `requests`_ for HTTP requests to 3rd party APIs.
* `requests-cache`_ for caching HTTP requests to avoid getting throttled by 3rd party API servers.
* `SQLAlchemy`_ for a SQL Python interface.
* `yfinance`_ for historical stock data from Yahoo! Finance.

API References
==============

* The `BEA API`_ and the `BEA API key registration link`_.
* The `FRED API`_ and the `FRED API key registration link`_.
* The `SEC API`_.

Related Projects
================

* `FinRL`_ is a collection of financial reinforcement learning environments and tools.
* `fredapi`_ is an implementation of the FRED API.
* `OpenBBTerminal`_ an open-source version of the Bloomberg Terminal.
* `sec-edgar`_ is an implementation of a file-based SEC EDGAR parser.

Frequently Asked Questions
==========================

Where should I start?
---------------------

Aggregate some data, create some analysis notebooks, or create some RL
environments using the implemented data features and SQL tables. This
project was originally created to make RL environments for financial
applications but has since focused its purpose to just aggregating financial
data and features. That being said, all the implemented features are
defined in such a way to make it very easy to develop financial AI/ML,
so we encourage you to do just that!

What Python versions are supported?
-----------------------------------

Python 3.10 and up are supported. We don't plan on supporting lower versions
because 3.10 introduces some nice quality of life updates that are used
throughout the package.

What operating systems are supported?
-------------------------------------

The package is developed and tested on both Linux and Windows, but we recommend
using Linux or WSL in practice. The package performs a good amount of I/O and
interprocess operations that could result in a noticeable performance
degradation on Windows.

.. _`BEA API`: https://apps.bea.gov/api/signup/
.. _`BEA API key registration link`: https://apps.bea.gov/API/signup/
.. _`BEA API site`: https://apps.bea.gov/API/signup/
.. _`FinRL`: https://github.com/AI4Finance-Foundation/FinRL
.. _`FRED API`: https://fred.stlouisfed.org/docs/api/fred/
.. _`FRED API key registration link`: https://fredaccount.stlouisfed.org/login/secure/
.. _`FRED API site`: https://fredaccount.stlouisfed.org/login/secure/
.. _`fredapi`: https://github.com/mortada/fredapi
.. _`OpenBBTerminal`: https://github.com/OpenBB-finance/OpenBBTerminal
.. _`pandas`: https://pandas.pydata.org/
.. _`requests`: https://requests.readthedocs.io/en/latest/
.. _`requests-cache`: https://requests-cache.readthedocs.io/en/stable/
.. _`SEC API`: https://www.sec.gov/edgar/sec-api-documentation
.. _`sec-edgar`: https://github.com/sec-edgar/sec-edgar
.. _`SQLAlchemy`: https://www.sqlalchemy.org/
.. _`yfinance`: https://github.com/ranaroussi/yfinance
