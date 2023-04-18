finagg: Financial Aggregation for Python
========================================

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating historical data from those APIs into SQL
databases, and tools for transforming aggregated data into features useful for
analysis and AI/ML.

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

Basic Usage
-----------

These are just **finagg** usage samples. See the :doc:`API docs <api/modules>`
for all the supported APIs and features.

Explore the APIs directly
^^^^^^^^^^^^^^^^^^^^^^^^^

*These methods require internet access and API keys/user agent declarations.*

Getting data from the BEA API.

>>> finagg.bea.api.gdp_by_industry.get(year=[2019]).head(5)  # doctest: +SKIP
   table_id freq  year quarter industry                         industry_description ...
0         1    Q  2019       1       11  Agriculture, forestry, fishing, and hunting ...
1         1    Q  2019       1    111CA                                        Farms ...
2         1    Q  2019       1    113FF    Forestry, fishing, and related activities ...
3         1    Q  2019       1       21                                       Mining ...
4         1    Q  2019       1      211                       Oil and gas extraction ...

Getting data from the FRED API.

>>> finagg.fred.api.series.observations.get(
...   "CPIAUCNS",
...   realtime_start=0,
...   realtime_end=-1,
...   output_type=4
... ).head(5)  # doctest: +SKIP
  realtime_start realtime_end        date  value series_id
0     1949-04-22   1953-02-26  1949-03-01  169.5  CPIAUCNS
1     1949-05-23   1953-02-26  1949-04-01  169.7  CPIAUCNS
2     1949-06-24   1953-02-26  1949-05-01  169.2  CPIAUCNS
3     1949-07-22   1953-02-26  1949-06-01  169.6  CPIAUCNS
4     1949-08-26   1953-02-26  1949-07-01  168.5  CPIAUCNS

Getting data from the SEC EDGAR API.

>>> finagg.sec.api.company_facts.get(ticker="AAPL").head(5)  # doctest: +SKIP
          end        value                  accn    fy  fp    form       filed ...
0  2009-06-27  895816758.0  0001193125-09-153165  2009  Q3    10-Q  2009-07-22 ...
1  2009-10-16  900678473.0  0001193125-09-214859  2009  FY    10-K  2009-10-27 ...
2  2009-10-16  900678473.0  0001193125-10-012091  2009  FY  10-K/A  2010-01-25 ...
3  2010-01-15  906794589.0  0001193125-10-012085  2010  Q1    10-Q  2010-01-25 ...
4  2010-04-09  909938383.0  0001193125-10-088957  2010  Q2    10-Q  2010-04-21 ...

Use installed raw data for exploring the most popular features
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*These methods require internet access, API keys/user agent declarations, and
downloading and installing raw data through the* ``finagg install`` *or*
``finagg <api/subpackage> install`` *commands.*

Getting FRED features.

>>> finagg.fred.feat.economic.from_raw().head(5)  # doctest: +SKIP
            CIVPART  LOG_CHANGE(CPIAUCNS)  LOG_CHANGE(CSUSHPINSA)  FEDFUNDS ...
date                                                                        ...
2014-10-06     62.8                   0.0                     0.0      0.09 ...
2014-10-08     62.8                   0.0                     0.0      0.09 ...
2014-10-13     62.8                   0.0                     0.0      0.09 ...
2014-10-15     62.8                   0.0                     0.0      0.09 ...
2014-10-20     62.8                   0.0                     0.0      0.09 ...

Getting SEC EDGAR features.

>>> finagg.sec.feat.quarterly.from_raw("AAPL").head(5)  # doctest: +SKIP
                    LOG_CHANGE(Assets)  LOG_CHANGE(AssetsCurrent) ...
fy   fp filed                                                     ...
2010 Q1 2010-01-25            0.182629                  -0.023676 ...
     Q2 2010-04-21            0.000000                   0.000000 ...
     Q3 2010-07-21            0.000000                   0.000000 ...
2011 Q1 2011-01-19            0.459174                   0.278241 ...
     Q2 2011-04-21            0.000000                   0.000000 ...

Getting fundamental financial features.

>>> finagg.fundam.feat.fundam.from_raw("AAPL").head(5)  # doctest: +SKIP
            PriceBookRatio  PriceEarningsRatio
date
2010-01-25        0.175061            2.423509
2010-01-26        0.178035            2.464678
2010-01-27        0.178813            2.475448
2010-01-28        0.177154            2.452471
2010-01-29        0.173825            2.406396

Use installed features for exploring refined aggregations of raw data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*These methods require installing refined data through the* ``finagg install``
*or* ``finagg <api/subpackage> install`` *commands.*

Getting industry-wide SEC EDGAR features.

>>> finagg.sec.feat.quarterly.industry.from_refined(ticker="AAPL").head(5)  # doctest: +SKIP
                                 mean                           ...            std ...
name               AssetCoverageRatio BookRatio DebtEquityRatio ... ReturnOnAssets ...
fy   fp filed                                                   ...                ...
2014 Q1 2014-05-15          10.731301  9.448954        0.158318 ...       0.002048 ...
     Q2 2014-08-14          10.731301  9.448954        0.158318 ...       0.004264 ...
     Q3 2014-11-14          10.731301  9.448954        0.158318 ...       0.027235 ...
2015 Q1 2015-05-15          16.738972  9.269250        0.294238 ...       0.006839 ...
     Q2 2015-08-13          16.738972  9.269250        0.294238 ...       0.015112 ...

Getting industry-normalized SEC EDGAR features.

>>> finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)  # doctest: +SKIP
                    NORM(LOG_CHANGE(Assets))  NORM(LOG_CHANGE(AssetsCurrent)) ...
fy   fp filed                                                                 ...
2010 Q2 2010-04-21                  0.000000                         0.000000 ...
     Q3 2010-07-21                  0.000000                         0.000000 ...
2011 Q1 2011-01-19                  0.978816                         0.074032 ...
     Q2 2011-04-21                  0.000000                         0.000000 ...
     Q3 2011-07-20                 -0.353553                        -0.353553 ...

Getting tickers sorted according to industry-normalized SEC EDGAR features.

>>> finagg.sec.feat.quarterly.normalized.get_tickers_sorted_by(
...   "EarningsPerShareBasic",
...   year=2019
... )[:5]  # doctest: +SKIP
['XRAY', 'TSLA', 'SYY', 'WHR', 'KMB']
>>> finagg.fundam.feat.fundam.normalized.get_tickers_sorted_by(
...   "PriceEarningsRatio",
...   date="2019-01-04"
... )[:5]  # doctest: +SKIP
['AMD', 'TRGP', 'HPE', 'CZR', 'TSLA']

.. toctree::
   :maxdepth: 2
   :caption: Contents

   Conventions <conventions>
   Walkthroughs <walkthroughs>
   Installation <installation>
   Configuration <configuration>
   CLI <cli>
   API <api/modules>
   References <references>
   FAQ <faq>

:ref:`genindex`
---------------

Alphabetically-ordered index of all package members.

.. _`(BEA) API`: https://apps.bea.gov/api/signup/
.. _`(FRED) API`: https://fred.stlouisfed.org/docs/api/fred/
.. _`(SEC) EDGAR API`: https://www.sec.gov/edgar/sec-api-documentation
