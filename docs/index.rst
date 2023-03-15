.. finagg documentation master file, created by
   sphinx-quickstart on Mon Feb 27 20:01:04 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

finagg: Financial Aggregation for Python
========================================

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating historical data from those APIs into SQL
databases, and tools for transforming aggregated data into features useful for
analysis and AI/ML.

**finagg** currently supports the following free APIs:

* The Bureau of Economic Analysis `(BEA) API`_.
* The Federal Reserve Economic Data `(FRED) API`_.
* The Securities and Exchange Commission’s `(SEC) EDGAR API`_.

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

>>> finagg.bea.api.gdp_by_industry.get(year=[2019]).head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
   table_id freq  year quarter industry                         industry_description       value
0         1    Q  2019       1       11  Agriculture, forestry, fishing, and hunting  156.300003
1         1    Q  2019       1    111CA                                        Farms  117.599998
2         1    Q  2019       1    113FF    Forestry, fishing, and related activities   38.700001
3         1    Q  2019       1       21                                       Mining  305.700012
4         1    Q  2019       1      211                       Oil and gas extraction  190.199997

Getting data from the FRED API.

>>> finagg.fred.api.series.observations.get(
...   "CPIAUCNS",
...   realtime_start=0,
...   realtime_end=-1,
...   output_type=4
... ).head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
  realtime_start realtime_end        date  value series_id
0     1949-04-22   1953-02-26  1949-03-01  169.5  CPIAUCNS
1     1949-05-23   1953-02-26  1949-04-01  169.7  CPIAUCNS
2     1949-06-24   1953-02-26  1949-05-01  169.2  CPIAUCNS
3     1949-07-22   1953-02-26  1949-06-01  169.6  CPIAUCNS
4     1949-08-26   1953-02-26  1949-07-01  168.5  CPIAUCNS

Getting data from the SEC EDGAR API.

>>> finagg.sec.api.company_facts.get(ticker="AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
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
the ``finagg <api/subpackage> install`` *commands.*

Getting FRED features.

>>> finagg.fred.feat.economic.from_raw().head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
            CIVPART_pct_change  CPIAUCNS_pct_change  CSUSHPINSA_pct_change  FEDFUNDS ...
date                                                                                 ...
2014-10-06                 0.0                  0.0                    0.0      0.09 ...
2014-10-08                 0.0                  0.0                    0.0      0.09 ...
2014-10-13                 0.0                  0.0                    0.0      0.09 ...
2014-10-15                 0.0                  0.0                    0.0      0.09 ...
2014-10-20                 0.0                  0.0                    0.0      0.09 ...

Getting SEC EDGAR features.

>>> finagg.sec.feat.quarterly.from_raw("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                    AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
fy   fp filed                                                                   ...
2010 Q1 2010-01-25                 -0.023398         0.363654              2.54 ...
     Q2 2010-04-21                  0.000000         0.363654              4.35 ...
     Q3 2010-07-21                  0.000000         0.363654              6.40 ...
2011 Q1 2011-01-19                  0.320805         0.433596              3.74 ...
     Q2 2011-04-21                  0.000000         0.433596              7.12 ...

Use installed features for exploring refined aggregations of raw data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*These methods require installing refined data through the* ``finagg install``
*or* ``finagg <api/subpackage> install`` *commands.*

Getting industry-wide SEC EDGAR features.

>>> finagg.sec.feat.quarterly.industry.from_refined(ticker="AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                        avg                                  ...
name               AssetsCurrent_pct_change DebtEquityRatio EarningsPerShare ...
fy   fp filed                                                                ...
2009 Q3 2009-10-30                 0.000000        0.573255         3.065000 ...
2010 Q1 2010-04-29                -0.012229        0.402497         0.865000 ...
     Q2 2010-07-30                 0.000000        0.500347         0.538571 ...
     Q3 2010-11-04                 0.001145        0.456791         1.203750 ...
2011 Q1 2011-05-05                 0.271624        0.465244         0.992000 ...

Getting industry-normalized SEC EDGAR features.

>>> finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                    AssetsCurrent_pct_change  DebtEquityRatio  EarningsPerShare ...
fy   fp filed                                                                   ...
2010 Q1 2010-01-25                 -0.257265        -0.260642          1.697972 ...
     Q2 2010-04-21                  0.000000        -0.530932          1.508060 ...
     Q3 2010-07-21                 -0.377964        -0.348547          1.932276 ...
2011 Q1 2011-01-19                  0.269259        -0.110688          2.880060 ...
     Q2 2011-04-21                  0.000000        -0.065501          2.899716 ...

Getting tickers sorted according to industry-normalized SEC EDGAR features.

>>> finagg.sec.feat.quarterly.normalized.get_tickers_sorted_by("EarningsPerShare", year=2019)[:5]
['XRAY', 'TSLA', 'SYY', 'WHR', 'KMB']
>>> finagg.fundam.feat.fundam.normalized.get_tickers_sorted_by(
...   "PriceEarningsRatio",
...   date="2019-01-04"
... )[:5]
['AMD', 'TRGP', 'HPE', 'CZR', 'TSLA']

.. toctree::
   :maxdepth: 2
   :caption: Contents

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
