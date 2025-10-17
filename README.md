# finagg: Financial Aggregation for Python

![PyPI Downloads](https://img.shields.io/pypi/dm/finagg)
![PyPI Version](https://img.shields.io/pypi/v/finagg)
![Python Versions](https://img.shields.io/pypi/pyversions/finagg)

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating historical data from those APIs into SQL
databases, and tools for transforming aggregated data into features useful for
analysis and AI/ML.

* **Documentation**: https://theogognf.github.io/finagg/
* **PyPI**: https://pypi.org/project/finagg/
* **Repository**: https://github.com/theOGognf/finagg

# Quick Start

## Installation

Install with pip for the latest stable version.

```console
pip install finagg
```

Install from GitHub for the latest unstable version.

```console
git clone https://github.com/theOGognf/finagg.git
pip install ./finagg/
```

Optionally install the recommended datasets (economic data, company
financials, stock histories, etc.) from 3rd party APIs into a local SQL
database.

```console
finagg install -ss economic -ts sec -z -r
```

The installation will point you where to get free API keys for each API that
requires one and will write those API keys to a local ``.env`` file for storage.
Run ``finagg install --help`` for more installation options and details.

## Basic Usage

These are just **finagg** usage samples. See the [documentation][4] for all the
supported APIs and features.

### Explore the APIs directly

*These methods require internet access and API keys/user agent declarations.*

Get Bureau of Economic Analysis (BEA) data.

```pycon
>>> finagg.bea.api.gdp_by_industry.get(year=[2019]).head(5)
   table_id freq  year quarter industry                         industry_description ...
0         1    Q  2019       1       11  Agriculture, forestry, fishing, and hunting ...
1         1    Q  2019       1    111CA                                        Farms ...
2         1    Q  2019       1    113FF    Forestry, fishing, and related activities ...
3         1    Q  2019       1       21                                       Mining ...
4         1    Q  2019       1      211                       Oil and gas extraction ...
```

Get Federal Reserve Economic Data (FRED).

```pycon
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
```

Get Securities and Exchange Commission (SEC) filings.

```pycon
>>> finagg.sec.api.company_facts.get(ticker="AAPL").head(5)
          end        value                  accn    fy  fp    form       filed ...
0  2009-06-27  895816758.0  0001193125-09-153165  2009  Q3    10-Q  2009-07-22 ...
1  2009-10-16  900678473.0  0001193125-09-214859  2009  FY    10-K  2009-10-27 ...
2  2009-10-16  900678473.0  0001193125-10-012091  2009  FY  10-K/A  2010-01-25 ...
3  2010-01-15  906794589.0  0001193125-10-012085  2010  Q1    10-Q  2010-01-25 ...
4  2010-04-09  909938383.0  0001193125-10-088957  2010  Q2    10-Q  2010-04-21 ...
```

### Use installed raw data for exploring the most popular features

*These methods require internet access, API keys/user agent declarations, and
downloading and installing raw data through the* ``finagg install`` *or*
``finagg <api/subpackage> install`` *commands.*

Get the most popular FRED features all in one dataframe.

```pycon
>>> finagg.fred.feat.economic.from_raw().head(5)
            CIVPART  LOG_CHANGE(CPIAUCNS)  LOG_CHANGE(CSUSHPINSA)  FEDFUNDS ...
date                                                                        ...
2014-10-06     62.8                   0.0                     0.0      0.09 ...
2014-10-08     62.8                   0.0                     0.0      0.09 ...
2014-10-13     62.8                   0.0                     0.0      0.09 ...
2014-10-15     62.8                   0.0                     0.0      0.09 ...
2014-10-20     62.8                   0.0                     0.0      0.09 ...
```

Get quarterly report features from SEC data.

```pycon
>>> finagg.sec.feat.quarterly.from_raw("AAPL").head(5)
                    LOG_CHANGE(Assets)  LOG_CHANGE(AssetsCurrent) ...
fy   fp filed                                                     ...
2010 Q1 2010-01-25            0.182629                  -0.023676 ...
     Q2 2010-04-21            0.000000                   0.000000 ...
     Q3 2010-07-21            0.000000                   0.000000 ...
2011 Q1 2011-01-19            0.459174                   0.278241 ...
     Q2 2011-04-21            0.000000                   0.000000 ...
```

### Use installed features for exploring refined aggregations of raw data

*These methods require installing refined data through the* ``finagg install``
*or* ``finagg <api/subpackage> install`` *commands.*

Get a ticker's industry's averaged quarterly report features.

```pycon
>>> finagg.sec.feat.quarterly.industry.from_refined(ticker="AAPL").head(5)
                                 mean                           ...
name               AssetCoverageRatio BookRatio DebtEquityRatio ...
fy   fp filed                                                   ...
2014 Q1 2014-05-15          10.731301  9.448954        0.158318 ...
     Q2 2014-08-14          10.731301  9.448954        0.158318 ...
     Q3 2014-11-14          10.731301  9.448954        0.158318 ...
2015 Q1 2015-05-15          16.738972  9.269250        0.294238 ...
     Q2 2015-08-13          16.738972  9.269250        0.294238 ...
```

Get a ticker's industry-averaged quarterly report features.

```pycon
>>> finagg.sec.feat.quarterly.normalized.from_refined("AAPL").head(5)
                    NORM(LOG_CHANGE(Assets))  NORM(LOG_CHANGE(AssetsCurrent)) ...
fy   fp filed                                                                 ...
2010 Q2 2010-04-21                  0.000000                         0.000000 ...
     Q3 2010-07-21                  0.000000                         0.000000 ...
2011 Q1 2011-01-19                  0.978816                         0.074032 ...
     Q2 2011-04-21                  0.000000                         0.000000 ...
     Q3 2011-07-20                 -0.353553                        -0.353553 ...
```

Get tickers sorted by an industry-averaged quarterly report feature.

```pycon
>>> finagg.sec.feat.quarterly.normalized.get_tickers_sorted_by(
...   "NORM(EarningsPerShareBasic)",
...   year=2019
... )[:5]
['XRAY', 'TSLA', 'SYY', 'WHR', 'KMB']
```

# Configuration

## API Keys and User Agents

API keys and user agent declarations are required for most of the APIs.
You can set environment variables to expose your API keys and user agents
to **finagg**, or you can pass your API keys and user agents to the implemented
APIs programmatically. The following environment variables are used for
configuring API keys and user agents:

* ``BEA_API_KEY`` is for the Bureau of Economic Analysis's API key. You can get
  a free API key from the [BEA API site][3].
* ``FRED_API_KEY`` is for the Federal Reserve Economic Data API key. You can get
  a free API key from the [FRED API site][8].
* ``SEC_API_USER_AGENT`` is for the Securities and Exchange Commission's API. This
  should be of the format ``FIRST_NAME LAST_NAME E_MAIL``.

## Data Locations

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

## Other

You can change some **finagg** behavior with other environment variables:

* ``FINAGG_DISABLE_HTTP_CACHE``: Set this to ``"1"`` or ``"True"`` to disable the
  HTTP requests cache. Instead of a cachable session, a default, uncached user
  session will be used for all requests.

# Dependencies

* [pandas][11] for fast, flexible, and expressive representations of relational data.
* [requests][12] for HTTP requests to 3rd party APIs.
* [requests-cache][13] for caching HTTP requests to avoid getting throttled by 3rd
  party API servers.
* [SQLAlchemy][17] for a SQL Python interface.

# API References

* The [BEA API][1] and the [BEA API key registration link][2].
* The [FRED API][6] and the [FRED API key registration link][7].
* The [SEC API][14].

# Related Projects

* [FinRL][5] is a collection of financial reinforcement learning environments
  and tools.
* [fredapi][9] is an implementation of the FRED API.
* [OpenBBTerminal][10] is an open-source version of the Bloomberg Terminal.
* [sec-edgar][15] is an implementation of a file-based SEC EDGAR parser.
* [sec-edgar-api][16] is an implementation of the SEC EDGAR REST API.

# Frequently Asked Questions

## Where should I start?

Aggregate some data, create some analysis notebooks, or create some RL
environments using the implemented data features and SQL tables. This
project was originally created to make RL environments for financial
applications but has since focused its purpose to just aggregating financial
data and features. That being said, all the implemented features are
defined in such a way to make it very easy to develop financial AI/ML,
so we encourage you to do just that!

## Why aren't features being installed for a specific ticker or economic data series?

Implemented APIs may be relatively new and simply may not provide data for a
particular ticker or economic data series. For example, earnings per share may
not be accessible for all companies through the SEC EDGAR API. In some cases,
APIs may raise an HTTP error, causing installations to skip the ticker or
series. Additionally, not all tickers and economic data series contain
sufficient data for feature normalization. If a ticker or series only has one
data point, that data point could be dropped when computing a feature (such as
percent change), causing no data to be installed.

## What Python versions are supported?

Python 3.10 and up are supported. We don't plan on supporting lower versions
because 3.10 introduces some nice quality of life updates that are used
throughout the package.

## What operating systems are supported?

The package is developed and tested on both Linux and Windows, but we recommend
using Linux or WSL in practice. The package performs a good amount of I/O and
interprocess operations that could result in a noticeable performance
degradation on Windows.

[1]: https://apps.bea.gov/api/signup/
[2]: https://apps.bea.gov/API/signup/
[3]: https://apps.bea.gov/API/signup/
[4]: https://theogognf.github.io/finagg/
[5]: https://github.com/AI4Finance-Foundation/FinRL
[6]: https://fred.stlouisfed.org/docs/api/fred/
[7]: https://fredaccount.stlouisfed.org/login/secure/
[8]: https://fredaccount.stlouisfed.org/login/secure/
[9]: https://github.com/mortada/fredapi
[10]: https://github.com/OpenBB-finance/OpenBBTerminal
[11]: https://pandas.pydata.org/
[12]: https://requests.readthedocs.io/en/latest/
[13]: https://requests-cache.readthedocs.io/en/stable/
[14]: https://www.sec.gov/edgar/sec-api-documentation
[15]: https://github.com/sec-edgar/sec-edgar
[16]: https://github.com/jadchaar/sec-edgar-api
[17]: https://www.sqlalchemy.org/
