# finagg: Financial Aggregation for Python

**finagg** is a Python package that provides implementations of popular and free
financial APIs, tools for aggregating data from those APIs into SQL databases,
and tools for transforming aggregated data into features useful for analysis
and AI/ML.

## Quick Start

### Installation

Install **finagg** from GitHub directly.

```python
git clone https://github.com/theOGognf/finagg.git
pip install ./finagg/
```

Optionally install the recommended datasets from 3rd party APIs into a local
SQL database.

```python
finagg install
```

The installation will point you where to get free API keys and write them to a
local `.env` file for storage.

### Basic Usage

Explore the APIs directly.

```python
import finagg

# Get Bureau of Economic Analysis (BEA) data.
gdp = finagg.bea.api.gdp_by_industry.get(year=[2019, 2020, 2021])

# Get Federal Reserve Economic Data (FRED).
inflation_rate = finagg.fred.api.series.get("CPIAUCNS")

# Get Securities and Exchange Commission (SEC) filings.
facts = finagg.sec.api.company_facts.get(ticker="AAPL")
```

Use installed raw data for exploring the most popular features.

```python
# Get the most popular FRED features all in one dataframe.
economic_data = finagg.fred.feat.economic.from_raw()

# Get quarterly report features from SEC data.
quarterly_data = finagg.sec.feat.quarterly.from_raw("AAPL")

# Get an aggregation of quarterly and daily features for a particular ticker.
fundamental_data = finagg.fundam.feat.fundam.from_raw("AAPL")
```

Use installed features for exploring refined feature aggregations.

```python
# Get a ticker's industry's averaged quarterly report features.
industry_quartery = finagg.sec.feat.quarterly.industry.from_refined(ticker="AAPL")

# Get a ticker's industry-averaged quarterly report features.
relative_quarterly = finagg.sec.feat.quarterly.relative.from_refined("AAPL")

# Get tickers sorted by an industry-averaged quarterly report feature.
lowest_earners = finagg.sec.feat.quarterly.relative.get_ids_sorted_by("EarningsPerShare")
```

## Configuration

### Data Locations

**finagg**'s root path, HTTP cache path, and database path are all configurable
through environment variables. By default, all data related to **finagg** is put
in a `./findata` directory relative to a root directory. You can change these
locations by modifying the respective environment variables:

- `FINAGG_ROOT_PATH` points to the parent directory of the `./findata` directory.
Defaults to your current working directory.
- `FINAGG_HTTP_CACHE_PATH` points to the HTTP requests cache SQLite storage.
Defaults to `./findata/http_cache.sqlite`
- `FINAGG_DATABASE_URL` points to the **finagg** data storage. Defaults to
`./findata/finagg.sqlite`.

### API Keys and User Agents

API keys and user agent declarations are required for most of the APIs.
You can set environment variables to expose your API keys and user agents
to **finagg**, or you can pass your API keys and user agents to the implemented
APIs programmatically. The following environment variables are used for
configuring API keys and user agents:

- `BEA_API_KEY` is for the Bureau of Economic Analysis's API key. You can get
  a free API key from the BEA site (linked [below](#api-references)).
- `FRED_API_KEY` is for the Federal Reserve Economic Data API key. You can get
  a free API key from the FRED site (linked [below](#api-references)).
- `INDICES_API_USER_AGENT` is for scraping popular indices' compositions from
  Wikipedia and should be equivalent to a browser's user agent declaration.
  This defaults to a hardcoded value, but it may not always work.
- `SEC_API_USER_AGENT` is for the Securities and Exchange Commission's API. This
  should be of the format `FIRST_NAME LAST_NAME E_MAIL`.

## Dependencies

- [**pandas** for fast, flexible, and expressive representations of relational data.](https://pandas.pydata.org/)
- [**requests** for HTTP requests to 3rd party APIs.](https://requests.readthedocs.io/en/latest/)
- [**requests-cache** for caching HTTP requests to avoid getting throttled by 3rd party API servers.](https://requests-cache.readthedocs.io/en/stable/)
- [**SQLAlchemy** for a SQL Python interface.](https://www.sqlalchemy.org/)
- [**yfinance** for historical stock data from Yahoo! Finance.](https://github.com/ranaroussi/yfinance)

## API References

- [The BEA API](https://apps.bea.gov/api/signup/) and [its respective API key registration link](https://apps.bea.gov/API/signup/).
- [The FRED API](https://fred.stlouisfed.org/docs/api/fred/) and [its respective API key registration link.](https://fredaccount.stlouisfed.org/login/secure/)
- [The SEC API.](https://www.sec.gov/edgar/sec-api-documentation)

## Related Projects

- [**FinRL** is a collection of financial reinforcement learning environments and tools.](https://github.com/AI4Finance-Foundation/FinRL)
- [**fredapi** is an implementation of the FRED API.](https://github.com/mortada/fredapi)
- [**OpenBBTerminal** an open-source version of the Bloomberg Terminal.](https://github.com/OpenBB-finance/OpenBBTerminal)
- [**sec-edgar** is an implementation of a file-based SEC EDGAR parser.](https://github.com/sec-edgar/sec-edgar)

## Frequently Asked Questions

### Where should I start?

Aggregate some data, create some analysis notebooks, or create some RL
environments using the implemented data features and SQL tables. This
project was originally created to make RL environments for financial
applications but has since focused its purpose to just aggregating financial
data and features. That being said, all the implemented features are
defined in such a way to make it very easy to develop financial AI/ML,
so we encourage you to do just that!

### What Python versions are supported?

Python 3.10 and up are supported. We don't plan on supporting lower versions
because 3.10 introduces some nice quality of life updates that are used
throughout the package.

### What operating systems are supported?

The package is developed and tested on both Linux and Windows, but we recommend
using Linux or WSL in practice. The package performs a good amount of I/O and
interprocess operations that could result in a noticeable performance
degradation on Windows.
