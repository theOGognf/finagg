# finagg: Financial Aggregation for Python

**finagg** is a Python package that provides implementations of popular financial APIs,
tools for aggregating data from those APIs in SQL databases, and tools for transforming
aggregated data into features useful for analysis and AI/ML.

## Quick Start

### Installation

Install **finagg** from GitHub directly.

```python
git clone https://github.com/theOGognf/finagg.git
cd finagg
pip install .
```

Optionally install the recommended datasets from 3rd party APIs into a local SQL database.

```python
finagg install
```

The installation will point you where to get free API keys and write them to a local
`.env` file for storage.

### Basic Usage

Explore the APIs directly.

```python
import finagg

# Get Bureau of Economic Analysis (BEA) data.
gdp = finagg.bea.api.get_gdp_by_industry.get(year=[2019, 2020, 2021])

# Get Federal Reserve Economic Data (FRED).
inflation_rate = finagg.fred.api.series.get("CPIAUCNS")

# Get Securities and Exchange Commission (SEC) filings.
facts = finagg.sec.api.company_facts.get(ticker="AAPL")
```

Or use downloaded data for exploring the most popular features.

```python
# Get the most popular FRED features all in one dataframe.
economic_data = finagg.fred.features.economic_features.from_sql()

# Get quarterly report features from SEC data.
quarterly_data = finagg.sec.features.quarterly_features.from_sql("AAPL")

# Get an aggregation of quarterly and daily features for a particular ticker.
fundamental_data = finagg.mixed.features.fundamental_features.from_sql("AAPL")
```

## Optional Installs

- `finagg[dev]` includes development dependencies for testing and static type checking.
- `finagg[learning]` includes canned PyTorch models and reinforcement learning environments.

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
- [**sec-edgar** is an implementation of a file-based SEC EDGAR parser.](https://github.com/sec-edgar/sec-edgar)

## Frequently Asked Questions

### Where should I start?

Aggregate some data, create some analysis notebooks, and train some RL agents! The package was
created to make it very easy to aggregate investment data, perform investment analysis, and train
trading/investment RL agents on a local, single GPU machine. Build custom models, observation and
reward functions, and rules-based trading algorithms.

### What Python versions are supported?

Python 3.10 and up are supported. We don't plan on supporting lower versions because 3.10 introduces
some nice quality of life updates that are used throughout the package.

### What operating systems are supported?

The package is developed and tested on both Linux and Windows, but we recommend using Linux or WSL
in practice. The package performs a good amount of I/O and interprocess operations that could result
in a noticeable performance degradation on Windows.
