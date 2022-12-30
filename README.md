# finagg: Financial Aggregation for Python

**finagg** is a Python package that provides implementations of popular financial APIs,
tools for aggregating data from those APIs in SQL databases, and tools for transforming
aggregated data into features useful for analysis and AI/ML.

## Quick Start

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
- [**requests-cache** for caching HTTP requests to help avoid getting throttled by 3rd party libraries.](https://requests-cache.readthedocs.io/en/stable/)
- [**SQLAlchemy** for a SQL Python interface.](https://www.sqlalchemy.org/)
- [**yfinance** for Yahoo! Finance data.](https://github.com/ranaroussi/yfinance)

## API References

- [**BEA API**](https://apps.bea.gov/api/signup/)
- [**FRED API**](https://fred.stlouisfed.org/docs/api/fred/)
- [**SEC API**](https://www.sec.gov/edgar/sec-api-documentation)

## Related Projects

- [**FinRL** is a collection of financial reinforcement learning environments and tools.](https://github.com/AI4Finance-Foundation/FinRL)
- [**sec-edgar** is an implementation of a file-based SEC EDGAR parser.](https://github.com/sec-edgar/sec-edgar)
