"""Main package interface."""

from . import _version, bea, fred, install, mixed, sec, tickers, utils, yfinance

__version__ = _version.get_versions()["version"]
