"""Main package interface."""

from . import _version, bea, eng, fred, install, sec, tickers, utils, yfinance

__version__ = _version.get_versions()["version"]
