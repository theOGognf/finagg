"""Main datbase initializaiton/installation script."""

from . import fred, sec, tickers, yfinance


def run() -> None:
    """Run all installation scripts for submodules."""
    tickers.install.run()
    sec.install.run()
    fred.install.run()
    yfinance.install.run()
