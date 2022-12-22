"""Main datbase initialization/installation script."""

from . import fred, indices, sec, yfinance


def run() -> None:
    """Run all installation scripts for submodules."""
    indices.install.run()
    sec.install.run()
    fred.install.run()
    yfinance.install.run()
