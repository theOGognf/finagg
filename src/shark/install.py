"""Main datbase initialization/installation script."""

from . import fred, indices, sec, yfinance


def run() -> None:
    """Run all installation scripts for submodules."""
    fred.install.run()
    indices.install.run()
    sec.install.run()
    yfinance.install.run()
