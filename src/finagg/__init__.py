"""Main package interface."""

from importlib.metadata import PackageNotFoundError, version

from dotenv import load_dotenv

load_dotenv()

from . import (
    backend,
    bea,
    fred,
    fundamentals,
    indices,
    install,
    sec,
    testing,
    utils,
    yfinance,
)

try:
    __version__ = version("finagg")
except PackageNotFoundError:
    pass
