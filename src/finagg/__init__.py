"""Main package interface."""

from dotenv import load_dotenv

load_dotenv()

from . import (
    _version,
    backend,
    bea,
    fred,
    indices,
    install,
    mixed,
    sec,
    testing,
    utils,
    yfinance,
)

__version__ = _version.get_versions()["version"]
