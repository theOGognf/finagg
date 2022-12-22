"""Main package interface."""

from . import (
    _version,
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
