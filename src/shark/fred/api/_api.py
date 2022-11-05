"""Abstract FRED API definition."""

import os
import pathlib
from abc import ABC, abstractmethod
from datetime import timedelta

import pandas as pd
import requests
import requests_cache

_API_CACHE_PATH = os.environ.get(
    "FRED_API_CACHE_PATH",
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "fred_api_cache",
)
_API_CACHE_PATH = pathlib.Path(_API_CACHE_PATH)
_API_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

session = requests_cache.CachedSession(
    _API_CACHE_PATH,
    ignored_parameters=["api_key", "file_type"],
    expire_after=timedelta(days=1),
)


class _Dataset(ABC):
    """Abstract FRED API."""

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a FRED API directly is not allowed. "
            "Use one of the `get` methods instead."
        )

    @classmethod
    @abstractmethod
    def get(cls, *, api_key: None | str = None) -> pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    @property
    @abstractmethod
    def url(cls) -> str:
        """Request API URL."""


def get(url: str, params: dict, /, *, api_key: None | str = None) -> requests.Response:
    """Main API get function used by all `_Dataset` classes."""
    api_key = api_key or os.environ.get("FRED_API_KEY", None)
    if not api_key:
        raise RuntimeError(
            "No FRED API key found. "
            "Pass the API key to the API directly, or "
            "set the `FRED_API_KEY` environment variable."
        )
    params.update({"api_key": api_key, "file_type": "json"})
    response = session.get(url, params=params)
    response.raise_for_status()
    return response
