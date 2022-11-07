"""Abstract FRED API definition."""

import inspect
import os
import pathlib
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any

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


class Dataset(ABC):
    """Abstract FRED API."""

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a FRED API directly is not allowed. "
            "Use one of the `get` methods instead."
        )

    @classmethod
    @property
    @abstractmethod
    def endpoint(cls) -> str:
        """Request API URL endpoint after the base URL."""

    @classmethod
    @abstractmethod
    def get(cls, *, api_key: None | str = None) -> pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    @property
    def url(cls) -> str:
        """Full request API URL."""
        return f"https://api.stlouisfed.org/fred/{cls.endpoint}"


def pformat(*_) -> dict[str, Any]:
    """FRED API parameter formatting.

    A little bit of magic to format parameters passed
    to the `Dataset.get` method being called. Since
    the FRED API uses PEP 8 -style parameters, we can
    easily inspect the current method being called and format
    the parameters/args for the request.

    Really try to limit magic done in the project, but this
    makes the functions so much easier to implement and
    more maintainable (assuming the FRED API style doesn't change much).
    Credit to https://stackoverflow.com/a/65927265.

    Beyond the magic, this function handles additional formatting
    that the FRED API is expecting.

    Args:
        *_: No usage. Exists so linters don't complain.

    Returns:
        Mapping of request parameter name to their value.

    """
    frame = inspect.currentframe().f_back
    keys, _, _, values = inspect.getargvalues(frame)
    params = {k: values[k] for k in keys if k != "cls" and values[k] is not None}
    for k in ("realtime_start", "realtime_end"):
        if k in params:
            match params[k]:
                case 0:
                    params[k] = "1776-07-04"
                case 1:
                    params[k] = "9999-12-31"

    for k in ("exclude_tag_names", "tag_names"):
        if k in params:
            v = params[k]
            if isinstance(v, str):
                v = [v]
            v = ";".join(v)
            params[k] = v

    if "vintage_dates" in params:
        v = params[k]
        if isinstance(v, str):
            v = [v]
        v = ",".join(v)
        params[k] = v

    api_key = params.pop("api_key", None) or os.environ.get("FRED_API_KEY", None)
    if not api_key:
        raise RuntimeError(
            "No FRED API key found. "
            "Pass the API key to the API directly, or "
            "set the `FRED_API_KEY` environment variable."
        )
    params.update({"api_key": api_key, "file_type": "json"})
    return params


def get(url: str, params: dict, /) -> requests.Response:
    """Main API get function used by all `Dataset.get` methods."""
    response = session.get(url, params=params)
    response.raise_for_status()
    return response
