"""Abstract FRED API definition."""

import os
import pathlib
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, ClassVar

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
    str(_API_CACHE_PATH),
    ignored_parameters=["api_key", "file_type"],
    expire_after=timedelta(days=1),
)


class Dataset(ABC):
    """Abstract FRED API."""

    #: Request API URL endpoint after the base URL.
    url: ClassVar[str]

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a FRED API directly is not allowed. "
            "Use one of the `get` methods instead."
        )

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs) -> pd.DataFrame:
        """Main dataset API method."""


def get(url: str, /, **kwargs) -> requests.Response:
    """Main API get function used by all `Dataset.get` methods.

    Also formats FRED API parameters for convenience.

    Args:
        url: Request URL.
        **kwargs: Mapping of request parameter name to their value.

    Returns:
        A valid FRED API response.

    """
    response = session.get(url, params=pformat(**kwargs))
    response.raise_for_status()
    return response


def pformat(**kwargs) -> dict[str, Any]:
    """FRED API parameter formatting.

    Args:
        **kwargs: All possible FRED API parameters.

    Returns:
        Mapping of request parameter name to their value.

    """
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    for k in ("observation_start", "observation_end", "realtime_start", "realtime_end"):
        if k in kwargs:
            match kwargs[k]:
                case 0:
                    kwargs[k] = "1776-07-04"
                case -1:
                    kwargs[k] = "9999-12-31"

    for k in ("exclude_tag_names", "tag_names"):
        if k in kwargs:
            v = kwargs[k]
            if isinstance(v, str):
                v = [v]
            kwargs[k] = ";".join(v)

    for k in ("include_observation_values", "include_release_dates_with_no_data"):
        if k in kwargs:
            kwargs[k] = "true" if kwargs[k] else "false"

    if "search_text" in kwargs:
        k = "search_text"
        v = kwargs[k]
        if isinstance(v, str):
            v = [v]
        kwargs[k] = "+".join(v)

    if "vintage_dates" in kwargs:
        k = "vintage_dates"
        v = kwargs[k]
        if isinstance(v, str):
            v = [v]
        kwargs[k] = ",".join(v)

    api_key = kwargs.pop("api_key", None) or os.environ.get("FRED_API_KEY", None)
    if not api_key:
        raise RuntimeError(
            "No FRED API key found. "
            "Pass the API key to the API directly, or "
            "set the `FRED_API_KEY` environment variable."
        )
    kwargs.update({"api_key": api_key, "file_type": "json"})
    return kwargs
