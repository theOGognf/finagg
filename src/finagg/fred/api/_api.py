"""Abstract FRED API definition."""

import os
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import partial
from typing import Any, ClassVar

import pandas as pd
import requests
import requests_cache

from ... import config, ratelimit

if config.disable_http_cache:
    session = requests.Session()
else:
    session = requests_cache.CachedSession(
        str(config.http_cache_path),
        ignored_parameters=["api_key", "file_type"],
        expire_after=timedelta(weeks=1),
    )


class API(ABC):
    """Abstract FRED API."""

    #: Request API URL.
    url: ClassVar[str]

    @classmethod
    @abstractmethod
    def get(cls, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Main dataset API method."""


@ratelimit.guard([ratelimit.RequestLimit(120, timedelta(minutes=1))])
def get(url: str, /, **kwargs: Any) -> requests.Response:
    """Main API get function used by all `Dataset.get` methods.

    Also formats FRED API parameters for convenience.

    Args:
        url: Request URL.
        **kwargs: Mapping of request parameter name to their value.

    Returns:
        A valid FRED API response.

    """
    params = pformat(**kwargs)
    if params.pop("cache", True):
        response = session.get(url, params=params)
    else:
        response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def maybe_paginate(data_key: str, url: str, /, **kwargs: Any) -> pd.DataFrame:
    """Do pagination for API get functions that support pagination (if
    pagination is enabled).

    Args:
        data_key: Key from the response that the underlying data resides in.
        url: Request URL.
        **kwargs: Mapping of request parameter name to their value.

    Returns:
        A dataframe containing (possibly all) data results.

    """
    do_paginate = kwargs.pop("paginate", False)
    offset = kwargs.pop("offset", 0)
    getter = partial(get, url, **kwargs)

    buffer: list[dict[str, Any]] = []
    batch_size = kwargs.get("limit", 1000)
    data = {"count": offset}
    i = offset
    while not buffer or i <= data["count"]:
        data = getter(offset=i).json()
        buffer.extend(data[data_key])
        if not do_paginate:
            break
        i += batch_size
    return pd.DataFrame(buffer)


def pformat(**kwargs: Any) -> dict[str, Any]:
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

    for k in ("search_text", "series_search_text", "tag_search_text"):
        if k in kwargs:
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
