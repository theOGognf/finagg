import json
import os
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import ClassVar, Generic, Literal, TypeVar

import pandas as pd
import requests
import requests_cache

requests_cache.install_cache("bea_api", ignored_parameters=["UserId", "ResultFormat"])

_K = TypeVar("_K", bound=str)
_V = TypeVar("_V", bound="_ThrottleWatchdog.State")


class _ThrottleWatchdog(Generic[_K, _V]):
    @dataclass
    class Response:
        #: Time the response was received and processed.
        time: datetime

        #: Response size in bytes.
        size: int

        #: Whether the response resulted in an API error.
        is_error: bool

        #: Time to wait in seconds if responses are being throttled.
        retry_after: float

    @dataclass(repr=False)
    class State:
        #: BEA API key associated with the state.
        api_key: str

        #: Deque of BEA API responses formatted for throttle-specific info.
        responses: deque["_ThrottleWatchdog.Response"]

        def __repr__(self) -> str:
            """Return a string representation of the state."""
            return f"<{self.__class__.__qualname__}(api_key={self.api_key}, errors_per_minute={self.errors_per_minute}, requests_per_minute={self.requests_per_minute}, volume_per_minute={self.volume_per_minute})>"

        @property
        def errors_per_minute(self) -> int:
            """Return BEA API response errors per minute."""
            self.pop()
            return sum([r.is_error for r in self.responses])

        def pop(self) -> None:
            """Remove all responses older than 60 seconds."""
            while (
                self.responses
                and (self.youngest.time - self.oldest.time).total_seconds() > 60.0
            ):
                self.responses.popleft()

        @property
        def next_valid_request_dt(self) -> float:
            """Return the number of seconds needed to wait
            until another request can be made without throttling.

            """
            if not self.responses:
                return 0.0
            dt = self.youngest.retry_after
            throttled = self.errors_per_minute >= _API.MAX_ERRORS_PER_MINUTE
            throttled |= self.requests_per_minute >= _API.MAX_REQUESTS_PER_MINUTE
            throttled |= self.volume_per_minute >= _API.MAX_VOLUME_PER_MINUTE
            if throttled:
                dt = max(
                    dt, 60 - (self.youngest.time - self.oldest.time).total_seconds()
                )
            return dt

        @property
        def oldest(self) -> "_ThrottleWatchdog.Response":
            """Return the oldest BEA API response formatted for throttle-specific info."""
            return self.responses[0]

        @property
        def requests_per_minute(self) -> int:
            """Return BEA API response requests per minute."""
            self.pop()
            return len(self.responses)

        def update(
            self, response: requests_cache.CachedResponse | requests.Response
        ) -> float:
            """Update the throttle state associated with the API key.

            Args:
                response: Raw BEA API response.

            Returns:
                Time needed to wait until another request can be made without throttling.

            """
            if hasattr(response, "from_cache") and response.from_cache:
                return 0.0
            retry_after = (
                float(response.headers["Retry-After"])
                if response.status_code == 429
                else 0.0
            )
            self.responses.append(
                _ThrottleWatchdog.Response(
                    datetime.now(tz=timezone.utc),
                    len(response.content),
                    response.status_code != 200,
                    retry_after,
                )
            )
            return self.next_valid_request_dt

        @property
        def volume_per_minute(self) -> int:
            """Return BEA API response volume per minute."""
            self.pop()
            return sum([r.size for r in self.responses])

        @property
        def youngest(self) -> "_ThrottleWatchdog.Response":
            """Return the youngest BEA API response formatted for throttle-specific info."""
            return self.responses[-1]

    #: Mapping of BEA API key to throttle state associated with that API key.
    states: dict[str, "_ThrottleWatchdog.State"]

    def __init__(self) -> None:
        self.states = {}

    def __getitem__(self, api_key: str) -> "_ThrottleWatchdog.State":
        """Get the throttle state associated with the given API key."""
        if api_key not in self.states:
            self.states[api_key] = _ThrottleWatchdog.State(api_key, deque())
        return self.states[api_key]

    def update(self, api_key: str, response: requests.Response) -> float:
        """Update the throttle state associated with the given API key.

        Args:
            response: Raw BEA API response.

        Returns:
            Time needed to wait until another request can be made without throttling.

        """
        return self.states[api_key].update(response)


class _DatasetAPI(ABC):
    @classmethod
    @property
    @abstractmethod
    def DATASET(cls) -> str:
        ...

    @classmethod
    @abstractmethod
    def get(cls, *, api_key: None | str = None) -> pd.DataFrame:
        ...

    @classmethod
    def get_parameter_list(cls, /, *, api_key: None | str = None) -> pd.DataFrame:
        return _API.get_parameter_list(cls.DATASET, api_key=api_key)

    @classmethod
    def get_parameter_values(
        cls, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        return _API.get_parameter_values(cls.DATASET, param, api_key=api_key)


class _GDPByIndustry(_DatasetAPI):
    DATASET: ClassVar[str] = "GdpByIndustry"

    def get(
        self,
        table_id: str = "ALL",
        freq: Literal["A", "Q"] = "Q",
        industry: str = "ALL",
        *,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        _API.get()


class _API:
    #: "GdpByIndustry" dataset API.
    gdp_by_industry: ClassVar[type[_GDPByIndustry]] = _GDPByIndustry

    #: Max allowed BEA API errors per minute.
    MAX_ERRORS_PER_MINUTE: ClassVar[int] = 30

    #: Max allowed BEA API requests per minute.
    MAX_REQUESTS_PER_MINUTE: ClassVar[int] = 100

    #: Max allowed BEA API response size (in MB) per minute.
    MAX_VOLUME_PER_MINUTE: ClassVar[int] = 100e6

    #: Throttling-prevention strategy. Tracks throttling metrics for each API key.
    throttle_watchdog: ClassVar[_ThrottleWatchdog] = _ThrottleWatchdog()

    #: BEA API URL.
    URL: ClassVar[str] = "https://apps.bea.gov/api/data"

    @classmethod
    def get(
        cls, params: dict, results_key: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        api_key = api_key or os.environ.get("BEA_API_KEY", None)
        if not api_key:
            raise RuntimeError(
                "No BEA API key found. "
                "Pass the API key to the API directly, or "
                "set the `BEA_API_KEY` environment variable."
            )
        time.sleep(cls.throttle_watchdog[api_key].next_valid_request_dt)
        params.update({"UserID": api_key, "ResultFormat": "JSON"})
        response = requests.get(cls.URL, params=params)
        cls.throttle_watchdog.update(api_key, response)
        results = json.loads(response.content)["BEAAPI"]["Results"][results_key]
        return pd.DataFrame(results)

    @classmethod
    def get_parameter_list(
        cls, dataset: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "Method": "GetParameterList",
            "DatasetName": dataset,
        }
        return cls.get(params, "Parameter", api_key=api_key)

    @classmethod
    def get_parameter_values(
        cls, dataset: str, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "Method": "GetParameterValues",
            "DatasetName": dataset,
            "ParameterName": param,
        }
        return cls.get(params, "ParamValue", api_key=api_key)


#: Public-facing BEA API.
api = _API
