import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar, Literal

import pandas as pd
import requests
import requests_cache

requests_cache.install_cache("bea_api", ignored_parameters=["UserId", "ResultFormat"])


class ThrottleWatchdog:
    @dataclass
    class Request:
        time: datetime

        size: int

        is_error: bool

    @dataclass
    class State:
        api_key: str

        requests: list["ThrottleWatchdog.Request"]

        @property
        def errors_per_minute(self) -> int:
            ...

        @property
        def next_valid_request_dt(self) -> float:
            ...

        @property
        def requests_per_minute(self) -> int:
            ...

        @property
        def volume_per_minute(self) -> int:
            ...


throttle_watchdog = ThrottleWatchdog()


class DatasetAPI(ABC):
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
        return API.get_parameter_list(cls.DATASET, api_key=api_key)

    @classmethod
    def get_parameter_values(
        cls, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        return API.get_parameter_values(cls.DATASET, param, api_key=api_key)


class GDPByIndustry(DatasetAPI):
    DATASET: ClassVar[str] = "GdpByIndustry"

    def get(
        self,
        table_id: str = "ALL",
        freq: Literal["A", "Q"] = "Q",
        industry: str = "ALL",
        *,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        API.get()


class API:
    gdp_by_industry: ClassVar[type[GDPByIndustry]] = GDPByIndustry

    MAX_REQUESTS_PER_MINUTE: ClassVar[int] = 100

    MAX_VOLUME_PER_MINUTE: ClassVar[int] = 100e6

    MAX_ERRORS_PER_MINUTE: ClassVar[int] = 30

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
        params.update({"UserID": api_key, "ResultFormat": "JSON"})
        response = requests.get(cls.URL, params=params)
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
