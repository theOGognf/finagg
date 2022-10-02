import json
import os
from typing import ClassVar

import pandas as pd
import requests


class BEAAPI:
    MAX_REQUESTS_PER_MINUTE: ClassVar[int] = 100

    MAX_VOLUME_PER_MINUTE: ClassVar[int] = 100e6

    MAX_ERRORS_PER_MINUTE: ClassVar[int] = 30

    URL: ClassVar[str] = "https://apps.bea.gov/api/data"

    def __init__(
        self,
        api_key: None | str = None,
        avoid_throttle: bool = True,
        retry: bool = True,
        return_type: type[pd.DataFrame | list[dict]] = pd.DataFrame,
    ) -> None:
        self.api_key = api_key or os.environ.get("BEA_API_KEY", None)
        if not self.api_key:
            raise RuntimeError(
                "No BEA API key found. "
                "Pass the API key to the API directly, or "
                "set the `BEA_API_KEY` environment variable."
            )
        self.avoid_throttle = avoid_throttle
        self.retry = retry
        self.return_type = return_type

    def describe(self, dataset: str, param: str, /) -> pd.DataFrame | list[dict]:
        """"""
        params = {
            "UserID": self.api_key,
            "method": "GetParameterValues",
            "datasetname": dataset,
            "ParameterName": param,
            "ResultFormat": "JSON",
        }
        response = requests.get(self.URL, params=params)
        results = json.loads(response.content)["BEAAPI"]["Results"]["ParamValue"]
        if self.return_type is pd.DataFrame:
            results = pd.DataFrame(results)
        return results

    def get(self) -> pd.DataFrame | list[dict]:
        ...


class describe(BEAAPI):
    def gdp_by_industry(self, param: str, /) -> pd.DataFrame | dict | str:

        r = requests.get(self.URL, params=params)
        return r.content


class get(BEAAPI):
    def gdp_by_industry(
        self,
    ) -> pd.DataFrame | dict | str:
        ...
