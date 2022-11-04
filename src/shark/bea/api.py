"""BEA API.

This implementation of the BEA API returns tables with normalized column names
and appropriately-casted dtypes. Throttling-prevention is handled internally,
sleeping for estimated quantities in an attempt to avoid server-side
rate-limiting.

See the official BEA API user guide for more info:
    https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

Examples:
    List datasets.
    >>> import shark
    >>> shark.bea.api.get_dataset_list()

    Listing parameters for GDP by industry.
    >>> shark.bea.api.gdp_by_industry.get_parameter_list()

    Listing possible parameter values.
    >>> shark.bea.api.gdp_by_industry.get_parameter_values("year")

    Getting GDP by industry for specific years.
    >>> shark.bea.api.gdp_by_industry.get(year=[1995, 1996])

"""

import json
import logging
import os
import pathlib
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import cache
from typing import ClassVar, Generic, Literal, Sequence, TypeVar

import pandas as pd
import requests
import requests_cache

logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.bea.api - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

_API_CACHE_PATH = os.environ.get(
    "BEA_API_CACHE_PATH",
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "bea_api_cache",
)
_API_CACHE_PATH = pathlib.Path(_API_CACHE_PATH)
_API_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

session = requests_cache.CachedSession(
    _API_CACHE_PATH,
    ignored_parameters=["UserId", "ResultFormat"],
    expire_after=timedelta(days=1),
)


_API_KEY = TypeVar("_API_KEY", bound=str)
_THROTTLE_WATCHDOG_STATE = TypeVar(
    "_THROTTLE_WATCHDOG_STATE", bound="_ThrottleWatchdog.State"
)
_YEAR = int | str


class _ThrottleWatchdog(Generic[_API_KEY, _THROTTLE_WATCHDOG_STATE]):
    """Throttling-prevention strategy. Tracks throttle metrics in BEA API responses."""

    @dataclass(frozen=True)
    class Response:
        """BEA API response with throttle-specific info."""

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
        """BEA API throttle state."""

        #: BEA API key associated with the state.
        api_key: str

        #: Deque of BEA API responses formatted for throttle-specific info.
        responses: deque["_ThrottleWatchdog.Response"]

        def __repr__(self) -> str:
            """Return a string representation of the state."""
            return (
                f"<{self.__class__.__qualname__}("
                f"api_key={self.api_key}, "
                f"errors_per_minute={self.errors_per_minute}, "
                f"requests_per_minute={self.requests_per_minute}, "
                f"volume_per_minute={self.volume_per_minute}"
                ")>"
            )

        @property
        def errors_per_minute(self) -> int:
            """Return BEA API response errors per minute."""
            self.pop()
            return sum([r.is_error for r in self.responses])

        @property
        def is_throttled(self) -> bool:
            """Are requests with the API key likely to be throttled?"""
            throttled = self.errors_per_minute >= _API.max_errors_per_minute
            throttled |= self.requests_per_minute >= _API.max_requests_per_minute
            throttled |= self.volume_per_minute >= _API.max_volume_per_minute
            return throttled

        @property
        def next_valid_request_dt(self) -> float:
            """Return the number of seconds needed to wait
            until another request can be made without throttling.

            """
            if not self.responses:
                return 0.0
            dt = self.youngest.retry_after
            if self.is_throttled:
                dt = max(
                    dt, 60 - (self.youngest.time - self.oldest.time).total_seconds()
                )
            return dt

        @property
        def oldest(self) -> "_ThrottleWatchdog.Response":
            """Return the oldest BEA API response formatted for throttle-specific info."""
            return self.responses[0]

        def pop(self) -> None:
            """Remove all responses older than 60 seconds."""
            while (
                self.responses
                and (self.youngest.time - self.oldest.time).total_seconds() > 60.0
            ):
                self.responses.popleft()

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


class _Dataset(ABC):
    """Interface for BEA Dataset APIs."""

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a BEA API directly is not allowed. "
            "Use one of the getter methods instead."
        )

    @classmethod
    @property
    @abstractmethod
    def name(cls) -> str:
        """Dataset API name."""

    @classmethod
    @abstractmethod
    def get(cls, *, api_key: None | str = None) -> pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    def get_parameter_list(cls, /, *, api_key: None | str = None) -> pd.DataFrame:
        """Return the list of parameters associated with the dataset API."""
        return _API.get_parameter_list(cls.name, api_key=api_key)

    @classmethod
    def get_parameter_values(
        cls, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        """Return all possible parameter values associated with the dataset API."""
        return _API.get_parameter_values(cls.name, param, api_key=api_key)


class _FixedAssets(_Dataset):
    """US fixed assets (assets for long-term use).

    Details low-level US economic details.
    See `_GDPByIndustry` for more coarse/high-level industry data.

    """

    #: BEA dataset API name.
    name: ClassVar[str] = "FixedAssets"

    @classmethod
    def get(
        cls,
        table_id: str | Sequence[str] = "ALL",
        year: _YEAR | Sequence[_YEAR] = "ALL",
        *,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get US fixed assets by asset and year.

        Args:
            table_id: IDs associated with assets of concern.
                Use :meth:`get_parameter_values` to see possible values.
            year: Years to return.

        Returns:
            Dataframe with normalized column names and true dtypes.

        """
        if table_id == "ALL":
            table_id = cls.get_parameter_values("TableName")["TableName"].to_list()
        elif isinstance(table_id, str):
            table_id = [table_id]

        results = []
        for tid in table_id:
            params = {
                "Method": "GetData",
                "DatasetName": cls.name,
                "TableName": tid,
                "Year": year,
            }
            df = _API.get(params, api_key=api_key)
            df = df["Data"]
            df = (
                pd.DataFrame(df)
                .drop("NoteRef", axis=1)
                .rename(
                    columns={
                        "TableName": "table_id",
                        "SeriesCode": "series_code",
                        "LineNumber": "line",
                        "LineDescription": "line_description",
                        "TimePeriod": "year",
                        "METRIC_NAME": "metric",
                        "CL_UNIT": "units",
                        "UNIT_MULT": "e",
                        "DataValue": "value",
                    }
                )
                .astype(
                    {
                        "table_id": "category",
                        "series_code": "category",
                        "line": "int16",
                        "line_description": "object",
                        "year": "int16",
                        "metric": "category",
                        "units": "category",
                        "e": "int16",
                    }
                )
            )
            df["value"] = df["value"].str.replace(",", "").astype("float32")
            results.append(df)
        return pd.concat(results)


class _GDPByIndustry(_Dataset):
    """GDP (a single summary statistic) for each industry.

    Data provided by this API is considered coarse/high-level.
    See `_InputOutput` for more granular/low-level industry data.

    """

    #: BEA dataset API name.
    name: ClassVar[str] = "GdpByIndustry"

    @classmethod
    def get(
        cls,
        table_id: str | Sequence[str] = "ALL",
        freq: Literal["A", "Q", "A,Q"] = "Q",
        year: _YEAR | Sequence[_YEAR] = "ALL",
        industry: str | Sequence[str] = "ALL",
        *,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get GDP by industry.

        Args:
            table_id: IDs associated with GDP value type. Use :meth:`get_parameter_values`
                to see possible values.
            freq: Data frequency to return. `"Q"` for quarterly, `"A"` for annually.
            year: Years to return.
            industry: IDs associated with industries. Use :meth:`get_parameter_values`
                to see possible values.

        Returns:
            Dataframe with normalized column names and true dtypes.

        """
        params = {
            "Method": "GetData",
            "DatasetName": cls.name,
            "TableID": table_id,
            "Frequency": freq,
            "Year": year,
            "Industry": industry,
        }
        (df,) = _API.get(params, api_key=api_key)
        df = df["Data"]
        df = pd.DataFrame(df)

        def _roman_to_int(item: str) -> int:
            _map = {"I": 1, "II": 2, "III": 3, "IV": 4}
            return _map[item]

        df["Quarter"] = df["Quarter"].apply(_roman_to_int)
        df.drop("NoteRef", axis=1, inplace=True)
        return df.rename(
            columns={
                "TableID": "table_id",
                "Frequency": "freq",
                "Year": "year",
                "Quarter": "quarter",
                "Industry": "industry",
                "IndustrYDescription": "industry_description",
                "DataValue": "value",
            }
        ).astype(
            {
                "table_id": "int16",
                "freq": "category",
                "year": "int16",
                "quarter": "category",
                "industry": "category",
                "industry_description": "object",
                "value": "float32",
            }
        )


class _InputOutput(_Dataset):
    """Specific input-output statistics for each industry.

    Data provided by this API is considered granular/low-level.
    See `_GDPByIndustry` for more coarse/high-level industry data.

    Data is provided for different "rows" and "columns" where:
        - a row is an industry and
        - a column is a statistic associated with that industry

    Columns are divided by column codes. Each industry of similar
    type has the same set of column codes that provide input-output
    statistics for that industry.

    """

    #: BEA dataset API name.
    name: ClassVar[str] = "InputOutput"

    @classmethod
    def get(
        cls,
        table_id: str | Sequence[str] = "ALL",
        year: _YEAR | Sequence[_YEAR] = "ALL",
        *,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get input-output statistics by industry.

        Args:
            table_id: IDs associated with input-output stats. Use :meth:`get_parameter_values`
                to see possible values.
            year: Years to return.

        Returns:
            Dataframe with normalized column names and true dtypes.

        """
        params = {
            "Method": "GetData",
            "DatasetName": cls.name,
            "TableID": table_id,
            "Year": year,
        }
        (results,) = _API.get(params, api_key=api_key)
        results = results["Data"]
        return (
            pd.DataFrame(results)
            .drop("NoteRef", axis=1)
            .rename(
                columns={
                    "TableID": "table_id",
                    "Year": "year",
                    "RowCode": "row_code",
                    "RowDescr": "row_description",
                    "RowType": "row_type",
                    "ColCode": "col_code",
                    "ColDescr": "col_description",
                    "ColType": "col_type",
                    "DataValue": "value",
                }
            )
            .astype(
                {
                    "table_id": "int16",
                    "year": "int16",
                    "row_code": "category",
                    "row_description": "object",
                    "row_type": "category",
                    "col_code": "category",
                    "col_description": "object",
                    "col_type": "category",
                    "value": "float32",
                }
            )
        )


class _NIPA(_Dataset):
    """National income and product accounts.

    Details high-level US economic details in several
    metrics.

    """

    #: BEA dataset API name.
    name: ClassVar[str] = "NIPA"

    @classmethod
    def get(
        cls,
        table_id: str | Sequence[str] = "ALL",
        freq: Literal["A", "Q", "A,Q"] = "Q",
        year: _YEAR | Sequence[_YEAR] = "ALL",
        *,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get US income and product accounts by metric.

        Args:
            table_id: IDs associated with metric of concern.
                Use :meth:`get_parameter_values` to see possible values.
            freq: Data frequency to return. `"Q"` for quarterly, `"A"` for annually.
            year: Years to return.

        Returns:
            Dataframe with normalized column names and true dtypes.

        """
        if table_id == "ALL":
            table_id = cls.get_parameter_values("TableName")["TableName"].to_list()
        elif isinstance(table_id, str):
            table_id = [table_id]

        results = []
        for tid in table_id:
            params = {
                "Method": "GetData",
                "DatasetName": cls.name,
                "TableName": tid,
                "Year": year,
                "Frequency": freq,
            }
            df = _API.get(params, api_key=api_key)
            df = df["Data"]
            df = pd.DataFrame(df)
            df[["Year", "Quarter"]] = df["TimePeriod"].str.split("Q", n=1, expand=True)
            df["Quarter"] = df["Quarter"].astype(int)
            df.drop(["TimePeriod", "NoteRef"], axis=1, inplace=True)
            df = df.rename(
                columns={
                    "TableName": "table_id",
                    "SeriesCode": "series_code",
                    "LineNumber": "line",
                    "LineDescription": "line_description",
                    "Year": "year",
                    "Quarter": "quarter",
                    "METRIC_NAME": "metric",
                    "CL_UNIT": "units",
                    "UNIT_MULT": "e",
                    "DataValue": "value",
                }
            ).astype(
                {
                    "table_id": "category",
                    "series_code": "category",
                    "line": "int16",
                    "line_description": "object",
                    "year": "int16",
                    "quarter": "int16",
                    "metric": "category",
                    "units": "category",
                    "e": "int16",
                }
            )
            df["value"] = df["value"].str.replace(",", "").astype("float32")
            results.append(df)
        return pd.concat(results)


class _API:
    """Collection of BEA APIs."""

    #: Throttling-prevention strategy. Tracks throttling metrics for each API key.
    _throttle_watchdog: ClassVar[_ThrottleWatchdog] = _ThrottleWatchdog()

    #: Path to BEA API requests cache.
    cache_path: ClassVar[str] = str(_API_CACHE_PATH)

    #: "FixedAssets" dataset API.
    fixed_assets: ClassVar[type[_FixedAssets]] = _FixedAssets

    #: "GdpByIndustry" dataset API.
    gdp_by_industry: ClassVar[type[_GDPByIndustry]] = _GDPByIndustry

    #: "InputOutput" dataset API.
    input_output: ClassVar[type[_InputOutput]] = _InputOutput

    #: Max allowed BEA API errors per minute.
    max_errors_per_minute: ClassVar[int] = 30

    #: Max allowed BEA API requests per minute.
    max_requests_per_minute: ClassVar[int] = 100

    #: Max allowed BEA API response size (in MB) per minute.
    max_volume_per_minute: ClassVar[int] = 100e6

    #: "NIPA" dataset API.
    nipa: ClassVar[type[_NIPA]] = _NIPA

    #: BEA API URL.
    url: ClassVar[str] = "https://apps.bea.gov/api/data"

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a BEA API directly is not allowed. "
            "Use one of the getter methods instead."
        )

    @classmethod
    def _api_error_as_response(cls, error: dict) -> requests.Response:
        """Convert an API error to a :class:`requests.Response` object."""
        response = requests.Response()
        response.status_code = int(error.pop("APIErrorCode"))
        response._content = json.dumps(error)
        return response

    @classmethod
    def get(
        cls,
        params: dict,
        /,
        *,
        api_key: None | str = None,
        results_key: None | str = None,
        return_type: None | type[pd.DataFrame] = None,
    ) -> list[dict] | pd.DataFrame:
        """Main get method used by dataset APIs.

        Handles throttle watchdog state updates, API key validation,
        and common formatting/parameters between API methods.

        Args:
            params: Params specific to the API method.

        Returns:
            A list of result dictionaries or a dataframe, depending
            on the dataset.

        Raises:
            RuntimeError: If no BEA API key is passed or found.
            BEAAPIException: If a BEA API error occurs.

        """
        api_key = api_key or os.environ.get("BEA_API_KEY", None)
        if not api_key:
            raise RuntimeError(
                "No BEA API key found. "
                "Pass the API key to the API directly, or "
                "set the `BEA_API_KEY` environment variable."
            )
        next_valid_request_dt = cls._throttle_watchdog[api_key].next_valid_request_dt
        if next_valid_request_dt > 0:
            logger.warning(
                f"API key ending in `{api_key[-4:]}` may be throttled. "
                f"Blocking until the next available request for {next_valid_request_dt:.2f} second(s)."
            )
        time.sleep(next_valid_request_dt)
        params.update({"UserID": api_key, "ResultFormat": "JSON"})
        response = session.get(cls.url, params=params)
        cls._throttle_watchdog.update(api_key, response)
        response.raise_for_status()
        content = json.loads(response.content)["BEAAPI"]
        if "Error" in content:
            error = cls._api_error_as_response(content["Error"])
            raise BEAAPIError(response.request, error, error.content)
        results = content["Results"]
        if results_key:
            results = results[results_key]
        if return_type:
            results = return_type(results)
        return results

    @classmethod
    @cache
    def get_dataset_list(cls, /, *, api_key: None | str = None) -> pd.DataFrame:
        """Return a list of datasets provided by the BEA API."""
        params = {
            "Method": "GetDatasetList",
        }
        return cls.get(
            params, api_key=api_key, results_key="Dataset", return_type=pd.DataFrame
        )

    @classmethod
    @cache
    def get_parameter_list(
        cls, dataset: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        """Get a dataset's list of parameters.

        Args:
            dataset: Dataset API to inspect. See meth:`get_dataset_list` for a
                list of datasets.

        Returns:
            Dataframe listing the dataset's parameters.

        """
        params = {
            "Method": "GetParameterList",
            "DatasetName": dataset,
        }
        return cls.get(
            params, api_key=api_key, results_key="Parameter", return_type=pd.DataFrame
        )

    @classmethod
    @cache
    def get_parameter_values(
        cls, dataset: str, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        """Get potential values for a dataset's parameter.

        Args:
            dataset: Dataset API to inspect. See meth:`get_dataset_list` for
                list of datasets.
            param: Dataset API's parameter to inspect.

        Returns:
            Dataframe describing the dataset's parameter values.

        """
        params = {
            "Method": "GetParameterValues",
            "DatasetName": dataset,
            "ParameterName": param,
        }
        return cls.get(
            params, api_key=api_key, results_key="ParamValue", return_type=pd.DataFrame
        )


#: Public-facing BEA API.
api = _API


class BEAAPIError(requests.RequestException):
    """Custom exception for BEA API errors."""
