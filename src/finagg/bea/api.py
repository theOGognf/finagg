"""BEA API.

This implementation of the BEA API returns tables with normalized column names
and appropriately-casted dtypes. Throttling-prevention is handled internally,
sleeping for estimated quantities in an attempt to avoid server-side
rate-limiting.

See the official BEA API user guide for more info:
    https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

Examples:
    List datasets.
    >>> import finagg
    >>> finagg.bea.api.get_dataset_list()

    Listing parameters for GDP by industry.
    >>> finagg.bea.api.gdp_by_industry.get_parameter_list()

    Listing possible parameter values.
    >>> finagg.bea.api.gdp_by_industry.get_parameter_values("year")

    Getting GDP by industry for specific years.
    >>> finagg.bea.api.gdp_by_industry.get(year=[1995, 1996])

"""

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import cache
from typing import ClassVar, Literal, Sequence

import pandas as pd
import requests
import requests_cache

from .. import backend, ratelimit

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | finagg.bea.api - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

session = requests_cache.CachedSession(
    str(backend.http_cache_path),
    ignored_parameters=["ResultFormat"],
    expire_after=timedelta(days=1),
)

_YEAR = int | str


class _Dataset(ABC):
    """Interface for BEA Dataset APIs."""

    #: Request API URL.
    name: ClassVar[str]

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a BEA API directly is not allowed. "
            "Use one of the getter methods instead."
        )

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs) -> pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    def get_parameter_list(cls, /, *, api_key: None | str = None) -> pd.DataFrame:
        """Return the list of parameters associated with the dataset API."""
        return get_parameter_list(cls.name, api_key=api_key)

    @classmethod
    def get_parameter_values(
        cls, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        """Return all possible parameter values associated with the dataset API."""
        return get_parameter_values(cls.name, param, api_key=api_key)


class _FixedAssets(_Dataset):
    """US fixed assets (assets for long-term use).

    Details low-level US economic details.
    See `_GDPByIndustry` for more coarse/high-level industry data.

    """

    #: BEA dataset API name.
    name = "FixedAssets"

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
            results_data = get(params, api_key=api_key)
            data = results_data["Data"]
            df = (
                pd.DataFrame(data)
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
    name = "GdpByIndustry"

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
        (results_data,) = get(params, api_key=api_key)
        data = results_data["Data"]  # type: ignore
        df = pd.DataFrame(data)

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
    name = "InputOutput"

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
        (results_data,) = get(params, api_key=api_key)
        data = results_data["Data"]  # type: ignore
        return (
            pd.DataFrame(data)
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
    name = "NIPA"

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
            results_data = get(params, api_key=api_key)
            data = results_data["Data"]
            df = pd.DataFrame(data)
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


#: "FixedAssets" dataset API.
fixed_assets = _FixedAssets

#: "GdpByIndustry" dataset API.
gdp_by_industry = _GDPByIndustry

#: "InputOutput" dataset API.
input_output = _InputOutput

#: "NIPA" dataset API.
nipa = _NIPA

#: BEA API URL.
url = "https://apps.bea.gov/api/data"


def _api_error_as_response(error: dict) -> requests.Response:
    """Convert an API error to a :class:`requests.Response` object."""
    response = requests.Response()
    response.status_code = int(error.pop("APIErrorCode"))
    response._content = json.dumps(error).encode("utf-8")
    return response


@ratelimit.guard(
    [
        ratelimit.RequestLimit(90, timedelta(minutes=1)),
        ratelimit.ErrorLimit(20, timedelta(minutes=1)),
        ratelimit.SizeLimit(90e6, timedelta(minutes=1)),
    ],
)
def _guarded_get(url: str, params: dict, /) -> requests.Response:
    """Guarded version of `session.get`."""
    return session.get(url, params=params)


def get(
    params: dict,
    /,
    *,
    api_key: None | str = None,
) -> dict[str, list[dict]]:
    """Main get method used by dataset APIs.

    Handles throttle watchdog state updates, API key validation,
    and common formatting/parameters between API methods.

    Args:
        params: Params specific to the API method.

    Returns:
        A list of result dictionaries.

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

    params.update({"UserID": api_key, "ResultFormat": "JSON"})
    response = _guarded_get(url, params)
    response.raise_for_status()
    content = response.json()["BEAAPI"]
    if "Error" in content:
        error = _api_error_as_response(content["Error"])
        raise BEAAPIError(response.request, error, error.content)
    return content["Results"]  # type: ignore


@cache
def get_dataset_list(*, api_key: None | str = None) -> pd.DataFrame:
    """Return a list of datasets provided by the BEA API."""
    params = {
        "Method": "GetDatasetList",
    }
    results = get(params, api_key=api_key)["Dataset"]
    return pd.DataFrame(results)


@cache
def get_parameter_list(dataset: str, /, *, api_key: None | str = None) -> pd.DataFrame:
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
    results = get(params, api_key=api_key)["Parameter"]
    return pd.DataFrame(results)


@cache
def get_parameter_values(
    dataset: str, param: str, /, *, api_key: None | str = None
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
    results = get(params, api_key=api_key)["ParamValue"]
    return pd.DataFrame(results)


class BEAAPIError(requests.RequestException):
    """Custom exception for BEA API errors."""
