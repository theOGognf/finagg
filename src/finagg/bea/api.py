"""An implementation of the Bureau of Economic Analysis (BEA) API.

A BEA API key is required to use this API. You can register for
a BEA API key at the `BEA API signup page`_. You can pass your
BEA API key directly to the implemented API getters, or you
can set the ``BEA_API_KEY`` environment variable to have the
BEA API key be passed to the implemented API getters for you.

Alternatively, running ``finagg bea install`` (or the broader
``finagg install``) will  prompt you where to aquire a BEA API
key and will automatically store it in an ``.env`` file in
your current working directory. The environment variables set
in that ``.env`` file will be loaded into your shell upon
using ``finagg`` (whether that be through the Python interface
or through the CLI tools).

See the official `BEA API user guide`_ for more info on the BEA API.

.. _`BEA API signup page`: https://apps.bea.gov/api/signup/
.. _`BEA API user guide`: https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf

.. note::
    This was the first API implementation in this project, but has since
    lost priority in favor of the FRED API as the FRED API provides
    data that's found through the BEA API in addition to a plethora
    of other data. This BEA API implementation is still maintained
    and supported, but other features such as data installation are not
    and will never be supported.

"""

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, ClassVar, Literal, Sequence

import pandas as pd
import requests
import requests_cache

from .. import backend, ratelimit

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

session = requests_cache.CachedSession(
    str(backend.http_cache_path),
    ignored_parameters=["ResultFormat"],
    expire_after=timedelta(days=1),
)

_YEAR = int | str


class _API(ABC):
    """Interface for BEA Dataset APIs."""

    #: Request API URL.
    name: ClassVar[str]

    @classmethod
    @abstractmethod
    def get(cls, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    def get_parameter_list(cls, /, *, api_key: None | str = None) -> pd.DataFrame:
        """Return the list of parameters associated with the dataset API."""
        return _get_parameter_list(cls.name, api_key=api_key)

    @classmethod
    def get_parameter_values(
        cls, param: str, /, *, api_key: None | str = None
    ) -> pd.DataFrame:
        """Return all possible parameter values associated with the dataset API."""
        return _get_parameter_values(cls.name, param, api_key=api_key)


class FixedAssets(_API):
    """US fixed assets (assets for long-term use)."""

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
            results_data = _get(params, api_key=api_key)
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


class GDPByIndustry(_API):
    """GDP (a single summary statistic) for each industry.

    The module variable :data:`finagg.bea.api.gdp_by_industry` is an instance
    of this API implementation and is the most popular interface for querying
    this API.

    Data provided by this API is considered coarse/high-level.
    See :class:`InputOutput` for more granular/low-level industry data.

    Examples:
        List the GDP by industry API parameters.

        >>> finagg.bea.api.gdp_by_industry.get_parameter_list()  # doctest: +ELLIPSIS
          ParameterName ParameterDataType                               ParameterDescription ... AllValue
        0     Frequency            string                            A - Annual, Q-Quarterly ...      ALL
        1      Industry            string       List of industries to retrieve (ALL for All) ...      ALL
        2       TableID           integer  The unique GDP by Industry table identifier (A... ...      ALL
        3          Year           integer  List of year(s) of data to retrieve (ALL for All) ...      ALL

        List possible GDP by industry tables we can query.

        >>> finagg.bea.api.gdp_by_industry.get_parameter_values("TableID").head(5)
                                                  ParamValue
        0  {'Key': '1', 'Desc': 'Value Added by Industry ...
        1  {'Key': '5', 'Desc': 'Value added by Industry ...
        2  {'Key': '6', 'Desc': 'Components of Value Adde...
        3  {'Key': '7', 'Desc': 'Components of Value Adde...
        4  {'Key': '8', 'Desc': 'Chain-Type Quantity Inde...

    """

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
                to see possible values. `"ALL"` indicates retrieve all GDP value
                measurement type tables.
            freq: Data frequency to return. `"Q"` for quarterly, `"A"` for
                annually, and `"A,Q"` for both annually and quarterly.
            year: Years to return. `"ALL"` indicates retrieve data for all
                available years.
            industry: IDs associated with industries. Use :meth:`get_parameter_values`
                to see possible values.

        Returns:
            Dataframe with GDP by industry, separated by year and/or quarter.

        Examples:
            Get the GDP value added by an industry for a specific year.

            >>> finagg.bea.api.gdp_by_industry.get(table_id=1, freq="A", year=2020).head(5)
               table_id freq  year quarter industry                         industry_description  value
            0         1    A  2020    2020       11  Agriculture, forestry, fishing, and hunting  162.2
            1         1    A  2020    2020    111CA                                        Farms  120.7
            2         1    A  2020    2020    113FF    Forestry, fishing, and related activities   41.5
            3         1    A  2020    2020       21                                       Mining  201.1
            4         1    A  2020    2020      211                       Oil and gas extraction  110.9

        """
        params = {
            "Method": "GetData",
            "DatasetName": cls.name,
            "TableID": table_id,
            "Frequency": freq,
            "Year": year,
            "Industry": industry,
        }
        (results_data,) = _get(params, api_key=api_key)
        data = results_data["Data"]  # type: ignore
        df = pd.DataFrame(data)

        if freq != "A":

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


class InputOutput(_API):
    """Specific input-output statistics for each industry.

    Data provided by this API is considered granular/low-level.
    See :class:`GDPByIndustry` for more coarse/high-level industry data.

    Data is provided for different "rows" and "columns" where:
        - a row is an industry and
        - a column is a statistic associated with that industry

    Columns are divided by column codes. Each industry of similar
    type has the same set of column codes that provide input-output
    statistics for that industry.

    """

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
            table_id: IDs associated with input-output stats. Use
                :meth:`get_parameter_values` to see possible values.
                `"ALL"` indicates retrieve all tables for all types
                of input-output statistics by industry.
            year: Years to return. `"ALL"` indicates retrieve data for all
                available years.

        Returns:
            Dataframe organized by table row and column codes.

        """
        params = {
            "Method": "GetData",
            "DatasetName": cls.name,
            "TableID": table_id,
            "Year": year,
        }
        (results_data,) = _get(params, api_key=api_key)
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


class NIPA(_API):
    """National income and product accounts.

    Details high-level US economic details in several
    metrics.

    """

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
            results_data = _get(params, api_key=api_key)
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


fixed_assets = FixedAssets()
"""The most popular way for accessing the :class:`FixedAssets` API
implementation.

:meta hide-value:
"""

gdp_by_industry = GDPByIndustry()
"""The most popular way for accessing the :class:`GDPByIndustry` API
implementation.

:meta hide-value:
"""

input_output = InputOutput()
"""The most popular way for accessing the :class:`InputOutput` API
implementation.

:meta hide-value:
"""

nipa = NIPA()
"""The most popular way for accessing the :class:`NIPA` API implementation.

:meta hide-value:
"""

#: The BEA API endpoint URL. All API requests are made to this URL.
url = "https://apps.bea.gov/api/data"


def _api_error_as_response(error: dict[str, str]) -> requests.Response:
    """Convert an API error to a :class:`requests.Response` object."""
    response = requests.Response()
    response.status_code = int(error.pop("APIErrorCode"))
    response._content = json.dumps(error).encode("utf-8")
    return response


def _get(
    params: dict[str, Any],
    /,
    *,
    api_key: None | str = None,
) -> dict[str, list[dict[str, Any]]]:
    """Main get method used by dataset APIs.

    Handles API key validation, formatting, and parameters common
    amongst several API methods.

    Args:
        params: Params specific to the API method.

    Returns:
        Results from the API request (typically a list of records).

    Raises:
        `RuntimeError`: If no BEA API key is passed or found.
        `BEAAPIError`: If a BEA API error occurs.

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
    if "Error" in content["Results"]:
        error = _api_error_as_response(content["Results"]["Error"])
        raise BEAAPIError(response.request, error, error.content)
    return content["Results"]  # type: ignore


def _get_parameter_list(dataset: str, /, *, api_key: None | str = None) -> pd.DataFrame:
    """Get a dataset's list of parameters.

    Args:
        dataset: Dataset API to inspect. See :meth:`get_dataset_list` for a
            list of datasets.

    Returns:
        Dataframe listing the dataset's parameters.

    """
    params = {
        "Method": "GetParameterList",
        "DatasetName": dataset,
    }
    results = _get(params, api_key=api_key)["Parameter"]
    return pd.DataFrame(results)


def _get_parameter_values(
    dataset: str, param: str, /, *, api_key: None | str = None
) -> pd.DataFrame:
    """Get potential values for a dataset's parameter.

    Args:
        dataset: Dataset API to inspect. See :meth:`get_dataset_list` for a
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
    results = _get(params, api_key=api_key)
    return pd.DataFrame(results)


@ratelimit.guard(
    [
        ratelimit.RequestLimit(90, timedelta(minutes=1)),
        ratelimit.ErrorLimit(20, timedelta(minutes=1)),
        ratelimit.SizeLimit(90e6, timedelta(minutes=1)),
    ],
)
def _guarded_get(url: str, params: dict[str, Any], /) -> requests.Response:
    """Guarded version of `session.get`."""
    return session.get(url, params=params)


def get_dataset_list(*, api_key: None | str = None) -> pd.DataFrame:
    """Return a list of datasets provided by the BEA API.

    Returns:
        A dataframe describing the datasets available through the BEA API.

    Examples:
        >>> finagg.bea.api.get_dataset_list()
                        DatasetName                    DatasetDescription
        0                      NIPA                  Standard NIPA tables
        1        NIUnderlyingDetail  Standard NI underlying detail tables
        2                       MNE             Multinational Enterprises
        3               FixedAssets          Standard Fixed Assets tables
        4                       ITA   International Transactions Accounts
        5                       IIP     International Investment Position
        6               InputOutput                     Input-Output Data
        7             IntlServTrade          International Services Trade
        8             GDPbyIndustry                       GDP by Industry
        9                  Regional                    Regional data sets
        10  UnderlyingGDPbyIndustry            Underlying GDP by Industry
        11       APIDatasetMetaData     Metadata about other API datasets

    """
    params = {
        "Method": "GetDatasetList",
    }
    results = _get(params, api_key=api_key)["Dataset"]
    return pd.DataFrame(results)


class BEAAPIError(requests.RequestException):
    """An error raised in response to BEA API request errors."""
