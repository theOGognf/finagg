"""The "fred/series" API.

See the official FRED API docs for more info:
    https://fred.stlouisfed.org/docs/api/fred/

"""

from functools import cache

import pandas as pd

from ._api import Dataset, get, pformat


class _Categories(Dataset):
    """Get the categories for an economic data series."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/categories"

    @classmethod
    @cache
    def get(
        cls,
        series_id: str,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the categories for an economic data series.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_categories.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on categories for the economic data series.

        """
        data = get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["categories"]
        return pd.DataFrame(data)


class _Observations(Dataset):
    """Get the observations or data values for an economic data series."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/observations"

    @classmethod
    def get(
        cls,
        series_id: str,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 100000,
        offset: None | int = 0,
        sort_order: None | str = "asc",
        observation_start: None | int | str = None,
        observation_end: None | int | str = None,
        units: None | str = "lin",
        frequency: None | str = None,
        aggregation_method: None | str = "avg",
        output_type: None | int = 1,
        vintage_dates: None | str | list[str] = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the observations or data values for an economic data series.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_observations.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            observation_start: The start date of the observation period.
            observation_end: The end date of the observation period.
            units: Units to return the series values in.
                Options include:
                    - "lin" = levels (no unit transformation)
                    - "chg" = change
                    - "ch1" = change from a year ago
                    - "pch" = percent change
                    - "pc1" = percent change from a year ago
                    - "pca" = compounded annual rate of change
                    - "cch" = continuously compounded rate of change
                    - "cca" = continuously compounded annual rate of change
                    - "log" = natural log
            frequency: An optional parameter that indicates a lower frequency to
                aggregate values to.
                Frequency options without period descriptions include:
                    - "d" = daily
                    - "w" = weekly
                    - "bw" = biweekly
                    - "m" = monthly
                    - "q" = quarterly
                    - "sa" = semiannual
                    - "a" = annual
                Frequency options with period descriptions include:
                    - "wef" = weekly, ending Friday
                    - "weth" = weekly, ending Thursday
                    - "wetu" = weekly, ending Wednesday
                    - "wem" = weekly, ending Monday
                    - "wesu" = weekly, ending Sunday
                    - "wesa" = weekly, ending Saturday
                    - "bwew" = weekly, ending Wednesday
                    - "bwem" = weekly, Monday
            aggregation_method: A key that indicates the aggregation method used
                for frequency aggregation.
                Options include:
                    - "avg" = average
                    - "sum" = sum
                    - "eop" = end of period
            output_type: An integer indicating the type of observations to include.
                Options include:
                    - 1 = observations by realtime period
                    - 2 = all observations by vintage dates
                    - 3 = new and revised observations only
                    - 4 = initial release observations only
            vintage_dates: Vintage dates used to download data as it existed on these
                specified dates in history. Vintage dates can be specified instead of
                realtime periods.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing economic data series observations/values according to
            the given parameters.

        """
        data = get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            sort_order=sort_order,
            observation_start=observation_start,
            observation_end=observation_end,
            units=units,
            frequency=frequency,
            aggregation_method=aggregation_method,
            output_type=output_type,
            vintage_dates=vintage_dates,
            api_key=api_key,
        ).json()
        data = data["observations"]
        return pd.DataFrame(data)


class _Release(Dataset):
    """Get the release for an economic data series."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/release"

    @classmethod
    def get(
        cls,
        series_id: str,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the release for an economic data series.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_release.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on a release for an economic data series.

        """
        data = get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["releases"]
        return pd.DataFrame(data)


class _SearchRelatedTags(Dataset):
    ...


class _SearchTags(Dataset):
    ...


class _Search(Dataset):
    url = "https://api.stlouisfed.org/fred/series/search"

    @classmethod
    def get(
        cls,
        search_text: str | list[str],
        /,
        *,
        search_type: None | str = "full_text",
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = "asc",
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str | list[str] = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            search_text,
            search_type,
            realtime_start,
            realtime_end,
            limit,
            offset,
            order_by,
            sort_order,
            filter_variable,
            filter_value,
            tag_names,
            exclude_tag_names,
            api_key,
        )
        data = get(cls.url, params).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _Tags(Dataset):
    ...


class _Updates(Dataset):
    ...


class _VintageDates(Dataset):
    ...


class _Series(Dataset):

    categories = _Categories

    observations = _Observations

    release = _Release

    search = _Search

    url = "https://api.stlouisfed.org/fred/series"

    @classmethod
    def get(
        cls,
        series_id: str,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(series_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["seriess"]
        return pd.DataFrame(data)


#: Public-facing "fred/series" API.
series = _Series
