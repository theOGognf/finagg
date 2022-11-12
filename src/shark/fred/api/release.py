"""The "fred/release" and "fred/releases" API.

See the official FRED API docs for more info:
    https://fred.stlouisfed.org/docs/api/fred/

"""

from typing import ClassVar

import pandas as pd

from ._api import Dataset, get, pformat


class _ReleasesDates(Dataset):
    """Get release dates for all releases of economic data."""

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "releases/dates"

    @classmethod
    def get(
        cls,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "release_date",
        sort_order: None | str = "desc",
        include_release_dates_with_no_data: None | bool = False,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get all releases of economic data.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/releases_dates.html

        Args:
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by.
                Options include:
                    - "release_date"
                    - "release_id"
                    - "release_name"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            include_release_dates_with_no_data: Whether to return release
                dates that don't contain any data.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on release dates for all
            releases of economic data.

        """
        params = pformat(
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            include_release_dates_with_no_data=include_release_dates_with_no_data,
            api_key=api_key,
        )
        params["include_release_dates_with_no_data"] = (
            "true" if params["include_release_dates_with_no_data"] else "false"
        )
        data = get(cls.url, params).json()
        data = data["releases"]
        return pd.DataFrame(data)


class _Releases(Dataset):
    """Get all releases of economic data."""

    #: "releases/dates" FRED API. Get dates for releases of economic data.
    dates: ClassVar[type[_ReleasesDates]] = _ReleasesDates

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "releases"

    @classmethod
    def get(
        cls,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "release_id",
        sort_order: None | str = "asc",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get all releases of economic data.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/releases.html

        Args:
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by.
                Options include:
                    - "release_id"
                    - "name"
                    - "press_release"
                    - "realtime_start"
                    - "realtime_end"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on all releases of economic
            data.

        """
        params = pformat(
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        )
        data = get(cls.url, params).json()
        data = data["releases"]
        return pd.DataFrame(data)


class _ReleaseDates(Dataset):
    """Get data on release dates for a particular release of economic data."""

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "release/dates"

    @classmethod
    def get(
        cls,
        release_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 10000,
        offset: None | int = 0,
        sort_order: None | str = "asc",
        include_release_dates_with_no_data: None | bool = False,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get data on release dates for a particular release of economic data.

        Args:
            release_id: The ID for a release.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            include_release_dates_with_no_data: Whether to return release
                dates that don't contain any data.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data for an economic data release's release dates.

        """
        params = pformat(
            release_id=release_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            sort_order=sort_order,
            include_release_dates_with_no_data=include_release_dates_with_no_data,
            api_key=api_key,
        )
        params["include_release_dates_with_no_data"] = (
            "true" if params["include_release_dates_with_no_data"] else "false"
        )
        data = get(cls.url, params).json()
        data = data["release_dates"]
        return pd.DataFrame(data)


class _Series(Dataset):
    """"""

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "release/series"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_id",
        sort_order: None | str = "asc",
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            release_id,
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


class _Sources(Dataset):
    endpoint: ClassVar[str] = "release/sources"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(release_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["sources"]
        return pd.DataFrame(data)


class _Tags(Dataset):
    endpoint: ClassVar[str] = "release/tags"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_count",
        sort_order: None | str = "asc",
        tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            release_id,
            realtime_start,
            realtime_end,
            limit,
            offset,
            order_by,
            sort_order,
            tag_names,
            tag_group_id,
            search_text,
            api_key,
        )
        data = get(cls.url, params).json()
        data = data["tags"]
        return pd.DataFrame(data)


class _RelatedTags(Dataset):
    endpoint: ClassVar[str] = "release/related_tags"

    @classmethod
    def get(
        cls,
        release_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_id",
        sort_order: None | str = "asc",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            release_id,
            realtime_start,
            realtime_end,
            tag_names,
            exclude_tag_names,
            tag_group_id,
            search_text,
            limit,
            offset,
            order_by,
            sort_order,
            api_key,
        )
        data = get(cls.url, params).json()
        data = data["tags"]
        return pd.DataFrame(data)


class _Tables(Dataset):
    endpoint: ClassVar[str] = "release/tables"

    @classmethod
    def get(
        cls,
        release_id: int,
        /,
        *,
        element_id: None | int = 0,
        include_observation_values: None | bool = False,
        observation_date: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            release_id,
            element_id,
            include_observation_values,
            observation_date,
            api_key,
        )
        data = get(cls.url, params).json()
        data = data["tables"]
        return pd.DataFrame(data)


class _Release(Dataset):
    """Collection of `fred/release` APIs."""

    dates: ClassVar[type[_ReleaseDates]] = _ReleaseDates

    related_tags: ClassVar[type[_RelatedTags]] = _RelatedTags

    series: ClassVar[type[_Series]] = _Series

    sources: ClassVar[type[_Sources]] = _Sources

    tables: ClassVar[type[_Tables]] = _Tables

    tags: ClassVar[type[_Tags]] = _Tags

    endpoint: ClassVar[str] = "release"

    @classmethod
    def get(
        cls,
        release_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(release_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["releases"]
        return pd.DataFrame(data)


#: Public-facing fred/releases and fred/release APIs.
releases = _Releases
release = _Release
