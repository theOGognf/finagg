from typing import ClassVar

import pandas as pd

from ._api import Dataset, get, pformat


class _ReleasesDates(Dataset):
    endpoint: ClassVar[str] = "releases/dates"

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
        include_release_dates_with_no_data: None | bool = False,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            realtime_start,
            realtime_end,
            limit,
            offset,
            order_by,
            sort_order,
            include_release_dates_with_no_data,
            api_key,
        )
        params["include_release_dates_with_no_data"] = (
            "true" if params["include_release_dates_with_no_data"] else "false"
        )
        data = get(cls.url, params).json()
        data = data["releases"]
        return pd.DataFrame(data)


class _Releases(Dataset):

    dates: ClassVar[type[_ReleasesDates]] = _ReleasesDates

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
        params = pformat(
            realtime_start, realtime_end, limit, offset, order_by, sort_order, api_key
        )
        data = get(cls.url, params).json()
        data = data["releases"]
        return pd.DataFrame(data)


class _ReleaseDates(Dataset):
    endpoint: ClassVar[str] = "release/dates"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 10000,
        offset: None | int = 0,
        sort_order: None | str = "asc",
        include_release_dates_with_no_data: None | bool = False,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            release_id,
            realtime_start,
            realtime_end,
            limit,
            offset,
            sort_order,
            include_release_dates_with_no_data,
            api_key,
        )
        params["include_release_dates_with_no_data"] = (
            "true" if params["include_release_dates_with_no_data"] else "false"
        )
        data = get(cls.url, params).json()
        data = data["release_dates"]
        return pd.DataFrame(data)


class _Series(Dataset):
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
