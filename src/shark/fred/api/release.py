from typing import ClassVar

import pandas as pd

from ._api import Dataset, get


class _Dates(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release/dates"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        limit: int = 10000,
        offset: int = 0,
        sort_order: str = "asc",
        include_release_dates_with_no_data: bool = False,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "limit": limit,
            "offset": offset,
            "sort_order": sort_order,
            "include_release_dates_with_no_data": include_release_dates_with_no_data,
        }
        params["include_release_dates_with_no_data"] = (
            "true" if params["include_release_dates_with_no_data"] else "false"
        )
        response = get(cls.url, params, api_key=api_key)
        return response


class _Series(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release/series"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        limit: int = 1000,
        offset: int = 0,
        order_by: str = "series_id",
        sort_order: str = "asc",
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "sort_order": sort_order,
            "filter_variable": filter_variable,
            "fitler_value": filter_value,
            "tag_names": tag_names,
            "exclude_tag_names": exclude_tag_names,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Sources(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release/sources"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Tags(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release/tags"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        limit: int = 1000,
        offset: int = 0,
        order_by: str = "series_count",
        sort_order: str = "asc",
        tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "sort_order": sort_order,
            "tag_names": tag_names,
            "tag_group_id": tag_group_id,
            "search_text": search_text,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _RelatedTags(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release/related_tags"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        limit: int = 1000,
        offset: int = 0,
        order_by: str = "series_id",
        sort_order: str = "asc",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "tag_names": tag_names,
            "exclude_tag_names": exclude_tag_names,
            "tag_group_id": tag_group_id,
            "search_text": search_text,
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "sort_order": sort_order,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Tables(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release/tables"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        element_id: int = 0,
        include_observation_values: bool = False,
        observation_date: str = "9999-12-31",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "element_id": element_id,
            "include_observation_values": include_observation_values,
            "observation_date": observation_date,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Release(Dataset):
    """Collection of `fred/release` APIs."""

    dates: ClassVar[type[_Dates]] = _Dates

    related_tags: ClassVar[type[_RelatedTags]] = _RelatedTags

    series: ClassVar[type[_Series]] = _Series

    sources: ClassVar[type[_Sources]] = _Sources

    tables: ClassVar[type[_Tables]] = _Tables

    tags: ClassVar[type[_Tags]] = _Tags

    url: ClassVar[str] = "https://api.stlouisfed.org/fred/release"

    @classmethod
    def get(
        cls,
        release_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = {
            "release_id": release_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


release = _Release
