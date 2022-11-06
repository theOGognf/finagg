from typing import ClassVar

import pandas as pd

from ._api import Dataset, get


class _Children(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category/children"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "category_id": category_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Related(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category/related"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "category_id": category_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Series(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category/series"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
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
        api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "category_id": category_id,
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


class _Tags(Dataset):
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category/tags"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        limit: int = 1000,
        offset: int = 0,
        order_by: str = "series_id",
        sort_order: str = "asc",
        tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "category_id": category_id,
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
    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category/related_tags"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: str = "9999-12-31",
        realtime_end: str = "9999-12-31",
        limit: int = 1000,
        offset: int = 0,
        order_by: str = "series_id",
        sort_order: str = "asc",
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        search_text: None | str = None,
        tag_group_id: None | str = None,
        api_key: None | str = None
    ) -> pd.DataFrame:
        params = {
            "category_id": category_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "sort_order": sort_order,
            "tag_names": tag_names,
            "exclude_tag_names": exclude_tag_names,
            "search_text": search_text,
            "tag_group_id": tag_group_id,
        }
        response = get(cls.url, params, api_key=api_key)
        return response


class _Category(Dataset):
    """Collection of `fred/category` APIs."""

    children: ClassVar[type[_Children]] = _Children

    related: ClassVar[type[_Related]] = _Related

    related_tags: ClassVar[type[_RelatedTags]] = _RelatedTags

    series: ClassVar[type[_Series]] = _Series

    tags: ClassVar[type[_Tags]] = _Tags

    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category"

    @classmethod
    def get(cls, category_id: int = 0, *, api_key: None | str = None) -> pd.DataFrame:
        params = {"category_id": category_id}
        response = get(cls.url, params, api_key=api_key)
        return response


category = _Category
