from typing import ClassVar

import pandas as pd

from ._api import Dataset, get, pformat


class _Categories(Dataset):
    endpoint: ClassVar[str] = "series/categories"

    @classmethod
    def get(
        cls,
        series_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(series_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["categories"]
        return pd.DataFrame(data)


class _Observations(Dataset):
    endpoint: ClassVar[str] = "series/observations"

    @classmethod
    def get(
        cls,
        series_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 100000,
        offset: None | int = 0,
        sort_order: None | str = "asc",
        observation_start: None | str = None,
        observation_end: None | str = None,
        units: None | str = "lin",
        frequency: None | str = None,
        aggregation_method: None | str = "avg",
        output_type: None | int = 1,
        vintage_dates: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            series_id,
            realtime_start,
            realtime_end,
            limit,
            offset,
            sort_order,
            observation_start,
            observation_end,
            units,
            frequency,
            aggregation_method,
            output_type,
            vintage_dates,
            api_key,
        )
        data = get(cls.url, params).json()
        data = data["observations"]
        return pd.DataFrame(data)


class _Release(Dataset):
    endpoint: ClassVar[str] = "series/release"

    @classmethod
    def get(
        cls,
        series_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(series_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["releases"]
        return pd.DataFrame(data)


class _SearchRelatedTags(Dataset):
    ...


class _SearchTags(Dataset):
    ...


class _Search(Dataset):
    endpoint: ClassVar[str] = "series/search"

    @classmethod
    def get(
        cls,
        search_text: str | list[str],
        /,
        *,
        search_type: None | str = "full_text",
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = "asc",
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        if isinstance(search_text, str):
            search_text = [search_text]
        search_text = "+".join(search_text)
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

    categories: ClassVar[type[_Categories]] = _Categories

    endpoint: ClassVar[str] = "series"

    release: ClassVar[type[_Release]] = _Release

    search: ClassVar[type[_Search]] = _Search

    @classmethod
    def get(
        cls,
        series_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(series_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["seriess"]
        return pd.DataFrame(data)


series = _Series
