from typing import ClassVar

import pandas as pd

from ._api import Dataset, get, pformat


class _Children(Dataset):
    endpoint: ClassVar[str] = "category/children"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(category_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["categories"]
        return pd.DataFrame(data)


class _Related(Dataset):

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "category/related"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(category_id, realtime_start, realtime_end, api_key)
        data = get(cls.url, params).json()
        data = data["categories"]
        return pd.DataFrame(data)


class _Series(Dataset):
    endpoint: ClassVar[str] = "category/series"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
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
            category_id,
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
    endpoint: ClassVar[str] = "category/tags"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_id",
        sort_order: None | str = "asc",
        tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            category_id,
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
    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "category/related_tags"

    @classmethod
    def get(
        cls,
        category_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_id",
        sort_order: None | str = "asc",
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        search_text: None | str = None,
        tag_group_id: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get tags related to a category.

        Args:
            category_id:
            realtime_start:
            realtime_end:
            limit:
            offset:
            order_by:
            sort_order:
            tag_names:
            exclude_tag_names:
            search_text:
            tag_group_id:
            api_key: Optional FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        """
        params = pformat(
            category_id,
            realtime_start,
            realtime_end,
            limit,
            offset,
            order_by,
            sort_order,
            tag_names,
            exclude_tag_names,
            search_text,
            tag_group_id,
            api_key,
        )
        data = get(cls.url, params).json()
        data = data["tags"]
        return pd.DataFrame(data)


class _Category(Dataset):
    """Collection of `fred/category` APIs.

    See the related FRED API documentation at:
        https://fred.stlouisfed.org/docs/api/fred/category.html

    """

    #: Get the children of a category.
    children: ClassVar[type[_Children]] = _Children

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "category"

    #: Get categories related to a category.
    related: ClassVar[type[_Related]] = _Related

    #: Get tags related to a category.
    related_tags: ClassVar[type[_RelatedTags]] = _RelatedTags

    #: Get a category's series.
    series: ClassVar[type[_Series]] = _Series

    #: Get a category's tags.
    tags: ClassVar[type[_Tags]] = _Tags

    @classmethod
    def get(cls, category_id: int = 0, *, api_key: None | str = None) -> pd.DataFrame:
        """Get a category's details.

        Args:
            category_id: The category's ID. Use the
                `category/children` API to explore categories.
            api_key: Optional FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            Dataframe of category details.

        """
        params = pformat(category_id, api_key)
        data = get(cls.url, params).json()
        data = data["categories"]
        return pd.DataFrame(data)


#: Public-facing fred/category API.
category = _Category
