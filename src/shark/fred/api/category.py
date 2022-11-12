from typing import ClassVar

import pandas as pd

from ._api import Dataset, get, pformat


class _Children(Dataset):
    """Get all child categories for a specific parent category."""

    #: FRED API endpoint name.
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
        category_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        params = pformat(
            category_id=category_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        )
        data = get(cls.url, params).json()
        data = data["categories"]
        return pd.DataFrame(data)


class _Series(Dataset):
    """Get data for series within a category."""

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "category/series"

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
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str = None,
        exclude_tag_names: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get series within a category.

        Args:
            category_id: The category's ID. Use the
                `category/children` API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by.
                Options include:
                    - "series_count"
                    - "popularity"
                    - "created"
                    - "name"
                    - "group_id"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            filter_variable: The attribute (or column) to filter results by.
                Options include:
                    - "frequency"
                    - "units"
                    - "seasonal_adjustment"
            filter_value: The value of `filter_variable` to filter results
                by.
            tag_names: Find tags related to these tags.
            exclude_tag_names: Exclude tags related to these tags.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data for a category's series
            according to the given parameters.

        """
        params = pformat(
            category_id=category_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            filter_variable=filter_variable,
            filter_value=filter_value,
            tag_names=tag_names,
            exclude_tag_names=exclude_tag_names,
            api_key=api_key,
        )
        data = get(cls.url, params).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _Tags(Dataset):
    """Get a category's tags."""

    #: FRED API endpoint name.
    endpoint: ClassVar[str] = "category/tags"

    @classmethod
    def get(
        cls,
        category_id: int,
        /,
        *,
        realtime_start: None | str = None,
        realtime_end: None | str = None,
        tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_count",
        sort_order: None | str = "asc",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get data for a category's tags.

        Args:
            category_id: The category's ID. Use the
                `category/children` API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            tag_names: Filtering of tag names to include in the results.
            tag_group_id: A tag group ID to filter tags by.
                Options include:
                    - "freq" = frequency
                    - "gen" = general or concept
                    - "geo" = geography
                    - "geot" = geography type
                    - "rls" = release
                    - "seas" = seasonal adjustment
                    - "src" = source
            search_text: The words to find matching tags with.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by.
                Options include:
                    - "series_count"
                    - "popularity"
                    - "created"
                    - "name"
                    - "group_id"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data for a category's tags
            according to the given parameters.

        """
        params = pformat(
            category_id=category_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            tag_names=tag_names,
            tag_group_id=tag_group_id,
            search_text=search_text,
            api_key=api_key,
        )
        data = get(cls.url, params).json()
        data = data["tags"]
        return pd.DataFrame(data)


class _RelatedTags(Dataset):
    """Get data for tags related to a category."""

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
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str = None,
        tag_group_id: None | str = None,
        search_text: None | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = "series_count",
        sort_order: None | str = "asc",
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get data for tags related to a category.

        Args:
            category_id: The category's ID. Use the
                `category/children` API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            tag_names: Find tags related to these tags.
            exclude_tag_names: Exclude tags related to these tags.
            tag_group_id: A tag group ID to filter tags by.
                Options include:
                    - "freq" = frequency
                    - "gen" = general or concept
                    - "geo" = geography
                    - "geot" = geography type
                    - "rls" = release
                    - "seas" = seasonal adjustment
                    - "src" = source
            search_text: The words to find matching tags with.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by.
                Options include:
                    - "series_count"
                    - "popularity"
                    - "created"
                    - "name"
                    - "group_id"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data for tags related to a category
            according to the given parameters.

        """
        params = pformat(
            category_id=category_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            tag_names=tag_names,
            exclude_tag_names=exclude_tag_names,
            tag_group_id=tag_group_id,
            search_text=search_text,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
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
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            Dataframe of category details.

        """
        params = pformat(category_id=category_id, api_key=api_key)
        data = get(cls.url, params).json()
        data = data["categories"]
        return pd.DataFrame(data)


#: Public-facing fred/category API.
category = _Category
