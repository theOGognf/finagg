"""The "fred/category" API.

See the official FRED API docs for more info:

    https://fred.stlouisfed.org/docs/api/fred/

"""

import pandas as pd

from . import _api


class Children(_api.API):
    """Get FRED child categories.

    The class variable :data:`finagg.fred.api.category.children` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    url = "https://api.stlouisfed.org/fred/category/children"

    @classmethod
    def get(
        cls,
        category_id: int = 0,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get all child categories for a specific parent category.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/category_children.html

        Args:
            category_id: The category's ID. Use the
                "category/children" API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data for a category's children.

        Examples:
            >>> finagg.fred.api.category.children.get()  # doctest: NORMALIZE_WHITESPACE
                  id                                     name  parent_id
            0  32991                Money, Banking, & Finance          0
            1     10  Population, Employment, & Labor Markets          0
            2  32992                        National Accounts          0
            3      1           Production & Business Activity          0
            4  32455                                   Prices          0
            5  32263                       International Data          0
            6   3008                       U.S. Regional Data          0
            7  33060                            Academic Data          0

        """
        data = _api.get(
            cls.url,
            category_id=category_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["categories"]
        return pd.DataFrame(data)


class Related(_api.API):
    """Get FRED related categories.

    The class variable :data:`finagg.fred.api.category.related` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    url = "https://api.stlouisfed.org/fred/category/related"

    @classmethod
    def get(
        cls,
        category_id: int,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get categories related to a category.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/category_related.html

        Args:
            category_id: The category's ID. Use the
                "category/children" API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data for categories
            related to the given category.

        """
        data = _api.get(
            cls.url,
            category_id=category_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        print(data)
        data = data["categories"]
        return pd.DataFrame(data)


class Series(_api.API):
    """Get FRED series within a category.

    The class variable :data:`finagg.fred.api.category.series` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    url = "https://api.stlouisfed.org/fred/category/series"

    @classmethod
    def get(
        cls,
        category_id: int,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str | list[str] = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get series within a category.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/category_series.html

        Args:
            category_id: The category's ID. Use the
                "category/children" API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by. Options include:

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
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data for a category's series
            according to the given parameters.

        Examples:
            >>> finagg.fred.api.category.series.get(33951)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                      id realtime_start realtime_end                                              title ...
            0   FFHTHIGH     2023-03-15   2023-03-15  High Value of the Federal Funds Rate for the I... ...
            1    FFHTLOW     2023-03-15   2023-03-15  Low Value of the Federal Funds Rate for the In... ...
            2  FFWSJHIGH     2023-03-15   2023-03-15  High Value of the Federal Funds Rate for the I... ...
            3   FFWSJLOW     2023-03-15   2023-03-15  Low Value of the Federal Funds Rate for the In... ...

        """
        data = _api.get(
            cls.url,
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
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class Tags(_api.API):
    """Get FRED category's tags.

    The class variable :data:`finagg.fred.api.category.tags` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    url = "https://api.stlouisfed.org/fred/category/tags"

    @classmethod
    def get(
        cls,
        category_id: int,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        tag_names: None | str | list[str] = None,
        tag_group_id: None | str = None,
        search_text: None | str | list[str] = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get a FRED category's tags.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/category_tags.html

        Args:
            category_id: The category's ID. Use the
                "category/children" API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            tag_names: Filtering of tag names to include in the results.
            tag_group_id: A tag group ID to filter tags by. Options include:

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
            order_by: Variable to order results by. Options include:

                - "series_count"
                - "popularity"
                - "created"
                - "name"
                - "group_id"

            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data for a category's tags
            according to the given parameters.

        Examples:
            >>> finagg.fred.api.category.tags.get(33951, limit=5)  # doctest: +NORMALIZE_WHITESPACE
                        name group_id notes                 created  popularity  series_count
            0   anbil, sriya      src        2020-07-03 11:52:33-05          16             8
            1  carlson, mark      src        2020-07-03 11:53:20-05          16             8
            2          daily     freq        2012-02-27 10:18:19-06          71             8
            3        federal      gen        2012-02-27 10:18:19-06          60             8
            4          funds      gen  None  2020-05-11 13:13:02-05          28             8

        """
        data = _api.get(
            cls.url,
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
        ).json()
        data = data["tags"]
        return pd.DataFrame(data)


class RelatedTags(_api.API):
    """Get FRED category's related tags.

    The class variable :data:`finagg.fred.api.category.related_tags` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    url = "https://api.stlouisfed.org/fred/category/related_tags"

    @classmethod
    def get(
        cls,
        category_id: int,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str | list[str] = None,
        tag_group_id: None | str = None,
        search_text: None | str | list[str] = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get data for tags related to a category.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/category_related_tags.html

        Args:
            category_id: The category's ID. Use the
                "category/children" API to explore categories.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            tag_names: Find tags related to these tags.
            exclude_tag_names: Exclude tags related to these tags.
            tag_group_id: A tag group ID to filter tags by. Options include:

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
            order_by: Variable to order results by. Options include:

                - "series_count"
                - "popularity"
                - "created"
                - "name"
                - "group_id"

            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data for tags related to a category
            according to the given parameters.

        Examples:
            >>> finagg.fred.api.category.related_tags.get(33951, tag_names="funds")  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                             name group_id                     notes ...
            0                        anbil, sriya      src                           ...
            1                       carlson, mark      src                           ...
            2                               daily     freq                           ...
            3                             federal      gen                           ...
            4                  hanes, christopher      src                           ...
            5                            interest      gen                           ...
            6                       interest rate      gen                           ...
            7                              nation     geot                           ...
            8                                 nsa     seas   Not Seasonally Adjusted ...
            9   public domain: citation requested       cc                      None ...
            10                               rate      gen                           ...
            11                                usa      geo  United States of America ...
            12                  wheelock, david c      src                           ...
            13                                wsj      rls       Wall Street Journal ...

        """
        data = _api.get(
            cls.url,
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
        ).json()
        data = data["tags"]
        return pd.DataFrame(data)


class Category(_api.API):
    """Collection of `fred/category` APIs.

    The class variable :data:`finagg.fred.api.category` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    children = Children()
    """"category/children" FRED API. Get the children of a category.
    The most popular way for accessing the :class:`Children` API.

    :meta hide-value:
    """

    related = Related()
    """"category/related" FRED API. Get categories related to a category.
    The most popular way for accessing the :class:`Related` API.

    :meta hide-value:
    """

    related_tags = RelatedTags()
    """"category/related_tags" FRED API. Get tags related to a category.
    The most popular way for accessing the :class:`RelatedTags` API.

    :meta hide-value:
    """

    series = Series()
    """"category/series" FRED API. Get a category's series.
    The most popular way for accessing the :class:`Series` API.

    :meta hide-value:
    """

    tags = Tags()
    """"category/tags" FRED API. Get a category's tags.
    The most popular way for accessing the :class:`Tags` API.

    :meta hide-value:
    """

    url = "https://api.stlouisfed.org/fred/category"

    @classmethod
    def get(cls, category_id: int = 0, *, api_key: None | str = None) -> pd.DataFrame:
        """Get a category's details.

        Args:
            category_id: The category's ID. Use the
                "category/children" API to explore categories.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            Dataframe of category details.

        Examples:
            >>> finagg.fred.api.category.get()  # doctest: +NORMALIZE_WHITESPACE
               id        name  parent_id
            0   0  Categories          0

        """
        data = _api.get(cls.url, category_id=category_id, api_key=api_key).json()
        data = data["categories"]
        return pd.DataFrame(data)


category = Category()
"""The most popular way for accessing the :class:`Category`.

:meta hide-value:
"""
