"""The "fred/tags" APIs.

See the official FRED API docs for more info:
    https://fred.stlouisfed.org/docs/api/fred/

Examples:
    List tags related to an economic data series.
    >>> import finagg.fred.api as fred
    >>> fred.tags.series.get()

"""

from functools import cache

import pandas as pd

from . import _api


class _RelatedTags(_api.API):
    """Get related FRED tags."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/related_tags"

    @classmethod
    def get(
        cls,
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
        """Get data for tags related to an economic release.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/related_tags.html

        Args:
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
            A dataframe containing data for related FRED tags.

        """
        data = _api.get(
            cls.url,
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


class _Series(_api.API):
    """Get the economic data series matching tags."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/tags/series"

    @classmethod
    def get(
        cls,
        *,
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str | list[str] = None,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the economic data series matching tags.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/tags_series.html

        Args:
            tag_names: Find series that match these tags.
            exclude_tag_names: Exclude series that match none of these tags.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by.
                Options include:
                    - "series_id"
                    - "title"
                    - "units"
                    - "frequency"
                    - "seasonal_adjustment"
                    - "realtime_start"
                    - "realtime_end"
                    - "last_updated"
                    - "observation_start"
                    - "observation_end"
                    - "popularity"
                    - "group_popularity"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing series data for related tags.

        """
        data = _api.get(
            cls.url,
            tag_names=tag_names,
            exclude_tag_names=exclude_tag_names,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _Tags(_api.API):
    """Get FRED tags."""

    #: "tags/series" FRED API. Get the series for a FRED tag.
    series = _Series()

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/tags"

    @classmethod
    @cache
    def get(
        cls,
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
        """Get the FRED tags for a series.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/tags.html

        Args:
            release_id: The ID for a release.
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
                    - "cc" = citation and copyright
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
            A dataframe containing data for all FRED economic data tags.

        """
        data = _api.get(
            cls.url,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            tag_names=tag_names,
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


#: Public-facing "fred/tags" and "fred/related_tags" APIs.
tags = _Tags()
related_tags = _RelatedTags()
