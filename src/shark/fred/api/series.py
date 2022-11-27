"""The "fred/series" API.

See the official FRED API docs for more info:
    https://fred.stlouisfed.org/docs/api/fred/

"""

from functools import cache

import pandas as pd

from ._api import Dataset, get


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
        sort_order: None | str = None,
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
        df = pd.DataFrame(data)
        df["series_id"] = series_id
        return df


class _Release(Dataset):
    """Get the release for an economic data series."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/release"

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
    """Get the related tags for a series search."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/search/related_tags"

    @classmethod
    def get(
        cls,
        series_search_text: None | str | list[str],
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str | list[str] = None,
        tag_group_id: None | str = None,
        tag_search_text: None | str | list[str] = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the related tags for a series search.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_search_tags.html

        Args:
            series_search_text: The words to match against economic data series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            tag_names: Tag names to only include in the response.
            exclude_tag_names: Tag names that series match none of.
            tag_group_id: A tag group ID to filter tags by type.
                Options include:
                    - "freq" = frequency
                    - "gen" = general or concept
                    - "geo" = geography
                    - "geot" = geography type
                    - "rls" = release
                    - "seas" = seasonal adjustment
                    - "src" = source
            tag_search_text: The words to find matching tags with.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Order results by values of the specified attribute.
                Options include:
                    - "series_count"
                    - "popularity"
                    - "created"
                    - "name"
                    - "group_id"
            sort_order: Sort results in ascending ("asc") or descending ("desc")
                order for the attribute values specified by `order_by`.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing related FRED tags for a series search.
            The dataframe can have results optionally filtered by the FRED
            servers according to the method's args.

        """
        data = get(
            cls.url,
            series_search_text=series_search_text,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            tag_names=tag_names,
            exclude_tag_names=exclude_tag_names,
            tag_group_id=tag_group_id,
            tag_search_text=tag_search_text,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _SearchTags(Dataset):
    """Get the tags for a series search."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/search/tags"

    @classmethod
    def get(
        cls,
        series_search_text: None | str | list[str],
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        tag_names: None | str | list[str] = None,
        tag_group_id: None | str = None,
        tag_search_text: None | str | list[str] = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the tags for a series search.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_search_tags.html

        Args:
            series_search_text: The words to match against economic data series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            tag_names: Tag names to only include in the response.
            tag_group_id: A tag group ID to filter tags by type.
                Options include:
                    - "freq" = frequency
                    - "gen" = general or concept
                    - "geo" = geography
                    - "geot" = geography type
                    - "rls" = release
                    - "seas" = seasonal adjustment
                    - "src" = source
            tag_search_text: The words to find matching tags with.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Order results by values of the specified attribute.
                Options include:
                    - "series_count"
                    - "popularity"
                    - "created"
                    - "name"
                    - "group_id"
            sort_order: Sort results in ascending ("asc") or descending ("desc")
                order for the attribute values specified by `order_by`.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing FRED tags for a series search.
            The dataframe can have results optionally filtered by the FRED
            servers according to the method's args.

        """
        data = get(
            cls.url,
            series_search_text=series_search_text,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            tag_names=tag_names,
            tag_group_id=tag_group_id,
            tag_search_text=tag_search_text,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _Search(Dataset):
    """Get economic data series that match search text."""

    #: "series/search/related_tags" FRED API. Get the related tags for a
    #: series search.
    related_tags = _SearchRelatedTags

    #: "series/search/tags" FRED API. Get the tags for a series search.
    tags = _SearchTags

    #: FRED API URL.
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
        sort_order: None | str = None,
        filter_variable: None | str = None,
        filter_value: None | str = None,
        tag_names: None | str | list[str] = None,
        exclude_tag_names: None | str | list[str] = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get economic data series that match search text.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_search.html

        Args:
            search_text: The words to match against economic data series.
            search_type: Determines the type of search to perform.
                Options include:
                    - "full_text" = search series attributes, units, frequency,
                        and tags by parsing words into stems.
                    - "series_id" = performs a substring search on series IDs.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Order results by values of the specified attribute.
                Options include:
                    - "search_rank"
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
            sort_order: Sort results in ascending ("asc") or descending ("desc")
                order for the attribute values specified by `order_by`.
            filter_variable: The attribute to filter results by.
                Options include:
                    - "frequency"
                    - "units"
                    - "seasonal_adjustment"
            filter_value: The value of the `filter_variable` attribute to filter
                results by.
            tag_names: List of tag names that series match all of.
            exclude_tag_names: List of tag names that series match none of.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on series matching the search.

        """
        data = get(
            cls.url,
            search_text=search_text,
            search_type=search_type,
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


class _Tags(Dataset):
    """Get FRED tags for a series."""

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/tags"

    @classmethod
    @cache
    def get(
        cls,
        series_id: str,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the FRED tags for a series.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_tags.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            order_by: Order results by values of the specified attribute.
                Options include:
                    - "series_count"
                    - "popularity"
                    - "created"
                    - "name"
                    - "group_id"
            sort_order: Sort results in ascending ("asc") or descending ("desc")
                order for the attribute values specified by `order_by`.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on FRED tags for series.

        """
        data = get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _Updates(Dataset):
    """Get economic data series sorted by when observations
    were updated on the FRED server.

    """

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/updates"

    @classmethod
    @cache
    def get(
        cls,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        filter_value: None | str = None,
        start_time: None | str = None,
        end_time: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get economic data series sorted by when observations
        were updated on the FRED server.

        Results are limited to series updated within the last two
        weeks.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_updates.html

        Args:
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            filter_value: Limit results by geographic type of economic data
                series.
                Options include:
                    - "macro" = limit results to macroeconomic data series
                    - "regional" = limit results to series for parts of the US
                    - "all" = does not filter results
            start_time: Start time for limiting results for a time range.
                Can filter down to minutes. Expects format "YYYMMDDHhmm".
            end_time: Start time for limiting results for a time range.
                Can filter down to minutes. Expects format "YYYMMDDHhmm".
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing info on recently updated economic
            data series.

        """
        data = get(
            cls.url,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            filter_value=filter_value,
            start_time=start_time,
            end_time=end_time,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _VintageDates(Dataset):
    """Get the dates in history when a series' data values were revised
    or new data values were released.

    """

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series/vintage_dates"

    @classmethod
    @cache
    def get(
        cls,
        series_id: str,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 10000,
        offset: None | int = 0,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get the dates in history when a series' data values were revised
        or new data values were released.

        Vintage dates are the release dates for a series excluding release dates
        when the data for the series did not change.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            sort_order: Sort results in ascending ("asc") or descending ("desc")
                `vintage_date` order.

        Returns:
            A dataframe containing dates on vintage release dates for a series.

        """
        data = get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


class _Series(Dataset):
    """Get an economic data series."""

    #: "series/categories" FRED API. Get the categories for
    #: an economic data series.
    categories = _Categories

    #: "series/observations" FRED API. Get the observations or
    #: data values for an economic data series.
    observations = _Observations

    #: "series/release" FRED API. Get the release for an economic data series.
    release = _Release

    #: "series/search" FRED API. Get economic data series that match search text.
    search = _Search

    #: "series/tags" FRED API. Get FRED tags for a series.
    tags = _Tags

    #: "series/updates" FRED API. Get economic data series sorted by
    #: when observations were updated on the FRED server.
    updates = _Updates

    #: FRED API URL.
    url = "https://api.stlouisfed.org/fred/series"

    #: "series/vintage_dates" FRED API. Get the dates in history when a
    #: a series' data values were revised or new data values were released.
    vintage_dates = _VintageDates

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
        """Get an economic data series.

        See the related FRED API documentation at:
            https://fred.stlouisfed.org/docs/api/fred/series_updates.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing info on an economic data series.

        """
        data = get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


#: Public-facing "fred/series" API.
series = _Series
