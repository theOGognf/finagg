"""The "fred/series" API.

This is by far the most popular FRED API implementation.
Useful for examining historical or projected economic data.
The API comes with builtin methods for filtering data based
on publication date or frequency. See the docs of each
method for more details.

See the official FRED API docs for more info:

    https://fred.stlouisfed.org/docs/api/fred/

"""

import pandas as pd

from . import _api


class Categories(_api.API):
    """Get the categories for an economic data series.

    The class variable :data:`finagg.fred.api.series.categories` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    url = "https://api.stlouisfed.org/fred/series/categories"

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
        """Get the categories for an economic data series.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/series_categories.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data on categories for the economic data series.

        Examples:
            >>> finagg.fred.api.series.categories.get("CPIAUCNS")  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
               id                                  name  parent_id
            0   9  Consumer Price Indexes (CPI and PCE)      32455

        """
        data = _api.get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["categories"]
        return pd.DataFrame(data)


class Observations(_api.API):
    """Get the observations or data values for an economic data series.

    This is by far the most popular FRED API method. The class variable
    :data:`finagg.fred.api.series.observations` is an instance of this
    API implementation and is the most popular interface for calling this API.

    """

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
                according to their publication date. ``0`` indicates since
                the beginning of time.
            realtime_end: End date for fetching results according
                to their publication date. ``-1`` indicates to present day.
            limit: Maximum number of results to return.
            offset: Result start offset.
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            observation_start: The start date of the observation period.
            observation_end: The end date of the observation period.
            units: Units to return the series values in. Options include:

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
                aggregate values to. Frequency options without period descriptions
                include:

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
                for frequency aggregation. Options include:

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
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing economic data series observations/values according to
            the given parameters.

        Examples:
            >>> finagg.fred.api.series.observations.get(
            ...     "CPIAUCNS",
            ...     realtime_start=0,
            ...     realtime_end=-1,
            ...     output_type=4
            ... ).head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
              realtime_start realtime_end        date  value series_id
            0     1949-04-22   1953-02-26  1949-03-01  169.5  CPIAUCNS
            1     1949-05-23   1953-02-26  1949-04-01  169.7  CPIAUCNS
            2     1949-06-24   1953-02-26  1949-05-01  169.2  CPIAUCNS
            3     1949-07-22   1953-02-26  1949-06-01  169.6  CPIAUCNS
            4     1949-08-26   1953-02-26  1949-07-01  168.5  CPIAUCNS

        """
        data = _api.get(
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


class Release(_api.API):
    """Get the latest release for an economic data seris.

    The class variable :data:`finagg.fred.api.series.release` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    url = "https://api.stlouisfed.org/fred/series/release"

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
        """Get the release for an economic data series.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/series_release.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data on a release for an economic data series.

        """
        data = _api.get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["releases"]
        return pd.DataFrame(data)


class SearchRelatedTags(_api.API):
    """Search for series tags related to a series's tags.

    The class variable :data:`finagg.fred.api.series.search.related_tags` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

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
            tag_group_id: A tag group ID to filter tags by type. Options include:

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
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing related FRED tags for a series search.
            The dataframe can have results optionally filtered by the FRED
            servers according to the method's args.

        Examples:
            >>> finagg.fred.api.series.search.related_tags.get("price index", tag_names="price", limit=5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                            name group_id                    notes                 created ...
            0                                nsa     seas  Not Seasonally Adjusted  2012-02-27 10:18:19-06 ...
            1                            indexes      gen                           2012-02-27 10:18:19-06 ...
            2                        price index      gen                           2012-02-27 10:18:19-06 ...
            3                            monthly     freq                           2012-02-27 10:18:19-06 ...
            4  public domain: citation requested       cc                     None  2018-12-17 23:33:13-06 ...

        """
        data = _api.get(
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
        data = data["tags"]
        return pd.DataFrame(data)


class SearchTags(_api.API):
    """Get FRED series tags.

    The class variable :data:`finagg.fred.api.series.search.tags` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

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
            tag_group_id: A tag group ID to filter tags by type. Options include:

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
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing FRED tags for a series search.
            The dataframe can have results optionally filtered by the FRED
            servers according to the method's args.

        Examples:
            >>> finagg.fred.api.series.search.tags.get("price index", limit=5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                            name group_id                    notes ...
            0                                nsa     seas  Not Seasonally Adjusted ...
            1                              price      gen                          ...
            2                            indexes      gen                          ...
            3                        price index      gen                          ...
            4  public domain: citation requested       cc                     None ...

        """
        data = _api.get(
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
        data = data["tags"]
        return pd.DataFrame(data)


class Search(_api.API):
    """Get economic data series that match search text.

    The class variable :data:`finagg.fred.api.series.search` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    related_tags = SearchRelatedTags()
    """"series/search/related_tags" FRED API. Get the related tags for a
    series search. The most popular way for accessing the :class:`SearchTags`
    API.

    :meta hide-value:
    """

    tags = SearchTags()
    """"series/search/tags" FRED API. Get the tags for a series search.
    The most popular way for accessing the :class:`SearchTags` API.

    :meta hide-value:
    """

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
            search_type: Determines the type of search to perform. Options include:

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
            filter_variable: The attribute to filter results by. Options include:

                - "frequency"
                - "units"
                - "seasonal_adjustment"

            filter_value: The value of the `filter_variable` attribute to filter
                results by.
            tag_names: List of tag names that series match all of.
            exclude_tag_names: List of tag names that series match none of.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data on series matching the search.

        Examples:
            >>> finagg.fred.api.series.search.get("price index", limit=5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                        id realtime_start realtime_end                                              title ...
            0     CPIAUCSL     2023-03-16   2023-03-16  Consumer Price Index for All Urban Consumers: ... ...
            1     CPIAUCNS     2023-03-16   2023-03-16  Consumer Price Index for All Urban Consumers: ... ...
            2  CUUS0000SA0     2023-03-16   2023-03-16  Consumer Price Index for All Urban Consumers: ... ...
            3   CSUSHPINSA     2023-03-16   2023-03-16    S&P/Case-Shiller U.S. National Home Price Index ...
            4    CSUSHPISA     2023-03-16   2023-03-16    S&P/Case-Shiller U.S. National Home Price Index ...

        """
        data = _api.get(
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


class Tags(_api.API):
    """Get FRED tags for an economic data series.

    The class variable :data:`finagg.fred.api.series.tags` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    url = "https://api.stlouisfed.org/fred/series/tags"

    @classmethod
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
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data on FRED tags for series.

        Examples:
            >>> finagg.fred.api.series.tags.get("CPIAUCNS").head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                                            name group_id                     notes ...
            0                                nsa     seas   Not Seasonally Adjusted ...
            1                                usa      geo  United States of America ...
            2  public domain: citation requested       cc                      None ...
            3                             nation     geot                           ...
            4                            monthly     freq                           ...

        """
        data = _api.get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["tags"]
        return pd.DataFrame(data)


class Updates(_api.API):
    """Get data on when economic data series where updated on the FRED server.

    The class variable :data:`finagg.fred.api.series.updates` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    url = "https://api.stlouisfed.org/fred/series/updates"

    @classmethod
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
                series. Options include:

                    - "macro" = limit results to macroeconomic data series
                    - "regional" = limit results to series for parts of the US
                    - "all" = does not filter results

            start_time: Start time for limiting results for a time range.
                Can filter down to minutes. Expects format "YYYMMDDHhmm".
            end_time: Start time for limiting results for a time range.
                Can filter down to minutes. Expects format "YYYMMDDHhmm".
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing info on recently updated economic
            data series.

        """
        data = _api.get(
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


class VintageDates(_api.API):
    """Get FRED series revision dates.

    The class variable :data:`finagg.fred.api.series.vintage_dates` is an
    instance of this API implementation and is the most popular interface for
    calling this API.

    """

    url = "https://api.stlouisfed.org/fred/series/vintage_dates"

    @classmethod
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
                vintage date order.

        Returns:
            A dataframe containing dates on vintage release dates for a series.

        """
        data = _api.get(
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


class Series(_api.API):
    """Get an economic data series.

    The class variable :data:`finagg.fred.api.series` is an instance of this
    API implementation and is the most popular interface for calling this API.

    """

    categories = Categories()
    """"series/categories" FRED API. Get the categories for an economic
    data series. The most popular way for accessing the :class:`Categories`
    API.

    :meta hide-value:
    """

    observations = Observations()
    """"series/observations" FRED API. Get the observations or
    data values for an economic data series. The most popular way
    for accessing the :class:`Observations` API.

    :meta hide-value:
    """

    release = Release()
    """"series/release" FRED API. Get the release for an economic data series.
    The most popular way for accessing the :class:`Release` API.

    :meta hide-value:
    """

    search = Search()
    """"series/search" FRED API. Get economic data series that match search
    text. The most popular way for accessing the :class:`Search` API.

    :meta hide-value:
    """

    tags = Tags()
    """"series/tags" FRED API. Get FRED tags for a series.
    The most popular way for accessing the :class:`Tags` API.

    :meta hide-value:
    """

    updates = Updates()
    """"series/updates" FRED API. Get economic data series sorted by
    when observations were updated on the FRED server. The most popular
    way for accessing the :class:`Updates` API.

    :meta hide-value:
    """

    url = "https://api.stlouisfed.org/fred/series"

    vintage_dates = VintageDates()
    """"series/vintage_dates" FRED API. Get the dates in history when a
    a series' data values were revised or new data values were released.
    The most popular way for accessing the :class:`VintageDates` API.

    :meta hide-value:
    """

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

            https://fred.stlouisfed.org/docs/api/fred/series.html

        Args:
            series_id: The ID for a series.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing info on an economic data series.

        Examples:
            >>> finagg.fred.api.series.get("CPIAUCNS", realtime_start=0, realtime_end=-1).head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                     id realtime_start realtime_end                                              title ...
            0  CPIAUCNS     1949-03-24   1953-02-26  Consumer Price Index for Urban Wage Earners an... ...
            1  CPIAUCNS     1953-02-27   1962-02-27  Consumer Price Index for Urban Wage Earners an... ...
            2  CPIAUCNS     1962-02-28   1971-02-18  Consumer Price Index for Urban Wage Earners an... ...
            3  CPIAUCNS     1971-02-19   1978-02-26  Consumer Price Index for Urban Wage Earners an... ...
            4  CPIAUCNS     1978-02-27   1988-02-25  Consumer Price Index for All Urban Consumers: ... ...

        """
        data = _api.get(
            cls.url,
            series_id=series_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["seriess"]
        return pd.DataFrame(data)


series = Series()
"""The most popular way for accessing :class:`Series`.

:meta hide-value:
"""
