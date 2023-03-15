"""The "fred/tags" APIs.

See the official FRED API docs for more info:

    https://fred.stlouisfed.org/docs/api/fred/

"""

import pandas as pd

from . import _api


class RelatedTags(_api.API):
    """Get FRED tags related to other FRED tags.

    The module variable :data:`related_tags` is an instance of this API
    implementation and is the most popular interface for calling
    this API.

    """

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
            A dataframe containing data for related FRED tags.

        Examples:
            >>> finagg.fred.api.related_tags.get(tag_names="bea", limit=5)  # doctest: +NORMALIZE_WHITESPACE
                                            name group_id                     notes                 created  popularity  series_count
            0  public domain: citation requested       cc                      None  2018-12-17 23:33:13-06          99         78680
            1                                usa      geo  United States of America  2012-02-27 10:18:19-06         100         78434
            2                                nsa     seas   Not Seasonally Adjusted  2012-02-27 10:18:19-06          99         67720
            3                             annual     freq                            2012-02-27 10:18:19-06          88         66478
            4                                gdp      gen    Gross Domestic Product  2012-02-27 10:18:19-06          81         60040

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


class Series(_api.API):
    """Get FRED series related to FRED tags.

    The class variable :data:`finagg.fred.api.tags.series` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

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
            order_by: Variable to order results by. Options include:

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
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing series data for related tags.

        Examples:
            >>> finagg.fred.api.tags.series.get(tag_names="bea", limit=5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
                            id realtime_start realtime_end                                             title ...
            0  A001RD3A086NBEA     2023-03-15   2023-03-15  Gross national product (implicit price deflator) ...
            1  A001RG3A086NBEA     2023-03-15   2023-03-15   Gross national product (chain-type price index) ...
            2  A001RI1A225NBEA     2023-03-15   2023-03-15   Gross National Product: Implicit Price Deflator ...
            3  A001RI1Q225SBEA     2023-03-15   2023-03-15   Gross National Product: Implicit Price Deflator ...
            4  A001RL1A225NBEA     2023-03-15   2023-03-15                       Real Gross National Product ...

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


class Tags(_api.API):
    """Get FRED tags.

    The module variable :data:`tags` is an instance of this API
    implementation and is the most popular interface for calling
    this API.

    """

    series = Series()
    """"tags/series" FRED API. Get the series for a FRED tag.
    The most popular way for accessing the :class:`Series` API.

    :meta hide-value:
    """

    url = "https://api.stlouisfed.org/fred/tags"

    @classmethod
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
                - "cc" = citation and copyright

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
            A dataframe containing data for all FRED economic data tags.

        Examples:
            >>> finagg.fred.api.tags.get(tag_group_id="src", limit=5)  # doctest: +NORMALIZE_WHITESPACE
                      name group_id                        notes                 created  popularity  series_count
            0       census      src                       Census  2012-02-27 10:18:19-06          79        237692
            1          bls      src   Bureau of Labor Statistics  2012-02-27 10:18:19-06          89        175376
            2  realtor.com      src                               2020-03-24 11:15:04-05          66         90632
            3          bea      src  Bureau of Economic Analysis  2012-02-27 10:18:19-06          78         78842
            4      frb stl      src                St. Louis Fed  2012-02-27 10:18:19-06          68         78442

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


tags = Tags()
"""The most popular way for accessing :class:`Tags`.

:meta hide-value:
"""

related_tags = RelatedTags()
"""The most popular way for accessing :class:`RelatedTags`.

:meta hide-value:
"""
