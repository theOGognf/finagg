"""The "fred/source" and "fred/sources" APIs.

See the official FRED API docs for more info:

    https://fred.stlouisfed.org/docs/api/fred/

"""

import pandas as pd

from . import _api


class Releases(_api.API):
    """Get all of a source's releases of economic data.

    The class variable :data:`finagg.fred.api.source.releases` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    url = "https://api.stlouisfed.org/fred/source/releases"

    @classmethod
    def get(
        cls,
        source_id: int,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get all releases for a source of economic data.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/source_releases.html

        Args:
            source_id: The ID for a source.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by. Options include:

                - "release_id"
                - "name"
                - "press_release"
                - "realtime_start"
                - "realtime_end"

            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data on all releases for a
            source of economic data.

        Examples:
            >>> finagg.fred.api.source.releases.get(1, limit=5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
               id realtime_start realtime_end                                               name  press_release                                         link
            0  13     2023-03-15   2023-03-15  G.17 Industrial Production and Capacity Utiliz...           True  http://www.federalreserve.gov/releases/g17/
            1  14     2023-03-15   2023-03-15                               G.19 Consumer Credit           True  http://www.federalreserve.gov/releases/g19/
            2  15     2023-03-15   2023-03-15                         G.5 Foreign Exchange Rates           True   http://www.federalreserve.gov/releases/g5/
            3  17     2023-03-15   2023-03-15                        H.10 Foreign Exchange Rates           True  http://www.federalreserve.gov/releases/h10/
            4  18     2023-03-15   2023-03-15                       H.15 Selected Interest Rates           True  http://www.federalreserve.gov/releases/h15/

        """
        data = _api.get(
            cls.url,
            source_id=source_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["releases"]
        return pd.DataFrame(data)


class Source(_api.API):
    """Get a source of economic data.

    The module variable :data:`finagg.fred.api.source` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    releases = Releases()
    """"source/releases" FRED API. Get the releases for a source of economic
    data. The most popular way for accessing the :class:`Releases` API.

    :meta hide-value:
    """

    url = "https://api.stlouisfed.org/fred/source"

    @classmethod
    def get(
        cls,
        source_id: int,
        /,
        *,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get overview data of an economic series.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/source.html

        Args:
            source_id: The ID for a source.
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing high-level info on an economic source.

        Examples:
            >>> finagg.fred.api.source.get(1)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
               id realtime_start realtime_end                                               name                            link
            0   1     2023-03-15   2023-03-15  Board of Governors of the Federal Reserve Syst...  http://www.federalreserve.gov/

        """
        data = _api.get(
            cls.url,
            source_id=source_id,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            api_key=api_key,
        ).json()
        data = data["sources"]
        return pd.DataFrame(data)


class Sources(_api.API):
    """Get all FRED sources of economic data.

    The module variable :data:`finagg.fred.api.sources` is an instance
    of this API implementation and is the most popular interface for calling
    this API.

    """

    url = "https://api.stlouisfed.org/fred/sources"

    @classmethod
    def get(
        cls,
        realtime_start: None | int | str = None,
        realtime_end: None | int | str = None,
        limit: None | int = 1000,
        offset: None | int = 0,
        order_by: None | str = None,
        sort_order: None | str = None,
        api_key: None | str = None,
    ) -> pd.DataFrame:
        """Get all sources of economic data.

        See the related FRED API documentation at:

            https://fred.stlouisfed.org/docs/api/fred/sources.html

        Args:
            realtime_start: Start date for fetching results
                according to their publication date.
            realtime_end: End date for fetching results according
                to their publication date.
            limit: Maximum number of results to return.
            offset: Result start offset.
            order_by: Variable to order results by. Options include:

                - "source_id"
                - "name"
                - "press_release"
                - "realtime_start"
                - "realtime_end"

            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Defaults to the ``FRED_API_KEY``
                environment variable.

        Returns:
            A dataframe containing data on all sources of economic
            data.

        Examples:
            >>> finagg.fred.api.sources.get(limit=5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
               id realtime_start realtime_end                                               name                              link
            0   1     2023-03-15   2023-03-15  Board of Governors of the Federal Reserve Syst...    http://www.federalreserve.gov/
            1   3     2023-03-15   2023-03-15               Federal Reserve Bank of Philadelphia  https://www.philadelphiafed.org/
            2   4     2023-03-15   2023-03-15                  Federal Reserve Bank of St. Louis        http://www.stlouisfed.org/
            3   6     2023-03-15   2023-03-15  Federal Financial Institutions Examination Cou...             http://www.ffiec.gov/
            4  11     2023-03-15   2023-03-15                                Dow Jones & Company           http://www.dowjones.com

        """
        data = _api.get(
            cls.url,
            realtime_start=realtime_start,
            realtime_end=realtime_end,
            limit=limit,
            offset=offset,
            order_by=order_by,
            sort_order=sort_order,
            api_key=api_key,
        ).json()
        data = data["sources"]
        return pd.DataFrame(data)


source = Source()
"""The most popular way for accessing :class:`Source`.

:meta hide-value:
"""

sources = Sources()
"""The most popular way for accessing :class:`Sources`.

:meta hide-value:
"""
