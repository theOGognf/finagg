"""The "fred/source" and "fred/sources" APIs.

See the official FRED API docs for more info:
    https://fred.stlouisfed.org/docs/api/fred/

Examples:
    List sources of economic data.
    >>> import finagg.fred.api as fred
    >>> fred.sources.get()

"""

import pandas as pd

from . import _api


class _Releases(_api.API):

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
            order_by: Variable to order results by.
                Options include:
                    - "release_id"
                    - "name"
                    - "press_release"
                    - "realtime_start"
                    - "realtime_end"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on all releases for a
            source of economic data.

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


class _Source(_api.API):
    """Get a source of economic data."""

    #: "source/releases" FRED API. Get the releases for a source of economic data.
    releases = _Releases()

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
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing high-level info on an economic source.

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


class _Sources(_api.API):

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
            order_by: Variable to order results by.
                Options include:
                    - "source_id"
                    - "name"
                    - "press_release"
                    - "realtime_start"
                    - "realtime_end"
            sort_order: Sort results in ascending ("asc") or
                descending ("desc") order.
            api_key: Your FRED API key. Pulled from the `FRED_API_KEY`
                environment variable if left `None`.

        Returns:
            A dataframe containing data on all sources of economic
            data.

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


#: Public-facing "fred/source" and "fred/sources" API.
source = _Source()
sources = _Sources()
