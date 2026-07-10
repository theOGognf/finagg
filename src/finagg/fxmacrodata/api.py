"""An implementation of the FXMacroData API.

FXMacroData provides macroeconomic announcements, release calendars,
forecast/consensus rows, FX spot history, COT positioning, commodity series,
and market-session context for FX and macro research workflows.

Public USD catalogue, calendar, and macro announcement examples work without
an API key. Protected coverage can be accessed by passing ``api_key`` directly
or by setting ``FXMD_API_KEY`` or ``FXMACRODATA_API_KEY`` in the environment.

See the official `FXMacroData docs`_ for endpoint-level details.

.. _`FXMacroData docs`: https://fxmacrodata.com/documentation
"""

import os
from datetime import timedelta
from typing import Any

import pandas as pd
import requests
import requests_cache

from .. import config

if config.disable_http_cache:
    session = requests.Session()
else:
    session = requests_cache.CachedSession(
        str(config.http_cache_path),
        ignored_parameters=["api_key"],
        expire_after=timedelta(hours=1),
    )


#: The FXMacroData API base URL. All API requests are made under this URL.
url = "https://api.fxmacrodata.com"


def _api_key(api_key: None | str = None) -> None | str:
    """Return an explicit or environment-provided FXMacroData API key."""
    return api_key or os.environ.get("FXMD_API_KEY") or os.environ.get(
        "FXMACRODATA_API_KEY"
    )


def pformat(**kwargs: Any) -> dict[str, Any]:
    """FXMacroData API parameter formatting.

    Args:
        **kwargs: All possible FXMacroData query parameters.

    Returns:
        Mapping of request parameter name to formatted value.
    """
    params: dict[str, Any] = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        if isinstance(value, bool):
            params[key] = "true" if value else "false"
        else:
            params[key] = value
    return params


def get(path: str, /, *, api_key: None | str = None, **kwargs: Any) -> requests.Response:
    """Main API get function used by all FXMacroData methods.

    Args:
        path: API path under :data:`url`.
        api_key: Optional FXMacroData API key.
        **kwargs: Query parameters.

    Returns:
        A valid FXMacroData API response.
    """
    headers: dict[str, str] = {}
    key = _api_key(api_key)
    if key:
        headers["X-API-Key"] = key

    response = session.get(
        f"{url}{path}",
        params=pformat(**kwargs),
        headers=headers or None,
    )
    response.raise_for_status()
    return response


def _frame(payload: Any) -> pd.DataFrame:
    """Normalize common FXMacroData response containers into a dataframe."""
    if isinstance(payload, list):
        return pd.json_normalize(payload)

    if isinstance(payload, dict):
        for key in (
            "data",
            "items",
            "results",
            "records",
            "calendar",
            "announcements",
            "predictions",
            "series",
            "cot",
            "commodities",
            "sessions",
            "indicators",
            "capabilities",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                df = pd.json_normalize(value)
                for meta_key, meta_value in payload.items():
                    if meta_key == key or isinstance(meta_value, (dict, list)):
                        continue
                    if meta_key not in df.columns:
                        df[meta_key] = meta_value
                return df
        return pd.json_normalize(payload)

    return pd.DataFrame({"value": [payload]})


def data_catalogue(
    currency: str = "usd",
    /,
    *,
    include_capabilities: None | bool = None,
    include_coverage: None | bool = None,
    indicator: None | str = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData catalogue and coverage metadata.

    Args:
        currency: Three-letter currency code, such as ``"usd"``.
        include_capabilities: Include endpoint capability metadata.
        include_coverage: Include coverage metadata.
        indicator: Filter catalogue rows to one indicator.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of catalogue rows.
    """
    response = get(
        f"/v1/data_catalogue/{currency.lower()}",
        include_capabilities=include_capabilities,
        include_coverage=include_coverage,
        indicator=indicator,
        api_key=api_key,
    )
    return _frame(response.json())


def indicator(
    currency: str,
    indicator_: str,
    /,
    *,
    start_date: None | str = None,
    end_date: None | str = None,
    series_mode: None | str = None,
    limit: None | int = None,
    offset: None | int = None,
    page: None | int = None,
    seasonality: None | bool = None,
    frequency: None | str = None,
    revisions: None | bool = None,
    basis: None | str = None,
    official_only: None | bool = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData macro announcement or indicator history rows.

    Args:
        currency: Three-letter currency code.
        indicator_: FXMacroData indicator slug.
        start_date: Optional inclusive start date in ``YYYY-MM-DD`` format.
        end_date: Optional inclusive end date in ``YYYY-MM-DD`` format.
        series_mode: Optional API series mode.
        limit: Optional page size.
        offset: Optional result offset.
        page: Optional page number.
        seasonality: Include seasonality fields when supported.
        frequency: Optional result frequency.
        revisions: Include revision rows when supported.
        basis: Optional basis parameter.
        official_only: Filter to official-source rows when supported.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of announcement or indicator rows.
    """
    response = get(
        f"/v1/announcements/{currency.lower()}/{indicator_}",
        start_date=start_date,
        end_date=end_date,
        series_mode=series_mode,
        limit=limit,
        offset=offset,
        page=page,
        seasonality=seasonality,
        frequency=frequency,
        revisions=revisions,
        basis=basis,
        official_only=official_only,
        api_key=api_key,
    )
    return _frame(response.json())


def calendar(
    currency: str = "usd",
    /,
    *,
    indicator_: None | str = None,
    start_date: None | str = None,
    end_date: None | str = None,
    timezone: None | str = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData economic release-calendar rows.

    Args:
        currency: Three-letter currency code.
        indicator_: Optional indicator slug filter.
        start_date: Optional inclusive start date in ``YYYY-MM-DD`` format.
        end_date: Optional inclusive end date in ``YYYY-MM-DD`` format.
        timezone: Optional timezone for announcement datetimes.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of release-calendar rows.
    """
    response = get(
        f"/v1/calendar/{currency.lower()}",
        indicator=indicator_,
        start_date=start_date,
        end_date=end_date,
        timezone=timezone,
        api_key=api_key,
    )
    return _frame(response.json())


def latest_announcements(
    currency: str = "usd",
    /,
    *,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get the latest FXMacroData announcements for a currency.

    Args:
        currency: Three-letter currency code.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of latest announcement rows.
    """
    response = get(f"/v1/announcements/{currency.lower()}/latest", api_key=api_key)
    return _frame(response.json())


def predictions(
    currency: str,
    indicator_: str,
    /,
    *,
    prediction_type: None | str = None,
    prediction_source: None | str = None,
    start_date: None | str = None,
    end_date: None | str = None,
    limit: None | int = None,
    offset: None | int = None,
    page: None | int = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData prediction, consensus, or forecast rows.

    Args:
        currency: Three-letter currency code.
        indicator_: FXMacroData indicator slug.
        prediction_type: Optional prediction type filter.
        prediction_source: Optional prediction source filter.
        start_date: Optional inclusive start date in ``YYYY-MM-DD`` format.
        end_date: Optional inclusive end date in ``YYYY-MM-DD`` format.
        limit: Optional page size.
        offset: Optional result offset.
        page: Optional page number.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of prediction rows.
    """
    response = get(
        f"/v1/predictions/{currency.lower()}/{indicator_}",
        prediction_type=prediction_type,
        prediction_source=prediction_source,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
        page=page,
        api_key=api_key,
    )
    return _frame(response.json())


def forex(
    base: str,
    quote: str = "usd",
    /,
    *,
    start_date: None | str = None,
    end_date: None | str = None,
    limit: None | int = None,
    offset: None | int = None,
    page: None | int = None,
    indicators: None | str = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData FX spot history rows.

    Args:
        base: Base currency code.
        quote: Quote currency code.
        start_date: Optional inclusive start date in ``YYYY-MM-DD`` format.
        end_date: Optional inclusive end date in ``YYYY-MM-DD`` format.
        limit: Optional page size.
        offset: Optional result offset.
        page: Optional page number.
        indicators: Optional derived-indicator selection.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of FX spot rows.
    """
    response = get(
        f"/v1/forex/{base.lower()}/{quote.lower()}",
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
        page=page,
        indicators=indicators,
        api_key=api_key,
    )
    return _frame(response.json())


def cot(
    currency: str,
    /,
    *,
    start_date: None | str = None,
    end_date: None | str = None,
    limit: None | int = None,
    offset: None | int = None,
    page: None | int = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData COT positioning rows.

    Args:
        currency: Three-letter currency code.
        start_date: Optional inclusive start date in ``YYYY-MM-DD`` format.
        end_date: Optional inclusive end date in ``YYYY-MM-DD`` format.
        limit: Optional page size.
        offset: Optional result offset.
        page: Optional page number.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of COT positioning rows.
    """
    response = get(
        f"/v1/cot/{currency.lower()}",
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
        page=page,
        api_key=api_key,
    )
    return _frame(response.json())


def commodities(
    indicator_: str,
    /,
    *,
    start_date: None | str = None,
    end_date: None | str = None,
    limit: None | int = None,
    offset: None | int = None,
    page: None | int = None,
    api_key: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData commodity or energy rows.

    Args:
        indicator_: FXMacroData commodity indicator slug.
        start_date: Optional inclusive start date in ``YYYY-MM-DD`` format.
        end_date: Optional inclusive end date in ``YYYY-MM-DD`` format.
        limit: Optional page size.
        offset: Optional result offset.
        page: Optional page number.
        api_key: Optional FXMacroData API key.

    Returns:
        Dataframe of commodity rows.
    """
    response = get(
        f"/v1/commodities/{indicator_}",
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
        page=page,
        api_key=api_key,
    )
    return _frame(response.json())


def market_sessions(
    *,
    at: None | str = None,
) -> pd.DataFrame:
    """Get FXMacroData market-session rows.

    Args:
        at: Optional timestamp to evaluate sessions at.

    Returns:
        Dataframe of market-session rows.
    """
    response = get("/v1/market_sessions", at=at)
    return _frame(response.json())
