from typing import Any

import finagg


class MockResponse:
    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self.payload


def test_calendar_get(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def get(url: str, **kwargs: Any) -> MockResponse:
        calls.append({"url": url, **kwargs})
        return MockResponse(
            {
                "currency": "usd",
                "calendar": [
                    {
                        "indicator": "cpi",
                        "release_date": "2026-01-01",
                    }
                ],
            }
        )

    monkeypatch.setattr(finagg.fxmacrodata.api.session, "get", get)

    df = finagg.fxmacrodata.api.calendar(
        "usd",
        indicator_="cpi",
        start_date="2026-01-01",
        end_date="2026-01-31",
    )

    assert calls[0]["url"].endswith("/v1/calendar/usd")
    assert calls[0]["params"] == {
        "indicator": "cpi",
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }
    assert df.loc[0, "indicator"] == "cpi"
    assert df.loc[0, "currency"] == "usd"


def test_data_catalogue_uses_api_key_header(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def get(url: str, **kwargs: Any) -> MockResponse:
        calls.append({"url": url, **kwargs})
        return MockResponse({"indicators": [{"indicator": "cpi"}]})

    monkeypatch.setattr(finagg.fxmacrodata.api.session, "get", get)

    df = finagg.fxmacrodata.api.data_catalogue(
        "usd",
        include_capabilities=True,
        api_key="test-key",
    )

    assert calls[0]["url"].endswith("/v1/data_catalogue/usd")
    assert calls[0]["headers"] == {"X-API-Key": "test-key"}
    assert calls[0]["params"] == {"include_capabilities": "true"}
    assert df.loc[0, "indicator"] == "cpi"


def test_endpoint_helpers(monkeypatch: Any) -> None:
    urls: list[str] = []

    def get(url: str, **kwargs: Any) -> MockResponse:
        urls.append(url)
        return MockResponse([{"ok": True}])

    monkeypatch.setattr(finagg.fxmacrodata.api.session, "get", get)

    finagg.fxmacrodata.api.indicator("usd", "cpi")
    finagg.fxmacrodata.api.latest_announcements("usd")
    finagg.fxmacrodata.api.predictions("usd", "cpi")
    finagg.fxmacrodata.api.forex("eur", "usd")
    finagg.fxmacrodata.api.cot("usd")
    finagg.fxmacrodata.api.commodities("xau_usd")
    finagg.fxmacrodata.api.market_sessions()

    assert [url.removeprefix(finagg.fxmacrodata.api.url) for url in urls] == [
        "/v1/announcements/usd/cpi",
        "/v1/announcements/usd/latest",
        "/v1/predictions/usd/cpi",
        "/v1/forex/eur/usd",
        "/v1/cot/usd",
        "/v1/commodities/xau_usd",
        "/v1/market_sessions",
    ]
