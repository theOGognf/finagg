"""Tickers API (symbols in popular indices)."""

import os
import pathlib
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import cache
from typing import ClassVar, Type

import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup

_API_CACHE_PATH = os.environ.get(
    "TICKERS_API_CACHE_PATH",
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "tickers_api_cache",
)
_API_CACHE_PATH = pathlib.Path(_API_CACHE_PATH)
_API_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

session = requests_cache.CachedSession(
    _API_CACHE_PATH,
    expire_after=timedelta(weeks=1),
)


class _Dataset(ABC):
    """Abstract ticlers API."""

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a tickers API directly is not allowed. "
            "Use the `get` method instead."
        )

    @classmethod
    @abstractmethod
    def get(cls, *, user_agent: None | str = None) -> dict | pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    def get_ticker_list(cls, *, user_agent: None | str = None) -> list[str]:
        """List the tickers in the index."""
        df = cls.get(user_agent=user_agent)
        return df["ticker"].tolist()

    @classmethod
    @property
    @abstractmethod
    def url(cls) -> str:
        """Request API URL."""


class _DJIA(_Dataset):
    """List all companies currently in the DJIA."""

    #: API URL.
    url: ClassVar[str] = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

    @classmethod
    @cache
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing data on the tickers in the DJIA."""
        response = _API.get(cls.url, user_agent=user_agent)
        soup = BeautifulSoup(response.text, "html.parser")
        tbl = soup.find("table", {"class": "wikitable"})
        (df,) = pd.read_html(str(tbl))
        df = pd.DataFrame(df)

        def _percent_to_fraction(item: str) -> float:
            value, _ = item.split("%")
            return float(value) / 100

        df.drop("Notes", axis=1, inplace=True)
        df = df.rename(
            columns={
                "Company": "company",
                "Exchange": "exchange",
                "Symbol": "ticker",
                "Industry": "industry",
                "Date added": "added",
                "Index weighting": "weight",
            }
        )
        df["weight"] = df["weight"].apply(_percent_to_fraction)
        return df


class _Nasdaq100(_Dataset):
    """List all companies currently in the Nasdaq 100."""

    #: API URL.
    url: ClassVar[str] = "https://en.wikipedia.org/wiki/Nasdaq-100"

    @classmethod
    @cache
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing data on the tickers in the Nasdaq 100."""
        response = _API.get(cls.url, user_agent=user_agent)
        soup = BeautifulSoup(response.text, "html.parser")
        tbl = soup.find_all("table", {"class": "wikitable"})[3]
        (df,) = pd.read_html(str(tbl))
        df = pd.DataFrame(df)
        return df.rename(
            columns={
                "Company": "company",
                "Ticker": "ticker",
                "GICS Sector": "industry",
                "GICS Sub-Industry": "sub_industry",
            }
        )


class _SP500(_Dataset):
    """List all companies currently in the S&P 500."""

    #: API URL.
    url: ClassVar[str] = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    @classmethod
    @cache
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing data on the tickers in the S&P 500."""
        response = _API.get(cls.url, user_agent=user_agent)
        soup = BeautifulSoup(response.text, "html.parser")
        tbl = soup.find("table", {"class": "wikitable"})
        (df,) = pd.read_html(str(tbl))
        df = pd.DataFrame(df)
        df.drop("SEC filings", axis=1, inplace=True)
        return df.rename(
            columns={
                "Symbol": "ticker",
                "Security": "company",
                "GICS Sector": "industry",
                "GICS Sub-Industry": "sub_industry",
                "Headquarters Location": "headquarters",
                "Date first added": "added",
                "CIK": "cik",
                "Founded": "founded",
            }
        )


class _API:
    """Collection of ticker APIs."""

    #: The Dow Jones Industrial Average.
    djia: ClassVar[Type[_DJIA]] = _DJIA

    #: The Nasdaq Composite 100.
    nasdaq100: ClassVar[Type[_Nasdaq100]] = _Nasdaq100

    #: The Standard and Poor's 500.
    sp500: ClassVar[Type[_SP500]] = _SP500

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating a tickers API directly is not allowed. "
            "Use the `get` method instead."
        )

    @classmethod
    def get(cls, url: str, /, *, user_agent: None | str = None) -> requests.Response:
        """Tickers API request helper.

        Args:
            url: Complete URL to get from.
            user_agent: Required user agent header declaration to avoid errors.

        Returns:
            Successful responses.

        """
        user_agent = user_agent or os.environ.get("TICKERS_API_USER_AGENT", None)
        if not user_agent:
            raise RuntimeError(
                "No tickers API user agent declaration found. "
                "Pass your user agent declaration to the API directly, or "
                "set the `TICKERS_API_USER_AGENT` environment variable."
            )
        response = session.get(url, headers={"User-Agent": user_agent})
        response.raise_for_status()
        return response

    @classmethod
    @cache
    def get_ticker_set(cls, *, user_agent: None | str = None) -> set[str]:
        """Get the set of tickers from all the indices."""
        tickers = set()
        tickers.update(cls.djia.get_ticker_list(user_agent=user_agent))
        tickers.update(cls.nasdaq100.get_ticker_list(user_agent=user_agent))
        tickers.update(cls.sp500.get_ticker_list(user_agent=user_agent))
        return tickers


#: Public-facing tickers API.
api = _API
