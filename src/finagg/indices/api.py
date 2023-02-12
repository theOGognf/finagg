"""Indices API (symbols in popular indices)."""

import os
from abc import ABC, abstractmethod
from datetime import timedelta
from functools import cache
from typing import ClassVar

import pandas as pd
import requests
import requests_cache
from bs4 import BeautifulSoup

from .. import backend

session = requests_cache.CachedSession(
    str(backend.http_cache_path),
    expire_after=timedelta(weeks=1),
)


class _API(ABC):
    """Abstract indices API."""

    #: Request API URL.
    url: ClassVar[str]

    @classmethod
    @abstractmethod
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    def get_ticker_list(cls, *, user_agent: None | str = None) -> list[str]:
        """List the tickers in the index."""
        df = cls.get(user_agent=user_agent)
        return df["ticker"].tolist()


class _DJIA(_API):

    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"

    @classmethod
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing data on the tickers in the DJIA."""
        response = get(cls.url, user_agent=user_agent)
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


class _Nasdaq100(_API):

    url = "https://en.wikipedia.org/wiki/Nasdaq-100"

    @classmethod
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing data on the tickers in the Nasdaq 100."""
        response = get(cls.url, user_agent=user_agent)
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


class _SP500(_API):

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    @classmethod
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing data on the tickers in the S&P 500."""
        response = get(cls.url, user_agent=user_agent)
        soup = BeautifulSoup(response.text, "html.parser")
        tbl = soup.find("table", {"class": "wikitable"})
        (df,) = pd.read_html(str(tbl))
        df = pd.DataFrame(df)
        df.drop("SEC filings", axis=1, inplace=True, errors="ignore")
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


#: The Nasdaq Composite 100.
djia = _DJIA()

#: The Nasdaq Composite 100.
nasdaq100 = _Nasdaq100()

#: The Standard and Poor's 500.
sp500 = _SP500()


def get(url: str, /, *, user_agent: None | str = None) -> requests.Response:
    """Tickers API request helper.

    Args:
        url: Complete URL to get from.
        user_agent: Required user agent header declaration to avoid errors.

    Returns:
        Successful responses.

    """
    user_agent = user_agent or os.environ.get(
        "INDICES_API_USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    )
    if not user_agent:
        raise RuntimeError(
            "No indices API user agent declaration found. "
            "Pass your user agent declaration to the API directly, or "
            "set the `INDICES_API_USER_AGENT` environment variable."
        )
    response = session.get(url, headers={"User-Agent": user_agent})
    response.raise_for_status()
    return response


@cache
def get_ticker_set(*, user_agent: None | str = None) -> set[str]:
    """Get the set of tickers from all the indices."""
    tickers = set()
    tickers.update(djia.get_ticker_list(user_agent=user_agent))
    tickers.update(nasdaq100.get_ticker_list(user_agent=user_agent))
    tickers.update(sp500.get_ticker_list(user_agent=user_agent))
    return tickers
