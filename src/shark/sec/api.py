"""SEC EDGAR API."""

import json
import os
import pathlib
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import ClassVar

import pandas as pd
import requests
import requests_cache

from ..utils import snake_case

_API_CACHE_PATH = os.environ.get(
    "SEC_API_CACHE_PATH",
    pathlib.Path(__file__).resolve().parent.parent.parent.parent
    / "data"
    / "sec_api_cache",
)
_API_CACHE_PATH = pathlib.Path(_API_CACHE_PATH)
_API_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

requests_cache.install_cache(
    _API_CACHE_PATH,
    expire_after=timedelta(days=1),
)


class _Dataset(ABC):
    """Abstract SEC EDGAR API."""

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating an SEC API directly is not allowed. "
            "Use the `get` method instead."
        )

    @classmethod
    @abstractmethod
    def get(cls, *, api_key: None | str = None) -> dict | pd.DataFrame:
        """Main dataset API method."""

    @classmethod
    @property
    @abstractmethod
    def url(cls) -> str:
        """Request API URL."""


class _CompanyConcept(_Dataset):
    """Get the full history of a company's concept (taxonomy and tag)."""

    #: API URL.
    url: ClassVar[str] = (
        "https://data.sec.gov/api/xbrl"
        "/companyconcept"
        "/CIK{cik}/{taxonomy}/{tag}.json"
    )

    @classmethod
    def get(
        cls,
        tag: str,
        /,
        *,
        cik: None | str = None,
        ticker: None | str = None,
        taxonomy: str = "us-gaap",
        units: str = "USD",
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Return all XBRL disclosures from a single company (CIK)
        and concept (a taxonomy and tag) in a single dataframe.

        Args:
            tag: Valid tag within the given `taxonomy`.
            cik: Company SEC CIK. Mutually exclusive with `ticker`.
            ticker: Company ticker. Mutually exclusive with `cik`.
            taxonomy: Valid SEC EDGAR taxonomy.
                See https://www.sec.gov/info/edgar/edgartaxonomies.shtml for taxonomies.
            units: Currency to view results in.
            user_agent: Self-declared bot header.

        Returns:
            Dataframe with normalized column names.

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`")

        if ticker:
            cik = str(_API.get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik, taxonomy=taxonomy, tag=tag)
        response = _API.get(url, user_agent=user_agent)
        content = json.loads(response.content)
        units = content.pop("units")
        results = []
        for unit, data in units.items():
            df = pd.DataFrame(data)
            df["units"] = unit
            results.append(df)
        results = pd.concat(results)
        for k, v in content.items():
            results[k] = v
        return results.rename(columns={"entityName": "entity", "val": "value"})


class _CompanyFacts(_Dataset):
    """Get all XBRL disclosures from a single company (CIK)."""

    #: API URL.
    url: ClassVar[str] = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    @classmethod
    def get(
        cls,
        /,
        *,
        cik: None | str = None,
        ticker: None | str = None,
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Return all XBRL disclosures from a single company (CIK).

        Args:
            cik: Company SEC CIK. Mutually exclusive with `ticker`.
            ticker: Company ticker. Mutually exclusive with `cik`.
            user_agent: Self-declared bot header.

        Returns:
            Dataframe with normalized column names.

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`")

        if ticker:
            cik = str(_API.get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = _API.get(url, user_agent=user_agent)
        content = json.loads(response.content)
        facts = content.pop("facts")
        results = []
        for taxonomy, tag_dict in facts.items():
            for tag, data in tag_dict.items():
                for col, rows in data["units"].items():
                    df = pd.DataFrame(rows)
                    df["taxonomy"] = taxonomy
                    df["tag"] = tag
                    df["label"] = data["label"]
                    df["description"] = data["description"]
                    df["units"] = col
                    results.append(df)
        results = pd.concat(results)
        for k, v in content.items():
            results[k] = v
        return results.rename(
            columns={"entityName": "entity", "uom": "units", "val": "value"}
        )


class _Frames(_Dataset):
    """Get one fact for each reporting entity that most closely fits
    the calendrical period requested.

    """

    #: API URL.
    url: ClassVar[str] = (
        "https://data.sec.gov/api/xbrl"
        "/frames"
        "/{taxonomy}/{tag}/{units}/CY{year}Q{quarter}I.json"
    )

    @classmethod
    def get(
        cls,
        tag: str,
        year: int | str,
        quarter: int | str,
        /,
        *,
        taxonomy: str = "us-gaap",
        units: str = "USD",
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Get one fact for each reporting entity that most closely fits
        the calendrical period requested.

        Args:
            tag: Valid tag within the given `taxonomy`.
            year: Year to retrieve.
            quarter: Quarter to retrieve.
            taxonomy: Valid SEC EDGAR taxonomy.
                See https://www.sec.gov/info/edgar/edgartaxonomies.shtml for taxonomies.
            units: Current to view results in.
            user_agent: Self-declared bot header.

        Returns:
            Dataframe with slightly improved column names.

        """
        url = cls.url.format(
            taxonomy=taxonomy, tag=tag, units=units, year=year, quarter=quarter
        )
        response = _API.get(url, user_agent=user_agent)
        content = json.loads(response.content)
        data = content.pop("data")
        df = pd.DataFrame(data)
        for k, v in content.items():
            df[k] = v
        return df.rename(
            columns={
                "ccp": "frame",
                "entityName": "entity",
                "uom": "units",
                "val": "value",
            }
        )


class _Submissions(_Dataset):
    """Get an entity's metadata and current filing history."""

    #: API URL.
    url: ClassVar[str] = "https://data.sec.gov/submissions/CIK{cik}.json"

    @classmethod
    def get(
        cls,
        /,
        *,
        cik: None | str = None,
        ticker: None | str = None,
        user_agent: None | str = None,
    ) -> dict:
        """Return all recent filings from a single company (CIK).

        Args:
            cik: Company SEC CIK. Mutually exclusive with `ticker`.
            ticker: Company ticker. Mutually exclusive with `cik`.
            user_agent: Self-declared bot header.

        Returns:
            Metadata and a filings dataframe with normalized column names.

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`")

        if ticker:
            cik = str(_API.get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = _API.get(url, user_agent=user_agent)
        content = json.loads(response.content)
        recent_filings = content.pop("filings")["recent"]
        df = pd.DataFrame(recent_filings)
        df.columns = map(snake_case, df.columns)
        df.rename(columns={"accession_number": "accn"})
        metadata = {snake_case(k): v for k, v in content.items()}
        mailing_address = metadata["addresses"]["mailing"]
        business_address = metadata["addresses"]["business"]
        metadata["addresses"]["mailing"] = {
            snake_case(k): v for k, v in mailing_address.items()
        }
        metadata["addresses"]["business"] = {
            snake_case(k): v for k, v in business_address.items()
        }
        return {"metadata": metadata, "filings": df}


class _Tickers(_Dataset):
    """Simple dataset to get the table of all SEC CIKs for all tickers."""

    #: API URL.
    url: ClassVar[str] = "https://www.sec.gov/files/company_tickers.json"

    @classmethod
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing all SEC-registered, basic ticker info.

        Contains:
            - SEC CIK
            - (uppercase) ticker
            - company title

        """
        response = _API.get(cls.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = json.loads(response.content)
        df = pd.DataFrame([items for _, items in content.items()])
        return df.rename(columns={"cik_str": "cik"})


class _API:
    """Collection of SEC EDGAR APIs."""

    #: Mapping of (uppercase) tickers to SEC CIK strings.
    _tickers_to_cik: ClassVar[dict[str, str]] = {}

    #: Path to SEC API requests cache.
    cache_path: ClassVar[str] = str(_API_CACHE_PATH)

    #: Get the full history of a company's concept (taxonomy and tag).
    company_concept: ClassVar[type[_CompanyConcept]] = _CompanyConcept

    #: Get all XBRL disclosures from a single company (CIK).
    company_facts: ClassVar[type[_CompanyFacts]] = _CompanyFacts

    #: Get one fact for each reporting entity that most closely fits
    #: the calendrical period requested.
    frames: ClassVar[type[_Frames]] = _Frames

    submissions: ClassVar[type[_Submissions]] = _Submissions

    #: Used to get all SEC ticker data as opposed to
    #: an individual ticker's SEC CIK.
    tickers: ClassVar[type[_Tickers]] = _Tickers

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating an SEC API directly is not allowed. "
            "Use the `get` method instead."
        )

    @classmethod
    def get(cls, url: str, /, *, user_agent: None | str = None) -> requests.Response:
        """SEC EDGAR API request helper.

        Args:
            url: Complete URL to get from.
            user_agent: Required user agent header declaration to avoid errors.

        Returns:
            Successful responses.

        """
        user_agent = user_agent or os.environ.get("SEC_API_USER_AGENT", None)
        if not user_agent:
            raise RuntimeError(
                "No SEC API user agent declaration found. "
                "Pass your user agent declaration to the API directly, or "
                "set the `SEC_API_USER_AGENT` environment variable."
            )
        response = requests.get(url, headers={"User-Agent": user_agent})
        response.raise_for_status()
        return response

    @classmethod
    def get_cik(cls, ticker: str, /, *, user_agent: None | str = None) -> str:
        """Return a ticker's SEC CIK."""
        if not cls._tickers_to_cik:
            response = cls.get(_Tickers.url, user_agent=user_agent)
            content: dict[str, dict[str, str]] = json.loads(response.content)
            for _, items in content.items():
                cls._tickers_to_cik[items["ticker"]] = items["cik_str"]
        return cls._tickers_to_cik[ticker.upper()]


#: Public-facing SEC API.
api = _API
