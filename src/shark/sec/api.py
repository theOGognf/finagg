"""SEC EDGAR API.

See the official SEC EDGAR API page for more info:
    https://www.sec.gov/edgar/sec-api-documentation

"""

import logging
import os
import sys
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import ClassVar

import pandas as pd
import requests
import requests_cache

from .. import backend, ratelimit, utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | shark.sec.api - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

session = requests_cache.CachedSession(
    str(backend.http_cache_path),
    expire_after=timedelta(weeks=1),
)


class _Dataset(ABC):
    """Abstract SEC EDGAR API."""

    #: Request API URL.
    url: ClassVar[str]

    def __init__(self, *args, **kwargs) -> None:
        raise RuntimeError(
            "Instantiating an SEC API directly is not allowed. "
            "Use the `get` method instead."
        )

    @classmethod
    @abstractmethod
    def get(cls, *args, **kwargs) -> dict | pd.DataFrame:
        """Main dataset API method."""


class _CompanyConcept(_Dataset):
    """Get the full history of a company's concept (taxonomy and tag)."""

    #: API URL.
    url = (
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
            cik = str(get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik, taxonomy=taxonomy, tag=tag)
        response = get(url, user_agent=user_agent)
        content = response.json()
        units = content.pop("units")
        results_list = []
        for unit, data in units.items():  # type: ignore
            df = pd.DataFrame(data)
            df["units"] = unit
            results_list.append(df)
        results = pd.concat(results_list)
        for k, v in content.items():
            results[k] = v
        results["cik"] = cik
        return results.rename(columns={"entityName": "entity", "val": "value"})


class _CompanyFacts(_Dataset):
    """Get all XBRL disclosures from a single company (CIK)."""

    #: API URL.
    url = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

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
            cik = str(get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = get(url, user_agent=user_agent)
        content = response.json()
        facts = content.pop("facts")
        results_list = []
        for taxonomy, tag_dict in facts.items():
            for tag, data in tag_dict.items():
                for col, rows in data["units"].items():
                    df = pd.DataFrame(rows)
                    df["taxonomy"] = taxonomy
                    df["tag"] = tag
                    df["label"] = data["label"]
                    df["description"] = data["description"]
                    df["units"] = col
                    results_list.append(df)
        results = pd.concat(results_list)
        for k, v in content.items():
            results[k] = v
        results["cik"] = cik
        return results.rename(
            columns={"entityName": "entity", "uom": "units", "val": "value"}
        )


class _Frames(_Dataset):
    """Get one fact for each reporting entity that most closely fits
    the calendrical period requested.

    """

    #: API URL.
    url = (
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
        response = get(url, user_agent=user_agent)
        content = response.json()
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
    url = "https://data.sec.gov/submissions/CIK{cik}.json"

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
            cik = str(get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = get(url, user_agent=user_agent)
        content = response.json()
        recent_filings = content.pop("filings")["recent"]
        df = pd.DataFrame(recent_filings)
        df.columns = map(utils.snake_case, df.columns)  # type: ignore
        df.rename(columns={"accession_number": "accn"})
        df["cik"] = cik
        metadata = {utils.snake_case(k): v for k, v in content.items()}
        mailing_address = metadata["addresses"]["mailing"]
        business_address = metadata["addresses"]["business"]
        metadata["addresses"]["mailing"] = {
            utils.snake_case(k): v for k, v in mailing_address.items()
        }
        metadata["addresses"]["business"] = {
            utils.snake_case(k): v for k, v in business_address.items()
        }
        return {"metadata": metadata, "filings": df}


class _Tickers(_Dataset):
    """Simple dataset to get the table of all SEC CIKs for all tickers."""

    #: API URL.
    url = "https://www.sec.gov/files/company_tickers.json"

    @classmethod
    def get(cls, *, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing all SEC-registered, basic ticker info.

        Contains:
            - SEC CIK
            - (uppercase) ticker
            - company title

        """
        response = get(cls.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        df = pd.DataFrame([items for _, items in content.items()])
        return df.rename(columns={"cik_str": "cik"})


#: Mapping of SEC CIK strings to (uppercase) tickers.
_cik_to_tickers: dict[str, str] = {}

#: Mapping of (uppercase) tickers to SEC CIK strings.
_tickers_to_cik: dict[str, str] = {}

#: Get the full history of a company's concept (taxonomy and tag).
company_concept = _CompanyConcept

#: Get all XBRL disclosures from a single company (CIK).
company_facts = _CompanyFacts

#: Get one fact for each reporting entity that most closely fits
#: the calendrical period requested.
frames = _Frames

#: Get a company's metadata and recent submissions.
submissions = _Submissions

#: Used to get all SEC ticker data as opposed to
#: an individual ticker's SEC CIK.
tickers = _Tickers


@ratelimit.guard([ratelimit.RequestLimit(9, timedelta(seconds=1))], warn=True)
def _guarded_get(url: str, user_agent: str, /) -> requests.Response:
    """Guarded version of `session.get`."""
    return session.get(url, headers={"User-Agent": user_agent})


def get(url: str, /, *, user_agent: None | str = None) -> requests.Response:
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
    response = _guarded_get(url, user_agent)
    response.raise_for_status()
    return response


def get_cik(ticker: str, /, *, user_agent: None | str = None) -> str:
    """Return a ticker's SEC CIK."""
    if not _tickers_to_cik:
        response = get(_Tickers.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        for _, items in content.items():
            normalized_cik = str(items["cik_str"]).zfill(10)
            _tickers_to_cik[items["ticker"]] = normalized_cik
            _cik_to_tickers[normalized_cik] = items["ticker"]
    return _tickers_to_cik[ticker.upper()]


def get_ticker(cik: str, /, *, user_agent: None | str = None) -> str:
    """Return an SEC CIK's ticker."""
    if not _cik_to_tickers:
        response = get(_Tickers.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        for _, items in content.items():
            normalized_cik = str(items["cik_str"]).zfill(10)
            _cik_to_tickers[normalized_cik] = items["ticker"]
            _tickers_to_cik[items["ticker"]] = normalized_cik
    cik = str(cik).zfill(10)
    return _cik_to_tickers[cik]
