"""SEC EDGAR API.

See the official SEC EDGAR API page for more info:
    https://www.sec.gov/edgar/sec-api-documentation

Examples:
    Get the historical values of a company concept (e.g., net income/loss).
    >>> import finagg.sec.api as sec
    >>> sec.company_concept.get(ticker="AAPL")

    Get all company facts (multiple concepts).
    >>> sec.company_facts.get(ticker="AAPL")

    Get a concept for a period from all companies.
    >>> sec.frames.get("NetIncomeLoss", 2020, 3)

    Get the SEC CIK string representation of a ticker.
    >>> sec.get_cik("AAPL")

    Get the ticker from an SEC CIK.
    >>> sec.get_ticker("0000320193")

"""

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from functools import cache
from typing import Any, ClassVar, TypedDict

import pandas as pd
import requests
import requests_cache

from .. import backend, ratelimit, utils

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

session = requests_cache.CachedSession(
    str(backend.http_cache_path),
    expire_after=timedelta(weeks=1),
)


class Concept(TypedDict):
    #: Valid tag within the given `taxonomy`.
    tag: str

    #: Valid SEC EDGAR taxonomy.
    #: See https://www.sec.gov/info/edgar/edgartaxonomies.shtml for taxonomies.
    taxonomy: str

    #: Currency the concept is in.
    units: str


class Frame(Concept):
    """A frame is just a concept with a flag indicating if it supports
    'instantaneous' frame data.

    """

    #: Whether to retrieve "instant" calendrical results.
    instant: bool


def frame_to_concept(frame: Frame) -> Concept:
    """Helper for converting from a frame to a concept."""
    return {
        "tag": frame["tag"],
        "taxonomy": frame["taxonomy"],
        "units": "/".join(frame["units"].split("-per-")),
    }


class SubmissionsResult(TypedDict):
    #: Company metadata.
    metadata: dict[str, Any]

    #: Most recent company filings.
    filings: pd.DataFrame


class _API(ABC):
    """Abstract SEC EDGAR API."""

    #: Request API URL.
    url: ClassVar[str]

    @classmethod
    @abstractmethod
    def get(cls, *args: Any, **kwargs: Any) -> pd.DataFrame | SubmissionsResult:
        """Main dataset API method."""


class CompanyConcept(_API):

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
            raise ValueError("Must provide a `cik` or a `ticker`.")

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


class CompanyFacts(_API):

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
            raise ValueError("Must provide a `cik` or a `ticker`.")

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


class Frames(_API):

    url = (
        "https://data.sec.gov/api/xbrl"
        "/frames"
        "/{taxonomy}/{tag}/{units}/CY{year}{quarter}.json"
    )

    @classmethod
    def get(
        cls,
        tag: str,
        year: int | str,
        /,
        quarter: None | int | str = None,
        *,
        instant: bool = True,
        taxonomy: str = "us-gaap",
        units: str = "USD",
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Get one fact for each reporting entity that most closely fits
        the calendrical period requested.

        Args:
            tag: Valid tag within the given `taxonomy`.
            year: Year to retrieve.
            quarter: Quarter to retrieve data for. Most data is only provided
                at a quarterly rate, so this should be provided for most cases.
            instant: Whether to get instantaneous data for the frame (data that
                most closely matches a frame's year and quarter without a
                time buffer). This flag should be enabled for most cases.
            taxonomy: Valid SEC EDGAR taxonomy.
                See https://www.sec.gov/info/edgar/edgartaxonomies.shtml for taxonomies.
            units: Current to view results in.
            user_agent: Self-declared bot header.

        Returns:
            Dataframe with slightly improved column names.

        """
        if quarter:
            quarter = f"Q{quarter}"
            if instant:
                quarter = f"{quarter}I"
        else:
            quarter = ""
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


class Submissions(_API):

    url = "https://data.sec.gov/submissions/CIK{cik}.json"

    @classmethod
    def get(
        cls,
        /,
        *,
        cik: None | str = None,
        ticker: None | str = None,
        user_agent: None | str = None,
    ) -> SubmissionsResult:
        """Return all recent filings from a single company (CIK).

        Args:
            cik: Company SEC CIK. Mutually exclusive with `ticker`.
            ticker: Company ticker. Mutually exclusive with `cik`.
            user_agent: Self-declared bot header.

        Returns:
            Metadata and a filings dataframe with normalized column names.

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`.")

        if ticker:
            cik = str(get_cik(ticker))

        if cik:
            ticker = str(get_ticker(cik))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = get(url, user_agent=user_agent)
        content = response.json()
        recent_filings = content.pop("filings")["recent"]
        df = pd.DataFrame(recent_filings)
        df.columns = map(utils.snake_case, df.columns)  # type: ignore
        df.rename(columns={"accession_number": "accn"})
        df["cik"] = cik
        metadata = {}
        for k, v in content.items():
            if isinstance(v, str):
                metadata[k] = utils.snake_case(v)
        if "exchanges" in metadata:
            metadata["exchanges"] = ",".join(metadata["exchanges"])
        metadata["cik"] = cik
        metadata["ticker"] = str(ticker)
        return {"metadata": metadata, "filings": df}


class Tickers(_API):

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
company_concept = CompanyConcept()

#: Get all XBRL disclosures from a single company (CIK).
company_facts = CompanyFacts()

#: Get one fact for each reporting entity that most closely fits
#: the calendrical period requested.
frames = Frames()

#: Get a company's metadata and recent submissions.
submissions = Submissions()

#: Used to get all SEC ticker data as opposed to
#: an individual ticker's SEC CIK.
tickers = Tickers()

#: Company frames that have high availability. Units are in valid frame
#: format.
common_frames: list[Frame] = [
    {
        "tag": "AssetsCurrent",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
    {
        "tag": "EarningsPerShareBasic",
        "taxonomy": "us-gaap",
        "units": "USD-per-shares",
        "instant": False,
    },
    {"tag": "InventoryNet", "taxonomy": "us-gaap", "units": "USD", "instant": True},
    {
        "tag": "LiabilitiesCurrent",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
    {
        "tag": "NetIncomeLoss",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
    {
        "tag": "OperatingIncomeLoss",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": False,
    },
    {
        "tag": "StockholdersEquity",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
]

#: Company concepts that have high availability. Units are not in valid frame
#: formats.
common_concepts: list[Concept] = [frame_to_concept(frame) for frame in common_frames]


@ratelimit.guard([ratelimit.RequestLimit(9, timedelta(seconds=1))])
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
    response = session.get(url, headers={"User-Agent": user_agent})
    response.raise_for_status()
    return response


def get_cik(ticker: str, /, *, user_agent: None | str = None) -> str:
    """Return a ticker's SEC CIK."""
    if not _tickers_to_cik:
        response = get(Tickers.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        for _, items in content.items():
            normalized_cik = str(items["cik_str"]).zfill(10)
            _tickers_to_cik[items["ticker"]] = normalized_cik
            _cik_to_tickers[normalized_cik] = items["ticker"]
    return _tickers_to_cik[ticker.upper()]


def get_ticker(cik: str, /, *, user_agent: None | str = None) -> str:
    """Return an SEC CIK's ticker."""
    if not _cik_to_tickers:
        response = get(Tickers.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        for _, items in content.items():
            normalized_cik = str(items["cik_str"]).zfill(10)
            _cik_to_tickers[normalized_cik] = items["ticker"]
            _tickers_to_cik[items["ticker"]] = normalized_cik
    cik = str(cik).zfill(10)
    return _cik_to_tickers[cik]


@cache
def get_ticker_set(*, user_agent: None | str = None) -> set[str]:
    """Get the set of tickers that published data for popular concepts
    during any of the quarters for the previous year.

    This effectively gets the set of tickers whose data is at least
    somewhat available through the SEC EDGAR API.

    """
    year = datetime.now().year - 1
    tickers = set()

    for frame in common_frames:
        tag = frame["tag"]
        instant = frame["instant"]
        taxonomy = frame["taxonomy"]
        units = frame["units"]
        for quarter in range(1, 4):
            df = frames.get(
                tag,
                year,
                quarter,
                instant=instant,
                taxonomy=taxonomy,
                units=units,
                user_agent=user_agent,
            )
            for cik in df["cik"]:
                try:
                    ticker = get_ticker(cik)
                except KeyError:
                    continue
                tickers.add(ticker)
    return tickers
