"""An implementation of the Securities and Exchange Commission's (SEC) EDGAR API.

The SEC EDGAR API provides methods for retrieving XBRL data (e.g., earnings
per share) from financial statements and SEC filing submission histories
(e.g., 10-Q/10-K forms). The SEC EDGAR API is one of the few APIs that
provides historical and current company financial data for free.

An SEC EDGAR API user agent declaration is required to use this API.
The user agent should be of format ``FIRST_NAME LAST_NAME E_MAIL``. You
can pass your user agent directly to the implemented API getters, or you
can set the ``SEC_API_USER_AGENT`` environment variable to have the user agent
be passed to the implemented API getters for you.

Alternatively, running ``finagg sec install`` (or the broader
``finagg install``) will prompt you to enter an SEC EDGAR API user agent
and will automatically store it in an ``.env`` file in your current working
directory. The environment variables set in that ``.env`` file will be loaded
into your shell upon using ``finagg`` (whether that be through the Python
interface or through the CLI tools).

See the official `SEC EDGAR API docs`_ for more info on the SEC API.

.. _`SEC EDGAR API docs`: https://www.sec.gov/edgar/sec-api-documentation

"""

import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from functools import cache
from typing import Any, ClassVar, TypedDict
from zipfile import ZipFile

import pandas as pd
import requests
import requests_cache
from tqdm import tqdm

from .. import config, ratelimit, utils

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

if config.disable_http_cache:
    session = requests.Session()
else:
    session = requests_cache.CachedSession(
        str(config.http_cache_path),
        expire_after=timedelta(weeks=1),
    )


class Concept(TypedDict):
    """A collection of XBRL tag (data field), SEC EDGAR taxonomy that
    tag is in, and units for that tag.

    """

    #: Valid tag within the given `taxonomy`.
    tag: str

    #: Valid SEC EDGAR taxonomy.
    #: See https://www.sec.gov/info/edgar/edgartaxonomies.shtml for taxonomies.
    taxonomy: str

    #: Currency the concept is in.
    units: str


class Frame(Concept):
    """A frame is just a :class:`Concept` with a flag indicating if it supports
    "instantaneous" frame data.

    """

    #: Whether to retrieve "instant" calendrical results.
    instant: bool


def _frame_to_concept(frame: Frame, /) -> Concept:
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


class API(ABC):
    """Abstract SEC EDGAR API."""

    #: Request API URL.
    url: ClassVar[str]

    @classmethod
    @abstractmethod
    def get(cls, *args: Any, **kwargs: Any) -> pd.DataFrame | SubmissionsResult:
        """Main API getter method."""


class CompanyConcept(API):
    """Get all XBRL disclosures for a single company and concept (a taxonomy
    and tag) in a single dataframe.

    The module variable :data:`finagg.sec.api.company_concept` is an instance of
    this API implementation and is the most popular interface for querying this
    API.

    .. seealso::
        :data:`finagg.sec.api.popular_concepts`: For a list of popular company
            concepts that can be used with this method. The concepts described
            within this member are the most widely available concepts for
            SEC filers.

    """

    url = "https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{tag}.json"

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
        cache: bool = True,
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Return all XBRL disclosures for a single company (CIK)
        and concept (a taxonomy and tag) in a single dataframe.

        Args:
            tag: Valid tag within the given ``taxonomy``.
            cik: Company SEC CIK. Mutually exclusive with ``ticker``.
            ticker: Company ticker. Mutually exclusive with ``cik``.
            taxonomy: Valid SEC EDGAR taxonomy.
                See https://www.sec.gov/info/edgar/edgartaxonomies.shtml for taxonomies.
            units: Currency to view results in.
            cache: Whether to cache the response from the API.
            user_agent: Self-declared bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            Dataframe with normalized column names.

        Raises:
            `ValueError`: If both a ``cik`` and ``ticker`` are provided or
                neither are provided.

        Examples:
            >>> finagg.sec.api.company_concept.get(
            ...     "EarningsPerShareBasic",
            ...     ticker="AAPL",
            ...     taxonomy="us-gaap",
            ...     units="USD/shares",
            ... ).head(5)  # doctest: +SKIP
                    start         end    val      accession_number    fy  fp ...
            0  2006-10-01  2007-09-29   4.04  0001193125-09-214859  2009  FY ...
            1  2006-10-01  2007-09-29   4.04  0001193125-10-012091  2009  FY ...
            2  2007-09-30  2008-06-28   4.20  0001193125-09-153165  2009  Q3 ...
            3  2008-03-30  2008-06-28   1.21  0001193125-09-153165  2009  Q3 ...
            4  2007-09-30  2008-09-27   5.48  0001193125-09-214859  2009  FY ...

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`.")

        if ticker:
            cik = str(get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik, taxonomy=taxonomy, tag=tag)
        response = _get(url, cache=cache, user_agent=user_agent)
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
        return results

    @classmethod
    def get_multiple_original(
        cls,
        concepts: list[Concept],
        *,
        cik: None | str = None,
        ticker: None | str = None,
        form: None | str = None,
        start: None | str = None,
        end: None | str = None,
        cache: bool = True,
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Return original (not amended) XBRL disclosures for a single company
        using a set of company concepts.

        Args:
            concepts: Company concepts to retrieve.
            cik: Company SEC CIK. Mutually exclusive with ``ticker``.
            ticker: Company ticker. Mutually exclusive with ``cik``.
            form: SEC filing form type to include. ``"10-Q"`` is for quarterly
                filing forms and ``"10-K"`` is for annual filing forms. Ignored
                if left ``None``.
            start: The start date of the observation period. Defaults to the
                first recorded date.
            end: The end date of the observation period. Defaults to the
                last recorded date.
            cache: Whether to cache the response from the API.
            user_agent: Self-declared bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            Pivoted dataframe with a tag per column.

        Examples:
            >>> finagg.sec.api.company_concept.get_multiple_original(
            ...     finagg.sec.api.popular_concepts,
            ...     ticker="AAPL",
            ... ).head(5)  # doctest: +SKIP
                 fy  fp     tag         end           val ...
            0  2009  Q3  Assets  2009-06-27  4.814000e+10 ...
            1  2010  Q1  Assets  2009-12-26  5.392600e+10 ...
            2  2010  Q2  Assets  2010-03-27  5.705700e+10 ...
            3  2010  Q3  Assets  2010-06-26  6.472500e+10 ...
            4  2011  Q1  Assets  2010-12-25  8.674200e+10 ...

        """
        start = start or "1776-07-04"
        end = end or utils.today
        dfs = []
        for concept in concepts:
            tag = concept["tag"]
            taxonomy = concept["taxonomy"]
            units = concept["units"]
            df = cls.get(
                tag,
                cik=cik,
                ticker=ticker,
                taxonomy=taxonomy,
                units=units,
                cache=cache,
                user_agent=user_agent,
            )
            df = filter_original_filings(df, form=form, units=units)
            df = df[(df["filed"] >= start) & (df["filed"] <= end)]
            dfs.append(df)
        df = pd.concat(dfs)
        return df.astype({"fy": "int64"})


class CompanyFacts(API):
    """Get all XBRL disclosures for a single company.

    The module variable :data:`finagg.sec.api.company_facts` is an instance of
    this API implementation and is the most popular interface for querying this
    API.

    """

    url = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    @classmethod
    def download_zip(
        cls,
        /,
        *,
        chunk_size: int = 1_000_000,
        user_agent: None | str = None,
    ) -> ZipFile:
        """Download all XBRL disclosures for all companies in a zip file.

        A progress bar displays the download progress as the download could
        take a long time because the zip file is more than 1GB.
        The zip file is updated by the SEC at approximately 3am ET nightly.

        Args:
            chunk_size: Chunk size to stream and write the zip file with.
            user_agent: Self-declared bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            A zip file object representing the downloaded zip file.

        """
        response = _get(
            "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
            cache=False,
            stream=True,
            user_agent=user_agent,
        )
        dst = config.root_path / "findata" / "companyfacts.zip"
        pb = tqdm(
            total=int(response.headers.get("content-length", 0)),
            desc=f"Downloading companyfacts.zip to {dst}",
            position=0,
            leave=True,
            unit="iB",
            unit_scale=True,
        )
        with open(dst, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                pb.update(len(chunk))
                f.write(chunk)
        pb.close()
        return ZipFile(dst)

    @classmethod
    def get(
        cls,
        /,
        *,
        cik: None | str = None,
        ticker: None | str = None,
        cache: bool = True,
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Return all XBRL disclosures for a single company (CIK).

        Args:
            cik: Company SEC CIK. Mutually exclusive with `ticker`.
            ticker: Company ticker. Mutually exclusive with `cik`.
            cache: Whether to cache the response from the API.
            user_agent: Self-declared bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            Dataframe with normalized column names.

        Raises:
            `ValueError`: If both a ``cik`` and ``ticker`` are provided or
                neither are provided.

        Examples:
            >>> finagg.sec.api.company_facts.get(ticker="AAPL").head(5)  # doctest: +SKIP
                      end         val      accession_number    fy  fp    form ...
            0  2009-06-27  8.9582e+08  0001193125-09-153165  2009  Q3    10-Q ...
            1  2009-10-16  9.0068e+08  0001193125-09-214859  2009  FY    10-K ...
            2  2009-10-16  9.0068e+08  0001193125-10-012091  2009  FY  10-K/A ...
            3  2010-01-15  9.0679e+08  0001193125-10-012085  2010  Q1    10-Q ...
            4  2010-04-09  9.0994e+08  0001193125-10-088957  2010  Q2    10-Q ...

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`.")

        if ticker:
            cik = str(get_cik(ticker))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = _get(url, cache=cache, user_agent=user_agent)
        content = response.json()
        df = _parse_company_facts(content)
        df["cik"] = cik
        return df


class Exchanges(API):
    """SEC-registered ticker info with exchange data.

    This API implementation is very similar to :class:`Tickers`. It returns
    all the same data in addition to the exchanges that each company is on.

    The module variable :data:`finagg.sec.api.exchanges` is an instance of this
    API implementation and is the most popular interface for querying this
    API.

    """

    url = "https://www.sec.gov/files/company_tickers_exchange.json"

    @classmethod
    def get(cls, *, cache: bool = True, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing all SEC-registered ticker
        info.

        Args:
            cache: Whether to cache the response from the API.
            user_agent: Self-declared SEC bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            A dataframe containing company names, their SEC CIKs, and their
            ticker symbols.

        Examples:
            >>> finagg.sec.api.exchanges.get().head(5)  # doctest: +SKIP
                   cik            name ticker exchange
            0   320193      Apple Inc.   AAPL   Nasdaq
            1   789019  MICROSOFT CORP   MSFT   Nasdaq
            2  1652044   Alphabet Inc.  GOOGL   Nasdaq
            3  1018724  AMAZON COM INC   AMZN   Nasdaq
            4  1045810     NVIDIA CORP   NVDA   Nasdaq

        """
        response = _get(cls.url, cache=cache, user_agent=user_agent)
        content: dict[str, list[str]] = response.json()
        df = pd.DataFrame(content["data"], columns=content["fields"])
        return df.rename(columns={"cik_str": "cik"})


class Frames(API):
    """Get all company filings for one particular fact that most closely
    matches the requested calendrical period.

    The module variable :data:`finagg.sec.api.frames` is an instance of
    this API implementation and is the most popular interface for querying this
    API.

    .. seealso::
        :data:`popular_frames`: For a list of popular company frames
            that can be used with this method. The frames described
            within this member are the most widely available frames for
            SEC filers.

    """

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
        cache: bool = True,
        user_agent: None | str = None,
    ) -> pd.DataFrame:
        """Get one fact for each reporting entity that most closely fits
        the calendrical period requested.

        Args:
            tag: Valid tag within the given ``taxonomy``.
            year: Year to retrieve.
            quarter: Quarter to retrieve data for. Most data is only provided
                at a quarterly rate, so this should be provided for most cases.
            instant: Whether to get instantaneous data for the frame (data that
                most closely matches a frame's year and quarter without a
                time buffer). This flag should be enabled for most cases. See
                :data:`popular_frames` for which tags should be ``instant``.
            taxonomy: Valid SEC EDGAR taxonomy.
                See https://www.sec.gov/info/edgar/edgartaxonomies for taxonomies.
            units: Units to view results in.
            cache: Whether to cache the response from the API.
            user_agent: Self-declared bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            Dataframe with slightly improved column names.

        Examples:
            >>> finagg.sec.api.frames.get(
            ...     "EarningsPerShareBasic",
            ...     2020,
            ...     quarter=3,
            ...     instant=False,
            ...     taxonomy="us-gaap",
            ...     units="USD-per-shares",
            ... ).head(5)  # doctest: +SKIP
                   accession_number   cik                      entityName    loc ...
            0  0001104659-21-118843  1750                       AAR CORP.  US-IL ...
            1  0001104659-21-133629  1800             ABBOTT LABORATORIES  US-IL ...
            2  0001264931-20-000235  1961                     WORLDS INC.  US-MA ...
            3  0001564590-21-055818  2098                ACME UNITED CORP  US-CT ...
            4  0000002178-22-000033  2178  ADAMS RESOURCES & ENERGY, INC.  US-TX ...

        """
        if quarter:
            quarter = f"Q{quarter}"
            if instant:
                quarter = f"{quarter}I"
        else:
            quarter = ""
        units = "-per-".join(units.split("/"))
        url = cls.url.format(
            taxonomy=taxonomy, tag=tag, units=units, year=year, quarter=quarter
        )
        response = _get(url, cache=cache, user_agent=user_agent)
        content = response.json()
        data = content.pop("data")
        df = pd.DataFrame(data)
        for k, v in content.items():
            df[k] = v
        return df


class Submissions(API):
    """Get a company's metadata and all its recent SEC filings.

    Not all the metadata typically found with the submissions API is supported
    by this API. Only common company metadata (e.g., company name, industry
    code, fiscal year end date, etc.) is returned by this implementation.

    The module variable :data:`finagg.sec.api.submissions` is an instance of
    this API implementation and is the most popular interface for querying this
    API.

    """

    url = "https://data.sec.gov/submissions/CIK{cik}.json"

    @classmethod
    def download_zip(
        cls,
        /,
        *,
        chunk_size: int = 1_000_000,
        user_agent: None | str = None,
    ) -> ZipFile:
        """Download all SEC filings for all companies in a zip file.

        A progress bar displays the download progress as the download could
        take a long time because the zip file is more than 1GB.
        The zip file is updated by the SEC at approximately 3am ET nightly.

        Args:
            chunk_size: Chunk size to stream and write the zip file with.
            user_agent: Self-declared bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            A zip file object representing the downloaded zip file.

        """
        response = _get(
            "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip",
            cache=False,
            stream=True,
            user_agent=user_agent,
        )
        dst = config.root_path / "findata" / "submissions.zip"
        pb = tqdm(
            total=int(response.headers.get("content-length", 0)),
            desc=f"Downloading submissions.zip to {dst}",
            position=0,
            leave=True,
            unit="iB",
            unit_scale=True,
        )
        with open(dst, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                pb.update(len(chunk))
                f.write(chunk)
        pb.close()
        return ZipFile(dst)

    @classmethod
    def get(
        cls,
        /,
        *,
        cik: None | str = None,
        ticker: None | str = None,
        cache: bool = True,
        user_agent: None | str = None,
    ) -> SubmissionsResult:
        """Return a company's metadata and all its recent SEC filings.

        Args:
            cik: Company SEC CIK. Mutually exclusive with ``ticker``.
            ticker: Company ticker. Mutually exclusive with ``cik``.
            cache: Whether to cache the response from the API.
            user_agent: Self-declared SEC bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            Company metadata and a dataframe with recent SEC filings.

        Raises:
            `ValueError`: If both a ``cik`` and ``ticker`` are provided or neither
                are provided.

        Examples:
            >>> out = finagg.sec.api.submissions.get(ticker="AAPL")
            >>> out["metadata"]  # doctest: +ELLIPSIS
            {'cik': '0000320193', 'entityType': 'operating', 'sic': '3571', ...}

        """
        if bool(cik) == bool(ticker):
            raise ValueError("Must provide a `cik` or a `ticker`.")

        if ticker:
            cik = str(get_cik(ticker))

        if cik:
            ticker = str(get_ticker(cik))

        cik = str(cik).zfill(10)
        url = cls.url.format(cik=cik)
        response = _get(url, cache=cache, user_agent=user_agent)
        content = response.json()
        recent_filings = content.pop("filings")["recent"]
        df = pd.DataFrame(recent_filings)
        df["cik"] = cik
        metadata = _parse_submission_metadata(content)
        metadata["cik"] = cik
        metadata["ticker"] = str(ticker)
        return {"metadata": metadata, "filings": df}


class Tickers(API):
    """SEC-registered ticker info.

    This is a broader method in comparison to :meth:`get_ticker_set`.
    :meth:`get_ticker_set` will get all the tickers that have popular
    fundamentals data available through the SEC EDGAR API for the
    previous year, while this method will get all tickers and CIKs
    that have ever had data available via the SEC EDGAR API.

    The module variable :data:`finagg.sec.api.tickers` is an instance of this
    API implementation and is the most popular interface for querying this
    API.

    """

    url = "https://www.sec.gov/files/company_tickers.json"

    @classmethod
    def get(cls, *, cache: bool = True, user_agent: None | str = None) -> pd.DataFrame:
        """Get a dataframe containing all SEC-registered ticker
        info.

        Args:
            cache: Whether to cache the response from the API.
            user_agent: Self-declared SEC bot header. Defaults to the value
                found in the ``SEC_API_USER_AGENT`` environment variable.

        Returns:
            A dataframe containing company names, their SEC CIKs, and their
            ticker symbols.

        Examples:
            >>> finagg.sec.api.tickers.get().head(5)  # doctest: +SKIP
                   cik ticker                   title
            0   320193   AAPL              Apple Inc.
            1   789019   MSFT          MICROSOFT CORP
            2  1652044  GOOGL           Alphabet Inc.
            3  1018724   AMZN          AMAZON COM INC
            4  1067983  BRK-B  BERKSHIRE HATHAWAY INC

        """
        response = _get(cls.url, cache=cache, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        df = pd.DataFrame([items for _, items in content.items()])
        return df.rename(columns={"cik_str": "cik"})


# Mapping of SEC CIK strings to (uppercase) tickers.
_cik_to_tickers: dict[str, str] = {}

# Mapping of (uppercase) tickers to SEC CIK strings.
_tickers_to_cik: dict[str, str] = {}

company_concept = CompanyConcept()
"""The most popular way for accessing the :class:`CompanyConcept` API
implementation.

:meta hide-value:
"""

company_facts = CompanyFacts()
"""The most popular way for accessing the :class:`CompanyFacts` API
implementation.

:meta hide-value:
"""

exchanges = Exchanges()
"""The most popular way for accessing the :class:`Exchanges` API implementation.

:meta hide-value:
"""

frames = Frames()
"""The most popular way for accessing the :class:`Frames` API implementation.

:meta hide-value:
"""

submissions = Submissions()
"""The most popular way for accessing the :class:`Submissions` API
implementation.

:meta hide-value:
"""

tickers = Tickers()
"""The most popular way for accessing the :class:`Tickers` API implementation.

:meta hide-value:
"""

popular_frames: list[Frame] = [
    {
        "tag": "Assets",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
    {
        "tag": "AssetsCurrent",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
    {
        "tag": "CommonStockSharesOutstanding",
        "taxonomy": "us-gaap",
        "units": "shares",
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
        "tag": "Liabilities",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
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
        "tag": "StockholdersEquity",
        "taxonomy": "us-gaap",
        "units": "USD",
        "instant": True,
    },
]
"""Company frames that have high availability and are relatively popular
for fundamental analysis. Includes things like earnings per share, current
assets, etc.. Frames are in valid format for usage with the :class:`Frames`
API implementation.

:meta hide-value:
"""

popular_concepts: list[Concept] = [_frame_to_concept(frame) for frame in popular_frames]
"""Company concepts that have high availability and are relatively popular
for fundamental analysis. Includes things like earnings per share, current
assets, etc.. Concepts contain all the parameters required for usage with the
:class:`CompanyConcept` API implementation.

:meta hide-value:
"""


@ratelimit.guard([ratelimit.RequestLimit(9, timedelta(seconds=1))])
def _get(
    url: str,
    /,
    *,
    stream: bool = False,
    cache: bool = True,
    user_agent: None | str = None,
) -> requests.Response:
    """SEC EDGAR API request helper.

    Args:
        url: Complete SEC EDGAR API URL to request from.
        stream: Whether to stream the response content (useful for file
            downloads).
        cache: Whether to cache the response from the given URL.
        user_agent: Self-declared SEC bot header. Defaults to the value
            found in the ``SEC_API_USER_AGENT`` environment variable.

    Returns:
        Successful responses.

    Raises:
        `RuntimeError`: If a user agent is not provided or found in the environment.

    """
    user_agent = user_agent or os.environ.get("SEC_API_USER_AGENT", None)
    if not user_agent:
        raise RuntimeError(
            "No SEC API user agent declaration found. "
            "Pass your user agent declaration to the API directly, or "
            "set the `SEC_API_USER_AGENT` environment variable."
        )
    headers = {"User-Agent": user_agent}
    if cache:
        response = session.get(url, headers=headers, stream=stream)
    else:
        response = requests.get(url, headers=headers, stream=stream)
    response.raise_for_status()
    return response


def compute_financial_ratios(df: pd.DataFrame, /) -> pd.DataFrame:
    """Compute financial ratios in-place for a dataframe using XBRL financial
    tags.

    Args:
        df: Dataframe with common XBRL financial tags (e.g., "Assets",
            "Liabilities", etc.).

    Returns:
        ``df`` but with additional columns that're normalized financial ratios.

    """
    df["AssetCoverageRatio"] = (df["Assets"] - df["LiabilitiesCurrent"]) / df[
        "Liabilities"
    ]
    df["BookRatio"] = (df["Assets"] - df["Liabilities"]) / df[
        "CommonStockSharesOutstanding"
    ]
    df["DebtEquityRatio"] = df["Liabilities"] / df["StockholdersEquity"]
    df["QuickRatio"] = (df["AssetsCurrent"] - df["InventoryNet"]) / df[
        "LiabilitiesCurrent"
    ]
    df["ReturnOnAssets"] = df["NetIncomeLoss"] / df["Assets"]
    df["ReturnOnEquity"] = df["NetIncomeLoss"] / df["StockholdersEquity"]
    df["WorkingCapitalRatio"] = df["AssetsCurrent"] / df["LiabilitiesCurrent"]
    return df


def filter_original_filings(
    df: pd.DataFrame, /, *, form: None | str = None, units: None | str = None
) -> pd.DataFrame:
    """Get all original filing rows as determined by the filing date and
    tag for a period.

    Args:
        df: Dataframe without mixed original and amendment rows.
        form: Only keep rows with form type ``form``. Most popular choices
            include ``"10-K"`` for annual and ``"10-Q"`` for quarterly.
            This parameter is ignored if left ``None``.
        units: Only keep rows with units ``units`` if not ``None``.

    Returns:
        Dataframe with rows that only correspond to original filings.

    Examples:
        Only get a company's original quarterly earnings-per-share filings.

        >>> df = finagg.sec.api.company_concept.get(
        ...     "EarningsPerShareBasic",
        ...     ticker="AAPL",
        ...     taxonomy="us-gaap",
        ...     units="USD/shares",
        ... )
        >>> finagg.sec.api.filter_original_filings(
        ...     df,
        ...     form="10-Q",
        ...     units="USD/shares"
        ... ).head(5)  # doctest: +SKIP
             fy  fp  ...
        0  2009  Q3  ...
        1  2010  Q1  ...
        2  2010  Q2  ...
        3  2010  Q3  ...
        4  2011  Q1  ...

    """
    end = pd.DatetimeIndex(df["end"])
    filed = pd.DatetimeIndex(df["filed"])
    filing_delay = filed - end
    # Make sure filings occurs within 90 days of the reporting end date.
    # Helps ensure each filing is the first filing and not an amendment.
    mask = filing_delay.days <= 90
    if form:
        mask &= df["form"] == form
        # Not all filings contain a start date, but it can be helpful to
        # use the start date to ensure the filing corresponds to the time
        # period we care about.
        if "start" in df:
            start: pd.DatetimeIndex = pd.DatetimeIndex(df["start"])
            start_to_end = end - start
        match form:
            case "10-K":
                mask &= df["fp"] == "FY"
                # Make sure the reporting frame is close to a year.
                if "start" in df:
                    mask &= start.isna() | ((350 <= start_to_end.days) & (start_to_end.days <= 380))  # type: ignore[no-untyped-call]
            case "10-Q":
                mask &= df["fp"].str.startswith("Q")
                # Make sure the reporting frame is close to a quarter.
                if "start" in df:
                    mask &= start.isna() | ((75 <= start_to_end.days) & (start_to_end.days <= 105))  # type: ignore[no-untyped-call]
    if units:
        mask &= df["units"] == units
    df = df[mask]
    return (
        df.sort_values(["fy", "fp", "filed"])
        .groupby(["fy", "fp", "tag"], as_index=False)
        .first()
    )


def get_cik(ticker: str, /, *, user_agent: None | str = None) -> str:
    """Return a company's SEC CIK from its ticker.

    Args:
        ticker: A company's ticker symbol.
        user_agent: Self-declared SEC bot header. Defaults to the value
            found in the ``SEC_API_USER_AGENT`` environment variable.

    Returns:
        The company's corresponding SEC CIK.

    Examples:
        Get Apple's SEC CIK from its ticker.

        >>> finagg.sec.api.get_cik("AAPL") == "0000320193"
        True

    """
    if not _tickers_to_cik:
        response = _get(Tickers.url, user_agent=user_agent)
        content: dict[str, dict[str, str]] = response.json()
        for _, items in content.items():
            normalized_cik = str(items["cik_str"]).zfill(10)
            _tickers_to_cik[items["ticker"]] = normalized_cik
            _cik_to_tickers[normalized_cik] = items["ticker"]
    return _tickers_to_cik[ticker.upper()]


def get_ticker(cik: str, /, *, user_agent: None | str = None) -> str:
    """Return a company's ticker from its SEC CIK.

    Args:
        cik: A company's 10-character SEC CIK.
        user_agent: Self-declared SEC bot header. Defaults to the value
            found in the ``SEC_API_USER_AGENT`` environment variable.

    Returns:
        The company's corresponding ticker.

    Examples:
        Get Apple's ticker from its SEC CIK.

        >>> finagg.sec.api.get_ticker("0000320193") == "AAPL"
        True

    """
    if not _cik_to_tickers:
        response = _get(Tickers.url, user_agent=user_agent)
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

    Args:
        user_agent: Self-declared SEC bot header. Defaults to the value
            found in the ``SEC_API_USER_AGENT`` environment variable.

    Returns:
        Set of tickers whose data is at least somewhat available through
        the SEC EDGAR API.

    Examples:
        >>> tickers = finagg.sec.api.get_ticker_set()
        >>> "AAPL" in tickers
        True
        >>> "MSFT" in tickers
        True
        >>> "GOOG" in tickers
        True

    """
    year = datetime.now().year - 1
    tickers = set()

    for frame in popular_frames:
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


def group_and_pivot_filings(
    df: pd.DataFrame, /, *, form: None | str = None
) -> pd.DataFrame:
    """Helper for grouping filings into a pivoted dataframe such that each
    tag has its own column.

    Tags are grouped according to fiscal year and fiscal period. The newest
    filing date is used when tags have different filing dates for the same
    fiscal year and fiscal period. Tags are forward-filled to fill gaps
    in filings.

    Args:
        df: "Melted" dataframe where each filing is a row.
        form: Type of form to group. ``"10-K"`` sets the index to the fiscal
            year and filing date whereas ``"10-Q"`` sets the index to the
            fiscal year, fiscal period, and filing date. If not provided,
            the form type is chosen by inspecting the first element of the
            ``"form"`` column.

    Returns:
        A pivoted dataframe where each column is a tag.

    Examples:
        Get all XBRL filings from a company, unintelligently select the first
        unique filings for each XBRL tag, and then group all filings into
        a pivoted table.

        >>> df = finagg.sec.api.company_facts.get(ticker="AAPL")
        >>> df = finagg.sec.api.filter_original_filings(df, form="10-K")
        >>> finagg.sec.api.group_and_pivot_filings(df, form="10-K")  # doctest: +SKIP
                         AccountsPayableCurrent  AccountsReceivableNetCurrent ...
        fy   filed                                                            ...
        2009 2009-10-27            5.520000e+09                  2.422000e+09 ...
        2010 2010-10-27            5.601000e+09                  3.361000e+09 ...
        2011 2011-10-26            1.201500e+10                  5.510000e+09 ...
        2012 2012-10-31            1.463200e+10                  5.369000e+09 ...
        2013 2013-10-30            2.117500e+10                  1.093000e+10 ...

    """
    if form is None:
        form = df.iloc[0]["form"]
    match form:
        case "10-K":
            df = df.drop(columns=["fp"]).set_index(["fy"]).sort_index()
            df["filed"] = df.groupby(["fy"])["filed"].max()
            df = df.reset_index().pivot(
                index=["fy", "filed"],
                columns="tag",
                values="val",
            )
        case "10-Q":
            df = df.set_index(["fy", "fp"]).sort_index()
            df["filed"] = df.groupby(["fy", "fp"])["filed"].max()
            df = df.reset_index().pivot(
                index=["fy", "fp", "filed"],
                columns="tag",
                values="val",
            )
    df.columns = df.columns.rename(None)
    return df


def _parse_company_facts(content: dict[str, Any], /) -> pd.DataFrame:
    """Helper for parsing company facts.

    This function is only defined to make parsing company
    facts easier and common between parsing from the REST
    API responses and the bulk zip file.

    Args:
        content: Company facts JSON data.

    Returns:
        A dataframe equivalent of the company facts data.

    """
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
    return results


def _parse_submission_metadata(content: dict[str, Any], /) -> dict[str, Any]:
    """Helper for parsing submission metadata.

    This function is only defined to make parsing submission
    metadata easier and common between parsing from the REST
    API responses and the bulk zip file.

    Args:
        content: Submissions JSON data.

    Returns:
        A dataframe equivalent of the submissions data.

    """
    metadata = {}
    for k, v in content.items():
        if isinstance(v, str):
            metadata[k] = utils.snake_case(v)
    if "exchanges" in content:
        metadata["exchanges"] = ",".join(content["exchanges"])
    return metadata
