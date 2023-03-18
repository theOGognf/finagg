"""Generic utils used by subpackages."""

import csv
import os
import pathlib
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import set_key


def CamelCase(s: str, /) -> str:
    """Transform a string to CamelCase.

    Credit:
        https://stackoverflow.com/a/1176023

    Args:
        s: Any string.

    Returns:
        A string in CamelCase format.

    Examples:
        >>> finagg.utils.CamelCase("snakes_are_dope") == "SnakesAreDope"
        True
        >>> finagg.utils.CamelCase("bar") == "Bar"
        True

    """
    return "".join(word.title() for word in s.split("_"))


def expand_tickers(tickers: str | list[str], /) -> set[str]:
    """Expand the given list of tickers into a set of tickers, where each value
    in the list of tickers could be:

        1. Comma-separated tickers
        2. A path that points to a CSV file containing tickers
        3. A regular ol' ticker

    Args:
        tickers: List of strings denoting tickers, comma-separated tickers,
            or CSV files containing tickers.

    Returns:
        A set of all tickers found within the given list.

    Examples:
        >>> ts = finagg.utils.expand_tickers(["AAPL,MSFT"])
        >>> "AAPL" in ts
        True

    """
    if isinstance(tickers, str):
        tickers = [tickers]

    out = set()
    for ticker_csv in tickers:
        ticker_list = ticker_csv.split(",")
        for ticker in ticker_list:
            ticker_path = Path(ticker)
            if ticker_path.exists():
                with open(ticker_path, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        out.update(row)
            else:
                out.add(ticker)
    return out


def join_with(s: str | list[str], /, delim: str) -> str:
    """Join a sequence of strings with the delimiter ``delim``.

    Examples:
        >>> finagg.utils.join_with(["foo", "bar"], ",") == "foo,bar"
        True
        >>> finagg.utils.join_with("foo", ",") == "foo"
        True

    """
    if isinstance(s, str):
        s = [s]
    return delim.join(s)


def safe_pct_change(series: pd.Series, /) -> pd.Series:  # type: ignore
    """Safely compute percent change on a column.

    Replaces ``Inf`` values with ``NaN`` and forward-fills.
    This function is meant to be used with ``pd.Series.apply``.

    Args:
        series: Series of values.

    Returns:
        A series representing percent changes of ``col``.

    """
    return (
        series.pct_change()
        .replace([-np.inf, np.inf], np.nan)
        .fillna(method="ffill")
        .dropna()
    )


def setenv(name: str, value: str, /, *, exist_ok: bool = False) -> pathlib.Path:
    """Set the value of the environment variable ``name`` to ``value``.

    The environment variable is permanently set in the environment
    and in the current process.

    Args:
        name: Environment variable name.
        value: Environment variable value.
        exist_ok: Whether it's okay if an environment variable of the
            same name already exists. If ``True``, it will be overwritten.

    Returns:
        Path to the file the environment variable was written to.

    Raises:
        `RuntimeError`: If ``exist_ok`` is ``False`` and an environment variable
            of the same name already exists.

    """
    if not exist_ok and name in os.environ:
        raise RuntimeError(
            f"The env variable `{name}` already exists. "
            "Set `exist_ok` to `True` to overwrite it."
        )

    os.environ[name] = value
    dotenv = pathlib.Path.cwd() / ".env"
    set_key(str(dotenv), name, value)
    return dotenv


def snake_case(s: str, /) -> str:
    """Transform a string to snake_case.

    Credit:
        https://stackoverflow.com/a/1176023

    Args:
        s: Any string.

    Returns:
        A string in snake_case format.

    Examples:
        >>> finagg.utils.snake_case("CamelsAreCool") == "camels_are_cool"
        True
        >>> finagg.utils.snake_case("Foo") == "foo"
        True

    """
    s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub("__([A-Z])", r"_\1", s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


today = datetime.today().strftime("%Y-%m-%d")
"""Today's date. Used by a number of submodules as the default end date
when getting data from APIs or SQL tables.

:meta hide-value:
"""
