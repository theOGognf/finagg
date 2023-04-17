"""Generic utils used by subpackages."""

import csv
import os
import pathlib
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy as sa
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


def expand_csv(values: str | list[str], /) -> set[str]:
    """Expand the given list of strings into a set of strings, where each value
    in the list of strings could be:

        1. Comma-separated values
        2. A path that points to a CSV file containing values
        3. A regular ol' string

    Args:
        values: List of strings denoting comma-separated values,
            or CSV files containing comma-separated values.

    Returns:
        A set of all strings found within the given list.

    Examples:
        >>> ts = finagg.utils.expand_csv(["AAPL,MSFT"])
        >>> "AAPL" in ts
        True

    """
    if isinstance(values, str):
        values = [values]

    out = set()
    for vstring in values:
        vlist = vstring.split(",")
        for v in vlist:
            csv_path = Path(v)
            if csv_path.exists():
                with open(csv_path, "r") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        out.update(row)
            else:
                out.add(v)
    return out


def get_func_cols(table: sa.Table, /) -> list[str]:
    """Return the column names in ``table`` that have the format
    ``FUNC(arg0, arg1, ...)``.

    Args:
        table: SQLAlchemy table.

    Returns:
        List of functional-style column names in ``table``. Returns
        an empty list if none are found.

    """
    return [key for key in table.columns.keys() if parse_func_call(key)]


def parse_func_call(s: str, /) -> None | tuple[str, list[str]]:
    """Parse a function's name and its arguments' names from a string of format
    ``FUNC(arg0, arg1, ...)``.

    Args:
        s: Any string of format ``FUNC(arg0, arg1, ...)``.

    Returns:
        A tuple containing the parsed function's name and its arguments' names.
        Returns ``None`` if the string doesn't match the expected format.

    Examples:
        >>> finagg.utils.parse_func_call("LOG_CHANGE(high, open)")
        ('LOG_CHANGE', ['high', 'open'])

    """
    match = re.match(r"(\w+)\((.*)\)", s)
    if not match:
        return None
    name, args = match.groups()
    return name, args.replace(" ", "").split(",")


def resolve_col_order(
    table: sa.Table, df: pd.DataFrame, /, *, extra_ignore: None | list[str] = None
) -> pd.DataFrame:
    """Reorder the columns in ``df`` to match the order of the columns in
    ``table``.

    Args:
        table: SQLAlchemy table that defines the column order. Primary keys
            are ignored from the column order as they're assumed to be used
            as part of the index in ``df``.
        df: Dataframe to reorder.
        extra_ignore: Extra columns to ignore in the reordering. Sometimes
            columns aren't used as primary keys but are used as part of the
            index in the dataframe. Those columns should be provided in this
            option.

    Returns:
        Dataframe with columns ordered according to the column order in
        ``table``.

    """
    if extra_ignore is None:
        extra_ignore = []
    ignore_keys = {col.key for col in table.primary_key}
    ignore_keys.update(extra_ignore)
    column_order = [key for key in table.columns.keys() if key not in ignore_keys]
    return df[column_order]


def resolve_func_cols(
    table: sa.Table, df: pd.DataFrame, /, *, drop: bool = False, inplace: bool = False
) -> pd.DataFrame:
    """Inspect ``table`` and apply functions to columns that exist in ``table``
    and ``df`` according to columns named like ``FUNC(col0, col1, ...)``
    within ``table`` such that new columns in ``df`` are the result of the
    applied functions and have names matching the function call signatures.

    Args:
        table: SQLAchemy table that defines a superset of columns that
            should exist in ``df``.
        df: Dataframe that contains a subset of columns within ``table``
            that will be updated with columns defined by ``table`` that
            have names like ``FUNC(col0, col1, ...)``.
        drop: Whether to drop all other columns on the returned dataframe
            except for the columns in ``table``.
        inplace: Whether to perform operations in-place and use ``df``
            as the output dataframe.

    Returns:
        A new dataframe with columns from ``df`` and columns according to
        columns named within ``table`` like ``FUNC(col0, col1, ...)`` where
        columns ``col0`` and ``col1`` exist in ``df``.

    """
    out = df if inplace else df.copy(deep=True)
    primary_keys = {col.key for col in table.primary_key}
    other_keys = [key for key in table.columns.keys() if key not in primary_keys]
    func_keys = set()
    for key in other_keys:
        if func_call := parse_func_call(key):
            func_keys.add(key)
            name, args = func_call
            cols = map(out.get, args)
            match name:
                case "LOG_CHANGE":
                    out[key] = safe_log_change(*cols)  # type: ignore[arg-type]
                case "PCT_CHANGE":
                    out[key] = safe_pct_change(*cols)  # type: ignore[arg-type]
                case _:
                    raise ValueError(f"{key} is not supported")
    if inplace and drop:
        out.drop(columns=list(set(out.columns) - set(other_keys)), inplace=True)
    elif drop:
        out = out.drop(columns=list(set(out.columns) - set(other_keys)))
    return out


def safe_log_change(series: pd.Series, other: None | pd.Series = None) -> pd.Series:  # type: ignore[type-arg]
    """Safely compute log change between two columns.

    Replaces ``Inf`` values with ``NaN`` and forward-fills.
    This function is meant to be used with ``pd.Series.apply``.

    Args:
        series: Series of values.
        other: Reference series to compute change against. Defaults to
            ``series`` shifted forward one index.

    Returns:
        A series representing percent changes of ``col``.

    """
    if other is None:
        other = series.shift(1)

    out = series.apply(np.log) - other.apply(np.log)
    return out.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")


def safe_pct_change(series: pd.Series, other: None | pd.Series = None) -> pd.Series:  # type: ignore[type-arg]
    """Safely compute percent change between two columns.

    Replaces ``Inf`` values with ``NaN`` and forward-fills.
    This function is meant to be used with ``pd.Series.apply``.

    Args:
        series: Series of values.
        other: Reference series to compute change against. Defaults to
            ``series`` shifted forward one index.

    Returns:
        A series representing percent changes of ``col``.

    """
    if other is None:
        other = series.shift(1)

    out = (series - other) / other
    return out.replace([-np.inf, np.inf], np.nan).fillna(method="ffill")


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
