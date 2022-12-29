"""Generic utils used by subpackages."""

import os
import pathlib
import re

import numpy as np
import pandas as pd


def CamelCase(s: str, /) -> str:
    """Convert a string to CamelCase.

    Credit:
        https://stackoverflow.com/a/1176023

    Examples:
        >>> CamelCase("snakes_are_dope")
        "SnakesAreDope"

        >>> CamelCase("bar")
        "Bar"

    """
    return "".join(word.title() for word in s.split("_"))


def join_with(s: str | list[str], /, delim: str) -> str:
    """Join a sequence of strings with the delimiter `delim`.

    Examples:
        >>> join_with(["foo", "bar"], ",")
        "foo,bar"

        >>> join_with("foo")
        "foo"

    """
    if isinstance(s, str):
        s = [s]
    return delim.join(s)


def quantile_clip(
    df: pd.DataFrame, /, *, lower: float = 0.01, upper: float = 0.99
) -> pd.DataFrame:
    """Clip dataframe values to be within the specified quantiles.

    Args:
        df: Dataframe to clip.
        lower: Lower bound quantile.
        upper: Upper bound quantile.

    Returns:
        A dataframe whose values are within the quantiles
        specified by `lower` and `upper`.

    """
    # Lower quantile clipping
    df = df.replace([-np.inf], np.nan)
    df_q_lower = df.quantile(lower, numeric_only=True)
    df = df.clip(lower=df_q_lower, axis=1)  # type: ignore
    df = df.fillna(method="ffill")
    # Upper quantile clipping
    df = df.replace([np.inf], np.nan)
    df_q_upper = df.quantile(upper, numeric_only=True)
    df = df.clip(upper=df_q_upper, axis=1)  # type: ignore
    df = df.fillna(method="ffill")
    return df


def safe_pct_change(col: pd.Series) -> pd.Series:
    """Safely compute percent change on a column.

    Replaces Inf values with NaN and forward-fills.
    This function is meant to be used with
    `pd.Series.apply`.

    Args:
        col: Series of values.

    Returns:
        A series representing percent changes of `col`.

    """
    return (
        col.pct_change()
        .replace([-np.inf, np.inf], np.nan)
        .fillna(method="ffill")
        .dropna()
    )


def setenv(name: str, value: str, /, *, exist_ok: bool = False) -> pathlib.Path:
    """Set the value of the environment variable `name` to `value`.

    The environment variable is permanently set in the environment
    and in the current process.

    Args:
        name: Environment variable name.
        value: Environment variable value.
        exist_ok: Whether it's okay if an environment variable of the
            same name already exists. If `True`, it will be overwritten.

    Returns:
        Path to the file the environment variable was written to.

    Raises:
        RuntimeError if `exist_ok` is `False` and an environment variable
            of the same name already exists

    """
    if not exist_ok and name in os.environ:
        raise RuntimeError(
            f"The env variable `{name}` already exists. "
            "Set `exist_ok` to `True` to overwrite it."
        )

    os.environ[name] = value
    dotenv = pathlib.Path(__file__).parent.parent.parent / ".env"
    with open(dotenv, "a+") as env_file:
        env_file.write(f"{name}={value}\n")
    return dotenv


def snake_case(s: str, /) -> str:
    """Convert a string to snake_case.

    Credit:
        https://stackoverflow.com/a/1176023

    Examples:
        >>> snake_case("CamelsAreCool")
        "camels_are_cool"

        >>> snake_case("Foo")
        "foo"

    """
    s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub("__([A-Z])", r"_\1", s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()
