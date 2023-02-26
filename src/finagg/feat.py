"""finagg features interfaces."""

from typing import Any, Protocol

import pandas as pd


class Features:
    columns: list[str]

    @classmethod
    def pct_change_source_columns(cls) -> list[str]:
        """Return the names of columns used for computed percent change
        columns.

        """
        return [
            col.removesuffix("_pct_change")
            for col in cls.columns
            if col.endswith("_pct_change")
        ]

    @classmethod
    def pct_change_target_columns(cls) -> list[str]:
        """Return the names of computed percent change columns."""
        return [col for col in cls.columns if col.endswith("_pct_change")]


class SupportsFromAPI(Protocol):
    @classmethod
    def from_api(cls, *args: Any, **kwargs: Any) -> pd.DataFrame:
        ...


class SupportsFromOtherRefined(Protocol):
    @classmethod
    def from_other_refined(cls, *args: Any, **kwargs: Any) -> pd.DataFrame:
        ...


class SupportsFromRaw(Protocol):
    @classmethod
    def from_raw(cls, *args: Any, **kwargs: Any) -> pd.DataFrame:
        ...


class SupportsFromRefined(Protocol):
    @classmethod
    def from_refined(cls, *args: Any, **kwargs: Any) -> pd.DataFrame:
        ...


class SupportsInstall(Protocol):
    @classmethod
    def install(cls, *args: Any, **kwargs: Any) -> int:
        ...


class SupportsToRefined(Protocol):
    @classmethod
    def to_refined(cls, *args: Any, **kwargs: Any) -> int:
        ...
