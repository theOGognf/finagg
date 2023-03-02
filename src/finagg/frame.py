"""`datetime` but for fiscal frames (quarter and year pairs)."""

import re
from dataclasses import dataclass
from typing import Union


def is_valid_fiscal_seq(seq: list[int]) -> bool:
    """Determine if the sequence of fiscal
    quarter differences is continuous.

    Args:
        seq: Sequence of integers.

    Returns:
        Whether the sequence is valid.

    """
    valid = {(1, 1, 2), (1, 2, 1), (2, 1, 1), (1, 1), (2, 1), (1, 2)}
    for i in range(len(seq) - 1):
        subseq = tuple(seq[i : i + 3])
        if subseq not in valid:
            return False
    return True


@dataclass
class FiscalDelta:
    """A displacement of `FiscalFrame`."""

    #: Year delta.
    years: int = 0

    #: Quarter delta.
    quarters: int = 0

    def __int__(self) -> int:
        """Return total number of fiscal periods (quarters)."""
        return 4 * self.years + self.quarters

    def __neg__(self) -> "FiscalDelta":
        """Return the negative of the delta."""
        return FiscalDelta(-self.years, -self.quarters)

    def __post_init__(self) -> None:
        """Cast underlying data to integers."""
        self.years = int(self.years)
        self.quarters = int(self.quarters)


@dataclass
class FiscalFrame:
    """A year and quarter pair.

    Examples:
        Getting quarter differences between frames and determining if the sequence
        is valid.

        >>> df = finagg.sec.api.company_concept.get("AssetsCurrent", ticker="AAPL")
        >>> frames: pd.Series = df["fy"].astype(int).astype(str) + df["fp"].astype(str)
        >>> frames = frames.apply(lambda row: finagg.frame.FiscalFrame.fromstr(row))
        >>> frames = frames.diff(periods=1).dropna().astype(int)
        >>> finagg.frame.is_valid_fiscal_seq(frames.tolist())

    """

    #: Fiscal year
    year: int

    #: Fiscal quarter (i.e., 1, 2, 3, 4)
    quarter: int

    def __add__(self, other: object) -> "FiscalFrame":
        """Add quarters and/or years to a fiscal frame."""
        if not isinstance(other, int | FiscalDelta | tuple):
            raise TypeError(
                f"can only add {int.__name__} and {FiscalDelta.__name__} "
                f"to {self.__class__.__name__} but got "
                f"`{other.__class__.__name__}` instead."
            )

        if isinstance(other, tuple):
            years, quarters = other
            other = FiscalDelta(years, quarters)

        other = int(other)
        if other != 0:
            quarters_place = self.quarter - 1 + other
            year = self.year + (quarters_place // 4)
            quarter = (quarters_place % 4) + 1
            return FiscalFrame(year, quarter)
        return FiscalFrame(self.year, self.quarter)

    def __eq__(self, other: object) -> bool:
        """Determine if two fiscal frames are equal."""
        if not isinstance(other, FiscalFrame | tuple):
            raise TypeError(
                f"can only compare type {self.__class__.__name__} "
                f"to {self.__class__.__name__} and {tuple.__name__} but got "
                f"`{other.__class__.__name__}` instead."
            )

        if isinstance(other, tuple):
            year, quarter = other
            other = FiscalFrame(year, quarter)

        return (self.year == other.year) and (self.quarter == other.quarter)

    def __post_init__(self) -> None:
        """Argument validation and bounding."""
        quarters_place = int(self.quarter) - 1
        self.year = int(self.year) + (quarters_place // 4)
        self.quarter = (quarters_place % 4) + 1

    def __sub__(self, other: object) -> Union[FiscalDelta, "FiscalFrame"]:
        """Subtract quarters and/or years from a fiscal frame."""
        if not isinstance(other, int | FiscalDelta | FiscalFrame | tuple):
            raise TypeError(
                f"can only subtract {int.__name__}, {FiscalDelta.__name__}, "
                f"{tuple.__name__}, and {self.__class__.__name__} from "
                f"{self.__class__.__name__} but got `{other.__class__.__name__}` instead."
            )

        if isinstance(other, tuple):
            years, quarters = other
            other = FiscalDelta(years, quarters)

        if isinstance(other, int | FiscalDelta):
            return self.__add__(-other)

        return FiscalDelta(self.year - other.year, self.quarter - other.quarter)

    @classmethod
    def fromstr(cls, s: str) -> "FiscalFrame":
        """Split a string into year-quarter parts by splitting on alphabetical characters."""
        year, quarter = [c for c in re.split("[a-zA-Z]", s) if c]
        return cls(int(year), int(quarter))
