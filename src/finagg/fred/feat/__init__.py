"""Features from FRED sources."""

from ._raw import Series
from ._refined import Economic, NormalizedEconomic, TimeSummarizedEconomic

__all__ = [
    "economic",
    "series",
    "Economic",
    "NormalizedEconomic",
    "TimeSummarizedEconomic",
    "Series",
]

economic = Economic()
"""The most popular way for accessing :class:`finagg.fred.feat.Economic`.

:meta hide-value:
"""

series = Series()
"""The most popular way for accessing :class:`finagg.fred.feat.Series`.

:meta hide-value:
"""
