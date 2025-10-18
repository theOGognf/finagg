"""Features from SEC sources."""

from ._raw import Entities, Filings, Submissions, Tags
from ._refined import (
    Annual,
    IndustryAnnual,
    IndustryQuarterly,
    NormalizedAnnual,
    NormalizedQuarterly,
    Quarterly,
)

__all__ = [
    "annual",
    "entities",
    "filings",
    "quarterly",
    "submissions",
    "tags",
    "Annual",
    "Entities",
    "Filings",
    "IndustryAnnual",
    "NormalizedAnnual",
    "Quarterly",
    "IndustryQuarterly",
    "NormalizedQuarterly",
    "Submissions",
    "Tags",
]

annual = Annual()
"""The most popular way for accessing :class:`finagg.sec.feat.Annual`.

:meta hide-value:
"""

entities = Entities()
"""The most popular way for accessing :class:`finagg.sec.feat.Entities`.

:meta hide-value:
"""

filings = Filings()
"""The most popular way for accessing :class:`finagg.sec.feat.Filings`.

:meta hide-value:
"""

quarterly = Quarterly()
"""The most popular way for accessing :class:`finagg.sec.feat.Quarterly`.

:meta hide-value:
"""

submissions = Submissions()
"""The most popular way for accessing :class:`finagg.sec.feat.Submissions`.

:meta hide-value:
"""

tags = Tags()
"""The most popular way for accessing :class:`finagg.sec.feat.Tags`.

:meta hide-value:
"""
