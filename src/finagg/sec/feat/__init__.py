"""Features from SEC sources."""

from .raw import Submissions, Tags, get_unique_filings
from .refined import Annual, Quarterly

annual = Annual()
"""The most popular way for accessing
:class:`~finagg.sec.feat.refined.annual_.Annual`.

:meta hide-value:
"""

quarterly = Quarterly()
"""The most popular way for accessing
:class:`~finagg.sec.feat.refined.quarterly_.Quarterly`.

:meta hide-value:
"""

submissions = Submissions()
"""The most popular way for accessing :class:`~finagg.sec.feat.raw.Submissions`.

:meta hide-value:
"""

tags = Tags()
"""The most popular way for accessing :class:`~finagg.sec.feat.raw.Tags`.

:meta hide-value:
"""
