"""Features from FRED sources."""

from ._raw import Series
from ._refined import Economic

economic = Economic()
"""The most popular way for accessing
:class:`~finagg.fred.feat.refined.Economic`.

:meta hide-value:
"""

series = Series()
"""The most popular way for accessing :class:`~finagg.fred.feat.raw.Series`.

:meta hide-value:
"""
