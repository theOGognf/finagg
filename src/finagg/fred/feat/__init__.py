"""Features from FRED sources."""

from .raw import Series
from .refined import Economic

economic = Economic()
"""The most popular way for accessing
:class:`~finagg.fred.feat.refined.Economic`.

:meta hide-value:
"""

series = Series()
"""The most popular way for accessing :class:`~finagg.fred.feat.raw.Series`.

:meta hide-value:
"""
