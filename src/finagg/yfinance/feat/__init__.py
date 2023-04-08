"""Features from Yahoo! Finance sources."""

from .raw import Prices
from .refined import Daily

daily = Daily()
"""The most popular way for accessing
:class:`~finagg.yfinance.feat.refined.Daily`.

:meta hide-value:
"""

prices = Prices()
"""The most popular way for accessing
:class:`~finagg.yfinance.feat.raw.Prices`.

:meta hide-value:
"""
