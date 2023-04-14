"""Features from Yahoo! Finance sources."""

from ._raw import Prices
from ._refined import Daily

__all__ = ["daily", "prices", "Daily", "Prices"]

daily = Daily()
"""The most popular way for accessing :class:`finagg.yfinance.feat.Daily`.

:meta hide-value:
"""

prices = Prices()
"""The most popular way for accessing :class:`finagg.yfinance.feat.Prices`.

:meta hide-value:
"""
