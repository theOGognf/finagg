"""Abstractions for observing the environment."""

from abc import ABC, abstractmethod
from typing import Any

from gym import spaces

from ....portfolio import Portfolio


class Observer(ABC):
    #: Underlying observation space.
    observation_space: spaces.Space

    @abstractmethod
    def observe(self, features: dict[str, float], portfolio: Portfolio) -> Any:
        """Observe the environment from predefined features and a portfolio."""

    def reset(self) -> Any:
        """This method is called on environment resets.

        Override this if the actor is stateful across environment transitions.

        """


class FundamentalsMonitor(Observer):
    """Observe daily and quarterly fundamentals."""

    def observe(self, features: dict[str, float], portfolio: Portfolio) -> list[float]:
        """Observe fundamental features.

        Args:
            features: Mixed features from the `finagg.mixed` submodule.
            portfolio: Portfolio to manage.

        Returns:
            List of fundamental features.

        """
        ticker = features["ticker"]
        price = features["price"]
        if ticker in portfolio:
            position_percent_change = portfolio[ticker].total_percent_change(price)
        else:
            position_percent_change = 0.0
        portfolio_percent_change = portfolio.total_percent_change({ticker: price})
        return [
            # Change in value
            portfolio_percent_change,
            position_percent_change,
            # Fundamentals
            features["PriceEarningsRatio"],
            features["EarningsPerShare"],
            features["WorkingCapitalRatio"],
            features["QuickRatio"],
            features["DebtEquityRatio"],
            features["ReturnOnEquity"],
            features["PriceBookRatio"],
            # Changes in prices and volumes
            features["open"],
            features["high"],
            features["low"],
            features["close"],
            features["volume"],
            # Changes w.r.t. common indices
            features["VOO_open"],
            features["VOO_high"],
            features["VOO_low"],
            features["VOO_close"],
            features["VOO_volume"],
            features["VGT_open"],
            features["VGT_high"],
            features["VGT_low"],
            features["VGT_close"],
            features["VGT_volume"],
        ]


def get_observer(observer: str, **kwargs) -> Observer:
    """Get an observer based on its short name."""
    observers = {
        "default": FundamentalsMonitor,
        "fundamentals": FundamentalsMonitor,
    }
    return observers[observer](**kwargs)
