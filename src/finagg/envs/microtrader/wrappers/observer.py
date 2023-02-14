"""Abstractions for observing the environment."""

from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import numpy.typing as npt
from gym import spaces

from ....portfolio import Portfolio


class Observer(ABC):
    #: Underlying observation space.
    observation_space: spaces.Space[Any]

    @abstractmethod
    def observe(self, features: dict[str, Any], portfolio: Portfolio) -> Any:
        """Observe the environment from predefined features and a portfolio."""

    @abstractmethod
    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> Any:
        """This method is called on environment resets.

        Override this if the actor is stateful across environment transitions.

        """


class FundamentalsMonitor(Observer):
    """Observe daily and quarterly fundamentals."""

    def __init__(self) -> None:
        super().__init__()
        self.observation_space = spaces.Box(
            -100,
            100,
            shape=(
                (
                    sum(
                        [
                            1,  # Time limits
                            2,  # Value on hand
                            2,  # Change in value
                            7,  # Fundamentals
                            5,  # Changes in prices and volume
                        ]
                    )
                ),
            ),
        )

    def observe(
        self, features: dict[str, Any], portfolio: Portfolio
    ) -> npt.NDArray[np.float32]:
        """Observe fundamental features.

        Args:
            features: Fundamental features from the `finagg.fundamental` submodule.
            portfolio: Portfolio to manage.

        Returns:
            List of fundamental features.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        if ticker in portfolio:
            position_percent_change = portfolio[ticker].total_percent_change(price)
        else:
            position_percent_change = 0.0
        portfolio_percent_change = portfolio.total_percent_change({ticker: price})
        return np.clip(
            [
                features["trading_days_remaining"] / features["max_trading_days"],
                # Total assets on hand
                portfolio.cash / portfolio.deposits_total,
                portfolio.total_dollar_value({ticker: price})
                / portfolio.deposits_total,
                # Change in value
                portfolio_percent_change,
                position_percent_change,
                # Fundamentals
                features["PriceEarningsRatio"] / 100,
                features["EarningsPerShare"] / 100,
                features["WorkingCapitalRatio"],
                features["QuickRatio"],
                features["DebtEquityRatio"],
                features["ReturnOnEquity"],
                features["PriceBookRatio"],
                # Changes in prices and volume
                features["open"],
                features["high"],
                features["low"],
                features["close"],
                features["volume"],
            ],
            -99,
            99,
        )

    def reset(
        self, features: dict[str, Any], portfolio: Portfolio
    ) -> npt.NDArray[np.float32]:
        """Just observe to reset."""
        return self.observe(features, portfolio)


def get_observer(observer: str, **kwargs: Any) -> Observer:
    """Get an observer based on its short name."""
    observers: dict[str, type[Observer]] = {
        "default": FundamentalsMonitor,
        "fundamentals": FundamentalsMonitor,
    }
    return observers[observer](**kwargs)
