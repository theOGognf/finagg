"""Abstractions for getting rewards from interacting with the environment."""

from abc import ABC, abstractmethod
from typing import Any

from ....portfolio import Portfolio


class Rewarder(ABC):
    @abstractmethod
    def reward(self, features: dict[str, Any], portfolio: Portfolio) -> float:
        """Get a reward from manging a `portfolio` with an `action`."""

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
        """This method is called on environment resets.

        Override this if the rewarder is stateful across environment transitions.

        """


class PortfolioCashChange(Rewarder):
    """Reward change in portfolio cash."""

    def reward(
        self,
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> float:
        """Get a reward from an environment step.

        Args:
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            Change in portfolio cash w.r.t. initial deposits.

        """
        price: float = features["price"]
        quantity: float = features["trade_amount"]
        if features["trade_type"] == 0:
            reward = 0.0
        if features["trade_type"] == 1:
            reward = -price * quantity
        if features["trade_type"] == 2:
            reward = price * quantity
        return reward / portfolio.deposits_total


class PortfolioTotalDollarValue(Rewarder):
    """Reward total portfolio value."""

    def reward(
        self,
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> float:
        """Get a reward from an environment step.

        Args:
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            Ratio of total portfolio value to initial deposits.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        return portfolio.total_dollar_value({ticker: price}) / portfolio.deposits_total


class PortfolioTotalPercentChange(Rewarder):
    """Reward total portfolio change in value."""

    def reward(
        self,
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> float:
        """Get a reward from an environment step.

        Args:
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            Percent change in total value.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        return portfolio.total_percent_change({ticker: price})


def get_rewarder(rewarder: str, **kwargs: Any) -> Rewarder:
    """Get a rewarder based on its short name."""
    rewarders: dict[str, type[Rewarder]] = {
        "default": PortfolioTotalDollarValue,
        "portfolio_cash_change": PortfolioCashChange,
        "portfolio_total_percent_change": PortfolioTotalPercentChange,
        "portfolio_total_dollar_value": PortfolioTotalDollarValue,
    }
    return rewarders[rewarder](**kwargs)
