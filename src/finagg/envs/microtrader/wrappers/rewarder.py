"""Abstractions for getting rewards from interacting with the environment."""

from abc import ABC, abstractmethod
from typing import Any

from ....portfolio import Portfolio


class Rewarder(ABC):
    @abstractmethod
    def reward(self, action: Any, features: dict, portfolio: Portfolio) -> float:
        """Get a reward from manging a `portfolio` with an `action`."""

    def reset(self) -> None:
        """This method is called on environment resets.

        Override this if the rewarder is stateful across environment transitions.

        """


class PortfolioDollarChange(Rewarder):
    """Reward total dollar changes in portfolio value."""

    #: Previous portfolio total dollar change.
    #: Used for computing the change from the new price.
    prev_total_dollar_change: float

    def __init__(self) -> None:
        super().__init__()
        self.prev_total_dollar_change = 0.0

    def reward(
        self,
        _: tuple[int, int],
        features: dict,
        portfolio: Portfolio,
    ) -> float:
        """Get a reward from an environment step.

        Args:
            action: Action taken.
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            A reward value.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        total_dollar_change = portfolio.total_dollar_change({ticker: price})
        reward = total_dollar_change - self.prev_total_dollar_change
        self.prev_total_dollar_change = total_dollar_change
        return reward


def get_rewarder(rewarder: str, **kwargs) -> Rewarder:
    """Get a rewarder based on its short name."""
    rewarders = {
        "default": PortfolioDollarChange,
        "portfolio_dollar_change": PortfolioDollarChange,
    }
    return rewarders[rewarder](**kwargs)
