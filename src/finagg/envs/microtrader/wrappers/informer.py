"""Abstractions for getting info from the environment."""

from abc import ABC, abstractmethod
from typing import Any

from ....portfolio import Portfolio


class Informer(ABC):
    @abstractmethod
    def inform(self, action: Any, features: dict, portfolio: Portfolio) -> dict:
        """Get info from `portfolio` and `action`."""

    def reset(self) -> None:
        """This method is called on environment resets.

        Override this if the informer is stateful across environment transitions.

        """


class TradeLocater(Informer):
    """Mark trade locations."""

    def inform(
        self,
        action: tuple[int, int],
        features: dict,
        portfolio: Portfolio,
    ) -> dict:
        """Get info from an environment step.

        Args:
            action: Action taken.
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            An info dictionary.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        cost_basis_total = portfolio[ticker].cost_basis_total
        average_cost_basis = portfolio[ticker].average_cost_basis
        return {
            "date": features["date"],
            "price": price,
            "type": action[0],
            "cost_basis_total": cost_basis_total,
            "average_cost_basis": average_cost_basis,
        }


def get_informer(informer: str, **kwargs) -> Informer:
    """Get an informer based on its short name."""
    informers = {"default": TradeLocater, "trade_locater": TradeLocater}
    return informers[informer](**kwargs)
