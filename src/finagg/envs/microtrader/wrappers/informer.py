"""Abstractions for getting info from the environment."""

from abc import ABC, abstractmethod
from typing import Any

from ....portfolio import Portfolio


class Informer(ABC):
    @abstractmethod
    def inform(self, features: dict[str, Any], portfolio: Portfolio) -> dict[str, Any]:
        """Get info from `portfolio` and `action`."""

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
        """This method is called on environment resets.

        Override this if the informer is stateful across environment transitions.

        """


class TradeLocater(Informer):
    """Mark trade locations."""

    def inform(
        self,
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> dict[str, Any]:
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
        if ticker in portfolio:
            position_percent_change = portfolio[ticker].total_percent_change(price)
            cost_basis_total = portfolio[ticker].cost_basis_total
            average_cost_basis = portfolio[ticker].average_cost_basis
        else:
            position_percent_change = 0.0
            cost_basis_total = 0.0
            average_cost_basis = 0.0
        portfolio_dollar_change = portfolio.total_dollar_change({ticker: price})
        portfolio_percent_change = portfolio.total_percent_change({ticker: price})
        return {
            "date": features["date"],
            "ticker": ticker,
            "price": price,
            "cost_basis_total": cost_basis_total,
            "average_cost_basis": average_cost_basis,
            "position_percent_change": position_percent_change,
            "portfolio_dollar_change": portfolio_dollar_change,
            "portfolio_percent_change": portfolio_percent_change,
        }


def get_informer(informer: str, **kwargs: Any) -> Informer:
    """Get an informer based on its short name."""
    informers = {"default": TradeLocater, "trade_locater": TradeLocater}
    return informers[informer](**kwargs)
