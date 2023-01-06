"""Abstractions for interacting with the environment."""

from abc import ABC, abstractmethod
from typing import Any

from gym import spaces

from ....portfolio import Portfolio


class Actor(ABC):
    #: Underlying action space.
    action_space: spaces.Space

    @abstractmethod
    def act(self, action: Any, features: dict, portfolio: Portfolio) -> None:
        """Manage `portfolio` with `action`."""

    def reset(self, features: dict, portfolio: Portfolio) -> None:
        """This method is called on environment resets.

        Override this if the actor is stateful across environment transitions.

        """


class DiscreteTrader(Actor):
    """Manage a portfolio containing cash and a position in just one security."""

    #: Right-end bin values for trade amounts.
    trade_amount_bins: list[float]

    def __init__(self, *, trade_amount_bins: int = 2) -> None:
        super().__init__()
        self.trade_amount_bins = [
            (i + 1) / (trade_amount_bins + 1) for i in range(trade_amount_bins)
        ]
        self.action_space = spaces.Tuple(
            [spaces.Discrete(3), spaces.Discrete(trade_amount_bins)]
        )

    def act(
        self,
        action: tuple[int, int],
        features: dict,
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: Tuple of action type (no-op, buy, or sell)
                and buy/sell amount bin ID.
            features: Environment state data such as security price.
            portfolio: Portfolio to manage.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        action_type, trade_amount_bin = action
        amount = self.trade_amount_bins[trade_amount_bin]
        match action_type:
            case 0:
                features["trade_type"] = 0
                features["trade_amount"] = 0
                return

            case 1:
                quantity = amount * portfolio.cash / price
                portfolio.buy(ticker, price, quantity)
                features["trade_type"] = 1
                features["trade_amount"] = quantity
                return

            case 2:
                quantity = amount * portfolio[ticker].quantity
                portfolio.sell(ticker, price, quantity)
                features["trade_type"] = 2
                features["trade_amount"] = quantity
                return

    def reset(self, features: dict, portfolio: Portfolio) -> None:
        """Start the portfolio with a small position in the security."""
        ticker: str = features["ticker"]
        price: float = features["price"]
        quantity = 0.01 * portfolio.cash / price
        portfolio.buy(ticker, price, quantity)


class FlattenedDiscreteTrader(Actor):
    """Manage a portfolio containing cash and a position in just one security."""

    #: Right-end bin values for trade amounts.
    trade_amount_bins: list[float]

    def __init__(self, *, trade_amount_bins: int = 2) -> None:
        super().__init__()
        self.trade_amount_bins = [0.5, 0.99]
        self.action_space = spaces.Discrete(
            sum([1, trade_amount_bins, trade_amount_bins])
        )

    def act(
        self,
        action: tuple[int, int],
        features: dict,
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: Tuple of action type (no-op, buy, or sell)
                and buy/sell amount bin ID.
            features: Environment state data such as security price.
            portfolio: Portfolio to manage.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]

        if action == 0:
            features["trade_type"] = 0
            features["trade_amount"] = 0
            return

        elif 1 <= action <= len(self.trade_amount_bins):
            amount = self.trade_amount_bins[action - 1]
            if portfolio.cash < price:
                features["trade_type"] = 1
                features["trade_amount"] = 0.0
                return
            quantity = amount * portfolio.cash / price
            features["trade_type"] = 1
            features["trade_amount"] = quantity
            portfolio.buy(ticker, price, quantity)
            return

        else:
            amount = self.trade_amount_bins[(action - 1) % len(self.trade_amount_bins)]
            if ticker not in portfolio:
                features["trade_type"] = 2
                features["trade_amount"] = 0.0
                return
            quantity = amount * portfolio[ticker].quantity
            features["trade_type"] = 2
            features["trade_amount"] = quantity
            portfolio.sell(ticker, price, quantity)
            return

    def reset(self, features: dict, portfolio: Portfolio) -> None:
        """Start the portfolio with a small position in the security."""
        ticker: str = features["ticker"]
        price: float = features["price"]
        quantity = 0.01 * portfolio.cash / price
        portfolio.buy(ticker, price, quantity)


def get_actor(actor: str, **kwargs) -> Actor:
    """Get an actor based on its short name."""
    actors = {"default": FlattenedDiscreteTrader, "discrete_trader": DiscreteTrader}
    return actors[actor](**kwargs)
