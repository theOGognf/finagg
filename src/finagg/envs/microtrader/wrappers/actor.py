"""Abstractions for interacting with the environment."""

from abc import ABC, abstractmethod
from typing import Any, Literal

from gym import spaces

from ....portfolio import Portfolio


class Actor(ABC):
    #: Underlying action space.
    action_space: spaces.Space

    @abstractmethod
    def act(self, action: Any, features: dict[str, Any], portfolio: Portfolio) -> None:
        """Manage `portfolio` with `action`."""

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
        """This method is called on environment resets.

        Override this if the actor is stateful across environment transitions.

        """


class DCABaseline(Actor):
    """Dollar cost averaging baseline."""

    #: Total cash to use each buy.
    trade_amount: float

    def __init__(self) -> None:
        super().__init__()
        self.action_space = spaces.Discrete(1)
        self.trade_amount = None

    def act(
        self,
        action: Literal[0],
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: Only buy.
            features: Environment state data such as security price.
            portfolio: Portfolio to manage.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        quantity = self.trade_amount / price
        portfolio.buy(ticker, price, quantity)
        features["trade_type"] = 1
        features["trade_quantity"] = quantity

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
        """Start the portfolio with a small position in the security."""
        max_trading_days = features["max_trading_days"]
        self.trade_amount = portfolio.deposits_total / max_trading_days


class BuyAndHoldBaseline(Actor):
    """Invest the entire portfolio immediately and hold forever."""

    def __init__(self) -> None:
        super().__init__()
        self.action_space = spaces.Discrete(1)

    def act(
        self,
        action: Literal[0],
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: Only buy.
            features: Environment state data such as security price.
            portfolio: Portfolio to manage.

        """
        if portfolio.cash > 0:
            ticker: str = features["ticker"]
            price: float = features["price"]
            quantity = portfolio.cash / price
            portfolio.buy(ticker, price, quantity)
            features["trade_type"] = 1
            features["trade_quantity"] = quantity
        else:
            features["trade_type"] = 0
            features["trade_quantity"] = 0


class BuyAndHoldTrader(Actor):
    """Manage a portfolio containing cash and a position in just one security
    by only buying and holding.

    """

    #: Right-end bin values for trade amounts.
    trade_amount_bins: list[float]

    def __init__(self, *, trade_amount_bins: int = 5) -> None:
        super().__init__()
        self.trade_amount_bins = [
            (i + 1) / (trade_amount_bins + 1) for i in range(trade_amount_bins)
        ]
        self.action_space = spaces.Tuple(
            [spaces.Discrete(2), spaces.Discrete(trade_amount_bins)]
        )

    def act(
        self,
        action: tuple[int, int],
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: Tuple of action type (no-op or buy)
                and buy amount bin ID.
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
                features["trade_quantity"] = 0
                return

            case 1:
                quantity = amount * portfolio.cash / price
                portfolio.buy(ticker, price, quantity)
                features["trade_type"] = 1
                features["trade_quantity"] = quantity
                return


class DCATrader(Actor):
    """Dollar cost averaging trader. Buy in small increments."""

    #: Minimum cash to use each buy.
    min_trade_amount: float

    #: Accumulated cash to use each buy.
    #: If a buy trade is skipped for a day, this amount is incremented
    #: by `min_trade_amount`.
    trade_amount: float

    def __init__(self) -> None:
        super().__init__()
        self.action_space = spaces.Discrete(2)
        self.trade_amount = None
        self.min_trade_amount = None

    def act(
        self,
        action: int,
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: No-op or buy.
            features: Environment state data such as security price.
            portfolio: Portfolio to manage.

        """
        action = int(action)
        ticker: str = features["ticker"]
        price: float = features["price"]
        match action:
            case 0:
                features["trade_type"] = 0
                features["trade_quantity"] = 0
                self.trade_amount += self.min_trade_amount
                return

            case 1:
                quantity = self.trade_amount / price
                portfolio.buy(ticker, price, quantity)
                features["trade_type"] = 1
                features["trade_quantity"] = quantity
                self.trade_amount = self.min_trade_amount
                return

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
        """Start the portfolio with a small position in the security."""
        max_trading_days = features["max_trading_days"]
        self.min_trade_amount = portfolio.deposits_total / max_trading_days
        self.trade_amount = self.min_trade_amount


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
        features: dict[str, Any],
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
                features["trade_quantity"] = 0
                return

            case 1:
                quantity = amount * portfolio.cash / price
                portfolio.buy(ticker, price, quantity)
                features["trade_type"] = 1
                features["trade_quantity"] = quantity
                return

            case 2:
                quantity = amount * portfolio[ticker].quantity
                portfolio.sell(ticker, price, quantity)
                features["trade_type"] = 2
                features["trade_quantity"] = quantity
                return

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
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
        self.trade_amount_bins = [
            (i + 1) / (trade_amount_bins + 1) for i in range(trade_amount_bins)
        ]
        self.action_space = spaces.Discrete(
            sum([1, trade_amount_bins, trade_amount_bins])
        )

    def act(
        self,
        action: int,
        features: dict[str, Any],
        portfolio: Portfolio,
    ) -> None:
        """No-op, buy, or sell positions.

        Args:
            action: Discrete action (no-op + trade bin options).
            features: Environment state data such as security price.
            portfolio: Portfolio to manage.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]

        if action == 0:
            features["trade_type"] = 0
            features["trade_quantity"] = 0
            return

        elif 1 <= action <= len(self.trade_amount_bins):
            amount = self.trade_amount_bins[action - 1]
            if portfolio.cash < price:
                features["trade_type"] = 1
                features["trade_quantity"] = 0.0
                return
            quantity = amount * portfolio.cash / price
            features["trade_type"] = 1
            features["trade_quantity"] = quantity
            portfolio.buy(ticker, price, quantity)
            return

        else:
            amount = self.trade_amount_bins[(action - 1) % len(self.trade_amount_bins)]
            if ticker not in portfolio:
                features["trade_type"] = 2
                features["trade_quantity"] = 0.0
                return
            quantity = amount * portfolio[ticker].quantity
            features["trade_type"] = 2
            features["trade_quantity"] = quantity
            portfolio.sell(ticker, price, quantity)
            return

    def reset(self, features: dict[str, Any], portfolio: Portfolio) -> None:
        """Start the portfolio with a small position in the security."""
        ticker: str = features["ticker"]
        price: float = features["price"]
        quantity = 0.01 * portfolio.cash / price
        portfolio.buy(ticker, price, quantity)


def get_actor(actor: str, **kwargs: Any) -> Actor:
    """Get an actor based on its short name."""
    actors: dict[str, type[Actor]] = {
        "default": BuyAndHoldTrader,
        "buy_and_hold_baseline": BuyAndHoldBaseline,
        "buy_and_hold_trader": BuyAndHoldTrader,
        "dca_baseline": DCABaseline,
        "dca_trader": DCATrader,
        "discrete_trader": DiscreteTrader,
        "flattened_discrete_trader": FlattenedDiscreteTrader,
    }
    return actors[actor](**kwargs)
