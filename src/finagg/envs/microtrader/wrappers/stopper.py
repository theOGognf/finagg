"""Abstractions for stopping the environment."""

from abc import ABC, abstractmethod

from ....portfolio import Portfolio


class Stopper(ABC):
    @abstractmethod
    def eval(self, features: dict, portfolio: Portfolio) -> bool:
        """Determine whether to stop from `features` and `portfolio`."""

    def reset(self) -> None:
        """This method is called on environment resets.

        Override this if the stopper is stateful across environment transitions.

        """


class BalanceAndTimeLimiter(Stopper):
    """Limit trading days and trading balances."""

    #: Max allowed loss in total portfolio value.
    max_loss: float

    #: Max allowed trading days.
    max_trading_days: int

    #: Days spent trading.
    trading_days: int

    def __init__(self, *, max_loss: float = -0.5, max_trading_days: int = 365) -> None:
        super().__init__()
        self.max_loss = max_loss
        self.max_trading_days = max_trading_days
        self.trading_days = 0

    def eval(self, features: dict, portfolio: Portfolio) -> bool:
        """Determine whether to stop trading.

        Args:
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            Whether the environment is done.

        """
        self.trading_days += 1
        ticker: str = features["ticker"]
        price: float = features["price"]
        return (portfolio.total_percent_change({ticker: price}) <= self.max_loss) or (
            self.trading_days >= self.max_trading_days
        )

    def reset(self) -> None:
        """Reset trading day counter."""
        self.trading_days = 0


def get_stopper(stopper: str, **kwargs) -> Stopper:
    """Get a stopper based on its short name."""
    stoppers = {
        "default": BalanceAndTimeLimiter,
        "balance_and_time_limiter": BalanceAndTimeLimiter,
    }
    return stoppers[stopper](**kwargs)
