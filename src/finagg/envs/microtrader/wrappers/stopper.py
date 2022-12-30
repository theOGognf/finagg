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


class LossLimiter(Stopper):
    """Limit trading balance loss."""

    #: Max allowed loss in total portfolio value.
    max_loss: float

    def __init__(self, *, max_loss: float = -0.5) -> None:
        super().__init__()
        self.max_loss = max_loss

    def eval(self, features: dict, portfolio: Portfolio) -> bool:
        """Determine whether to stop trading.

        Args:
            features: Environment state data.
            portfolio: Portfolio to observe.

        Returns:
            Whether the environment is done.

        """
        ticker: str = features["ticker"]
        price: float = features["price"]
        return portfolio.total_percent_change({ticker: price}) <= self.max_loss


def get_stopper(stopper: str, **kwargs) -> Stopper:
    """Get a stopper based on its short name."""
    stoppers = {
        "default": LossLimiter,
        "loss_limiter": LossLimiter,
    }
    return stoppers[stopper](**kwargs)
