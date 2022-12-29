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


class PerformanceMonitor(Observer):
    ...


def get_observer(observer: str, **kwargs) -> Observer:
    """Get an observer based on its short name."""
    observers = {
        "default": PerformanceMonitor,
        "performance_monitor": PerformanceMonitor,
    }
    return observers[observer](**kwargs)
