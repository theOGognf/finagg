"""Abstractions for observing the environment."""

from abc import ABC, abstractmethod

import pandas as pd
from gym import spaces

from ....portfolio import Portfolio


class Observer(ABC):
    @property
    @abstractmethod
    def observation_space(self) -> spaces.Space:
        """Each observer can have a different space."""

    @abstractmethod
    def observe(self, features: pd.DataFrame, portfolio: Portfolio) -> ...:
        """Observe features from"""

    def reset(self) -> None:
        """This method is called on environment resets.

        Override this if the actor is stateful across environment transitions.

        """


class PerformanceMonitor(Observer):
    ...
