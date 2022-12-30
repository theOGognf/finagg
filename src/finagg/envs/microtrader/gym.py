"""Actual environment definition using `wrappers`."""

import random
from dataclasses import dataclass, field
from typing import Any

import gym

from ... import mixed
from ...portfolio import Portfolio
from . import wrappers


class MicroTrader(gym.Env):
    """Manage a portfolio containing cash and a single security position."""

    @dataclass
    class Config:
        """Environment configuration."""

        actor: str = "default"
        informer: str = "default"
        observer: str = "default"
        rewarder: str = "default"
        stopper: str = "default"
        actor_config: dict = field(default_factory=dict)
        informer_config: dict = field(default_factory=dict)
        observer_config: dict = field(default_factory=dict)
        rewarder_config: dict = field(default_factory=dict)
        stopper_config: dict = field(default_factory=dict)
        starting_cash: float = 10_000
        max_trading_days: int = 365

    #: Interface for managing the portfolio.
    actor: wrappers.Actor

    #: Environment config
    conig: Config

    #: Interface for getting extra data out of the environment.
    #: Most useful for debugging/analysis.
    informer: wrappers.Informer

    #: Interface for observing the environment.
    observer: wrappers.Observer

    #: Interface for playing with reward shaping.
    rewarder: wrappers.Rewarder

    #: Interface for stopping the environment.
    stopper: wrappers.Stopper

    def __init__(self, config: None | dict = None) -> None:
        super().__init__()
        self.config = self.Config(**config)
        self.actor = wrappers.get_actor(self.config.actor, **self.config.actor_config)
        self.informer = wrappers.get_informer(
            self.config.informer, **self.config.informer_config
        )
        self.observer = wrappers.get_observer(
            self.config.observer, **self.config.observer_config
        )
        self.rewarder = wrappers.get_rewarder(
            self.config.rewarder, **self.config.rewarder_config
        )
        self.stopper = wrappers.get_stopper(
            self.config.stopper, **self.config.stopper_config
        )
        self.portfolio = None
        self.ticker = None

    @property
    def action_space(self) -> gym.Space:
        """Return the actor's action space."""
        return self.actor.action_space

    @property
    def observation_space(self) -> gym.Space:
        """Return the observer's action space."""
        return self.observer.observation_space

    def reset(self) -> Any:
        """Reset the environment."""
        self.portfolio = Portfolio(self.config.starting_cash)
        self.ticker = random.sample(mixed.store.get_ticker_set())
        df = mixed.features.fundamental_features.from_store(self.ticker)
        nrows = len(df.index)
        if nrows < (self.config.max_trading_days):
            subsample = df
        else:
            idx = random.randint(0, nrows - (self.config.max_trading_days))
            subsample = df.iloc[idx : idx + self.config.max_trading_days]
        self.trading_data = subsample.itertuples()
        features = next(self.trading_data)
        features["ticker"] = self.ticker
        self.actor.reset()
        self.informer.reset()
        self.rewarder.reset()
        self.stopper.reset()
        return self.observer.reset(features, self.portfolio)

    def step(self, action: Any) -> Any:
        """Step the environment with `action`."""
