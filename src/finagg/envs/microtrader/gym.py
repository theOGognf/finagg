"""Actual environment definition using `wrappers`."""

import random
from dataclasses import field
from typing import Any, Hashable, Iterable

import gym
import pandas as pd
from pydantic import BaseModel

from ... import mixed
from ...portfolio import Portfolio
from . import wrappers


class Config(BaseModel):
    """Environment configuration."""

    #: Actor wrapper ID.
    actor: str = "default"

    #: Informer wrapper ID.
    informer: str = "default"

    #: Observer wrapper ID.
    observer: str = "default"

    #: Rewarder wrapper ID.
    rewarder: str = "default"

    #: Stopper wrapper ID.
    stopper: str = "default"

    #: Actor config.
    actor_config: dict[str, Any] = field(default_factory=dict)

    #: Informer config.
    informer_config: dict[str, Any] = field(default_factory=dict)

    #: Observer config.
    observer_config: dict[str, Any] = field(default_factory=dict)

    #: Rewarder config.
    rewarder_config: dict[str, Any] = field(default_factory=dict)

    #: Stopper config.
    stopper_config: dict[str, Any] = field(default_factory=dict)

    #: Starting portfolio value and cash.
    starting_cash: float = 10_000

    #: Days to trade.
    trading_days: int = 365


class Sampler:
    """Helper for sampling trading data features."""

    #: Max number of samples to get.
    trading_days: int

    #: `trading_days + 1` to account for calling `reset`.
    required_rows: int

    #: Number of times `step` can be called to get features.
    remaining_rows: int

    #: Trading symbol/ticker.
    ticker: str

    #: Backend iterator that iterates over all the samples.
    iterator: Iterable[tuple[Hashable, pd.Series]]  # type: ignore

    def __init__(self, trading_days: int, /) -> None:
        self.trading_days = trading_days
        self.required_rows = trading_days + 1

    def reset(self) -> tuple[dict[str, Any], bool]:
        """Sample a new set of features."""
        tickers = list(mixed.store.get_ticker_set())
        num_rows = 0
        while num_rows < self.required_rows:
            self.ticker = random.choice(tickers)
            df = mixed.features.fundamental_features.from_store(self.ticker)
            num_rows = len(df.index)
        if num_rows < self.required_rows:
            subsample = df
        else:
            idx = random.randint(0, num_rows - self.required_rows)
            subsample = df.iloc[idx : idx + self.required_rows]
        self.remaining_rows = len(subsample.index) - 1
        self.iterator = subsample.iterrows()
        date, features = next(self.iterator)  # type: ignore
        features["date"] = date
        features["ticker"] = self.ticker
        features["max_trading_days"] = self.trading_days
        features["trading_days_remaining"] = self.remaining_rows
        return features, self.remaining_rows <= 0

    def step(self) -> tuple[dict[str, Any], bool]:
        """Return the next row of features and whether there's more data remaining."""
        self.remaining_rows -= 1
        date, features = next(self.iterator)  # type: ignore
        features["date"] = date
        features["ticker"] = self.ticker
        features["max_trading_days"] = self.trading_days
        features["trading_days_remaining"] = self.remaining_rows
        return features, self.remaining_rows <= 0


class MicroTrader(gym.Env):  # type: ignore
    """Manage a portfolio containing cash and a single security position."""

    #: Interface for managing the portfolio.
    actor: wrappers.Actor

    #: Environment config.
    config: Config

    #: Features (environment state) updated each step.
    features: dict[str, Any]

    #: Interface for getting extra data out of the environment.
    #: Most useful for debugging/analysis.
    informer: wrappers.Informer

    #: Interface for observing the environment.
    observer: wrappers.Observer

    #: New portfolio created on `reset`.
    portfolio: Portfolio

    #: Interface for playing with reward shaping.
    rewarder: wrappers.Rewarder

    #: Mechanism for sampling trading data each trading day.
    sampler: Sampler

    #: Interface for stopping the environment.
    stopper: wrappers.Stopper

    def __init__(self, config: None | dict[str, Any] = None) -> None:
        super().__init__()
        if config is None:
            config = {}
        self.config = Config(**config)
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
        self.sampler = Sampler(self.config.trading_days)
        self.action_space = self.actor.action_space
        self.observation_space = self.observer.observation_space

    def reset(self, *, seed: None | int = None) -> Any:
        """Reset the environment."""
        if seed is not None:
            random.seed(seed)
        self.portfolio = Portfolio(self.config.starting_cash)
        self.features, _ = self.sampler.reset()
        self.actor.reset(self.features, self.portfolio)
        self.informer.reset(self.features, self.portfolio)
        self.rewarder.reset(self.features, self.portfolio)
        self.stopper.reset(self.features, self.portfolio)
        return self.observer.reset(self.features, self.portfolio)

    def step(self, action: Any) -> tuple[Any, float, bool, dict[str, Any]]:
        """Step the environment with `action`."""
        self.actor.act(action, self.features, self.portfolio)
        reward = self.rewarder.reward(self.features, self.portfolio)
        done = self.stopper.eval(self.features, self.portfolio)
        info = self.informer.inform(self.features, self.portfolio)
        self.features, out_of_data = self.sampler.step()
        obs = self.observer.observe(self.features, self.portfolio)
        return obs, reward, done or out_of_data, info
