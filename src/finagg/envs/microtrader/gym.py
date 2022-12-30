import gym

from . import wrappers


class MicroTrader(gym.Env):

    #: Interface for managing the portfolio.
    actor: wrappers.Actor

    #: Interface for getting extra data out of the environment.
    #: Most useful for debugging/analysis.
    informer: wrappers.Informer

    #: Interface for observing the environment.
    observer: wrappers.Observer

    #: Interface for playing with reward shaping.
    rewarder: wrappers.Rewarder

    #: Interface for stopping the environment.
    stopper: wrappers.Stopper
