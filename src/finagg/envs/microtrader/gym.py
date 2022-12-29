import gym

from . import wrappers


class MicroTrader(gym.Env):

    actor: wrappers.Actor

    informer: wrappers.Informer

    observer: wrappers.Observer

    rewarder: wrappers.Rewarder

    stopper: wrappers.Stopper
