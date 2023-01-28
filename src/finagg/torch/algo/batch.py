"""Definitions related to data passed between algorithm modules."""

import torch

DEVICE = int | str | torch.device


class Batch:
    #: Key denoting observations from the environment.
    #: Typically processed by a policy model.
    OBS = "obs"

    #: Key denoting rewards from the environment.
    #: Typically used by a learning algorithm.
    REWARDS = "rewards"

    #: Key denoting features output from a policy model.
    #: Typically processed by a policy action distribution.
    FEATURES = "features"

    #: Key denoting features output by a policy action distribution.
    #: Usually propagated through an environment.
    ACTIONS = "actions"

    #: Key denoting the log probability of taking `actions` with feature
    #: and a model. Typically used by learning algorithms.
    LOGP = "logp"

    #: Key denoting value function approximation from a policy model.
    #: Typically used by learning algorithms or for analyzing a trained model.
    VALUES = "values"

    #: Key denoting elements that're inputs to a model and have corresponding
    #: "padding_mask" elements.
    INPUTS = "inputs"

    #: Key denoting elements that're used for indicating padded elements
    #: with respect to elements corresponding to an "inputs" key.
    PADDING_MASK = "padding_mask"

    #: Key denoting view requirements applied to another key. These are
    #: the preprocessed inputs to a model.
    VIEWS = "views"

    #: Key denoting advantages (action value function baselined by the state
    #: value function).
    ADVANTAGES = "advantages"
