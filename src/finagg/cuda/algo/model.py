"""Abstract model definition."""

from abc import ABC, abstractmethod
from typing import Any

import torch
from tensordict import TensorDict
from torchrl.data import TensorSpec

from .view import ViewRequirement


class Model(ABC, torch.nn.Module):
    """Policy component that processes environment observations into
    a value function approximation and features to be consumed by an
    action distribution for action sampling.

    This definition is largely influenced by RLlib's model concept:
        https://github.com/ray-project/ray/blob/master/rllib/models/modelv2.py.

    The model is intended to be called with the forward pass (like any
    other PyTorch module) to get the inputs to the policy's action
    distribution. It's expected that the value function approximation
    is stored after each forward pass in some intermediate attribute
    and can be accessed with a subsequent call to `value_function`.
    Both the `forward` method and the `value_function` must be implemented
    to properly implement a model.

    Example data flow (adapted from the RLlib reference above):
        batch -> forward() -> features
            \-> value_function() -> V(s)

    Args:
        observation_spec: Spec defining the forward pass input.
        feature_spec: Spec defining the forward pass output.
        action_spec: Spec defining the outputs of the policy's action
            distribution that this model is a component of.
        config: Model-specific configuration.

    """

    #: Spec defining the outputs of the policy's action distribution that
    #: this model is a component of. Useful for defining the model as a
    #: function of the action spec.
    action_spec: TensorSpec

    #: Model-specific configuration. Passed from the policy and algorithm.
    config: dict[str, Any]

    #: Spec defining the forward pass output. Useful for passing inputs to an
    #: action distribution or stroing values in the replay buffer.
    feature_spec: TensorSpec

    #: Spec defining the forward pass input. Useful for validating the forward
    #: pass and for defining the model as a function of the observation spec.
    observation_spec: TensorSpec

    #: Requirements on how a tensor batch should be preprocessed by the
    #: policy prior to being passed to the forward pass. Useful for handling
    #: sequence shifting or masking so you don't have to.
    #: By default, observations are passed with no shifting.
    view_requirements: dict[str, ViewRequirement]

    def __init__(
        self,
        observation_spec: TensorSpec,
        feature_spec: TensorSpec,
        action_spec: TensorSpec,
        /,
        *,
        config: None | dict[str, Any] = None,
    ) -> None:
        super().__init__()
        self.observation_spec = observation_spec
        self.feature_spec = feature_spec
        self.action_spec = action_spec
        self.config = config if config else {}
        self.view_requirements = {"obs": ViewRequirement(col="obs", shift=0)}

    @abstractmethod
    def forward(self, batch: TensorDict) -> TensorDict:
        """Process a batch of tensors and return features to be fed into an
        action distribution.

        Args:
            batch: A tensordict expected to have at least an "obs" key with any
                tensor spec. The policy that the model is a component of
                processes the batch according to the model's `view_requirements`
                prior to passing the batch to the forward pass.

        Returns:
            Features that will be passed to an action distribution.

        """

    @abstractmethod
    def value_function(self) -> torch.Tensor:
        """Return the value function output for the most recent forward pass.
        Note that a `forward` call has to be performed first before this
        method can return anything.

        This helps prevent extra forward passes from being performed just to
        get a value function output in case the value function and action
        distribution components share parameters.

        """
