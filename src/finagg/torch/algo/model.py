"""Abstract model definition."""

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any

import torch
from tensordict import TensorDict

from ..specs import (
    CompositeSpec,
    DiscreteTensorSpec,
    MultiDiscreteTensorSpec,
    TensorSpec,
    UnboundedContinuousTensorSpec,
)
from .data import DataKeys
from .view import VIEW_KIND, ViewRequirement


class Model(ABC, torch.nn.Module):
    """Policy component that processes environment observations into
    a value function approximation and features to be consumed by an
    action distribution for action sampling.

    This definition is largely inspired by RLlib's model concept:
        https://github.com/ray-project/ray/blob/master/rllib/models/modelv2.py.

    The model is intended to be called with the forward pass (like any
    other PyTorch module) to get the inputs to the policy's action
    distribution. It's expected that the value function approximation
    is stored after each forward pass in some intermediate attribute
    and can be accessed with a subsequent call to `value_function`.

    Example data flow (adapted from the RLlib reference above):
        batch -> forward() -> features
            \-> value_function() -> V(s)

    Args:
        observation_spec: Spec defining the forward pass input.
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
    #: action distribution or stroing values in the replay buffer. Defaults
    #: to `action_spec`. This should be overwritten in a model's `__init__`.
    feature_spec: TensorSpec

    #: Spec defining the forward pass input. Useful for validating the forward
    #: pass and for defining the model as a function of the observation spec.
    observation_spec: TensorSpec

    #: Requirements on how a tensor batch should be preprocessed by the
    #: policy prior to being passed to the forward pass. Useful for handling
    #: sequence shifting or masking so you don't have to.
    #: By default, observations are passed with no shifting.
    #: This should be overwritten in a model's `__init__`.
    view_requirements: dict[str, ViewRequirement]

    def __init__(
        self,
        observation_spec: TensorSpec,
        action_spec: TensorSpec,
        /,
        *,
        config: None | dict[str, Any] = None,
    ) -> None:
        super().__init__()
        self.observation_spec = observation_spec
        self.action_spec = action_spec
        self.config = config if config else {}
        self.feature_spec = self.default_feature_spec(action_spec)
        self.view_requirements = {DataKeys.OBS: ViewRequirement(DataKeys.OBS, shift=0)}

    def __call__(self, *args: Any, **kwds: Any) -> torch.Tensor | TensorDict:
        """Call the model's forward pass. This just supplies a type signature."""
        return super().__call__(*args, **kwds)

    def apply_view_requirements(
        self, batch: TensorDict, /, *, kind: VIEW_KIND = "last"
    ) -> TensorDict:
        """Apply the model's view requirements, reshaping tensors as-needed.

        This is usually called by the policy that the model is a component
        of, but can be used within the model if the model is deployed without
        the policy or action distribution.

        Args:
            batch: Batch to feed into the policy's underlying model. Expected
                to be of size [B, T, ...] where B is the batch dimension,
                and T is the time or sequence dimension. B is typically
                the number of parallel environments being sampled for during
                massively parallel training, and T is typically the number
                of time steps or observations sampled from the environments.
                The B and T dimensions are typically combined into one dimension
                during application of the view requirements.
            kind: String indicating the type of view requirements to apply.
                The model's view requirements are applied slightly differently
                depending on the value. Options include:
                    - "last": Apply the view requirements using only the samples
                        necessary to sample for the most recent observations
                        within the `batch`'s T dimension.
                    - "all": Sample from `batch` using all observations within
                        the `batch`'s T dimension. Expand the B and T dimensions
                        together.

        """
        batch_sizes = {}
        out = {}
        for key, view_requirement in self.view_requirements.items():
            match kind:
                case "all":
                    item = view_requirement.apply_all(batch)
                case "last":
                    item = view_requirement.apply_last(batch)
            out[key] = item
            B_NEW = item.size(0)
            batch_sizes[key] = B_NEW
        batch_size = next(iter(batch_sizes.values()))
        return TensorDict(out, batch_size=batch_size, device=batch.device)

    @property
    def burn_size(self) -> int:
        """Return the model's burn size (also the burn size for all view
        requirements.

        """
        burn_sizes = {}
        for key, view_requirement in self.view_requirements.items():
            burn_sizes[key] = view_requirement.burn_size
        return next(iter(burn_sizes.values()))

    @staticmethod
    def default_feature_spec(action_spec: TensorSpec) -> TensorSpec:
        """Return a default feature spec given an action spec.

        Useful for defining feature specs for simple and common action
        specs. Custom models with complex action specs should define
        their own custom feature specs as an attribute.

        Args:
            action_spec: Spec defining the outputs of the policy's action
                distribution that this model is a component of. Typically
                passed into `Model.__init__`.

        Returns:
            A spec defining the inputs to the policy's action distribution.
            For simple distributions (e.g., categorical or diagonal gaussian),
            this returns a spec defining the inputs to those distributions
            (e.g., logits and mean/scales, respectively). For complex
            distributions, this returns a copy of the action spec and the model
            is expected to assign the correct feature spec within its own
            `__init__`.

        """
        match action_spec:
            case DiscreteTensorSpec():
                return CompositeSpec(
                    logits=UnboundedContinuousTensorSpec(
                        shape=action_spec.space.n, device=action_spec.device
                    )
                )  # type: ignore
            case MultiDiscreteTensorSpec():
                return CompositeSpec(
                    logits=UnboundedContinuousTensorSpec(
                        shape=action_spec.space.n, device=action_spec.device
                    )
                )  # type: ignore
            case UnboundedContinuousTensorSpec():
                return CompositeSpec(
                    mean=UnboundedContinuousTensorSpec(
                        shape=action_spec.shape, device=action_spec.device
                    ),
                    log_std=UnboundedContinuousTensorSpec(
                        shape=action_spec.shape, device=action_spec.device
                    ),
                )  # type: ignore
            case _:
                return deepcopy(action_spec)

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

    def validate_view_requirements(self) -> None:
        """Helper for validating a model's view requirements.

        Raises:
            RuntimeError if the model's view requirements result in an
            ambiguous batch size, making training and sampling impossible.

        """
        burn_sizes = {}
        for key, view_requirement in self.view_requirements.items():
            burn_sizes[key] = view_requirement.burn_size
        if len(set(burn_sizes.values())) > 1:
            raise RuntimeError(
                f"""{self} view requirements with burn sizes {burn_sizes}
                result in an ambiguous batch size. It's recommended you:
                    1) use a view requirement method that does not have sample
                        burn, allowing view requirements with different sizes
                    2) reformulate your model and observation function such
                        that view requirements are not necessary or are
                        handled internal to your environment

                """
            )

    @abstractmethod
    def value_function(self) -> torch.Tensor:
        """Return the value function output for the most recent forward pass.
        Note that a `forward` call has to be performed first before this
        method can return anything.

        This helps prevent extra forward passes from being performed just to
        get a value function output in case the value function and action
        distribution components share parameters.

        """
