"""Abstract model definition."""

from abc import ABC, abstractmethod
from typing import Any

import torch
from tensordict import TensorDict
from torchrl.data import TensorSpec

from .batch import Batch
from .view import ViewRequirement


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
        self.view_requirements = {
            str(Batch.OBS): ViewRequirement(str(Batch.OBS), shift=0)
        }

    def __call__(self, *args: Any, **kwds: Any) -> torch.Tensor | TensorDict:
        """Call the model's forward pass. This just supplies a type signature."""
        return super().__call__(*args, **kwds)

    def apply_view_requirements(
        self, batch: TensorDict, /, *, kind: str = "last"
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
        in_batch = TensorDict({}, batch_size=[])
        for key, view_requirement in self.view_requirements.items():
            match kind:
                case "all":
                    item = view_requirement.apply_all(batch)
                case "last":
                    item = view_requirement.apply_last(batch)
            in_batch[key] = item
            B_NEW = item.size(0)
            batch_sizes[key] = B_NEW
        batch_size = next(iter(batch_sizes.values()))
        in_batch.batch_size = batch_size
        return in_batch

    @property
    def burn_size(self) -> int:
        """Return the model's burn size (also the burn size for all view
        requirements.

        """
        burn_sizes = {}
        for key, view_requirement in self.view_requirements.items():
            burn_sizes[key] = view_requirement.burn_size
        return next(iter(burn_sizes.values()))

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

    def validate(self, batch: TensorDict) -> None:
        """Helper for validating that a batch is compliant with a model's
        tensor specs. This should really only needed to be called once
        during training or deployment.

        Args:
            batch: Batch with output from the model. Expected to be of size
                [B, T, ...] where B is the batch dimension, and T is the
                time or sequence dimension. B is typically the number of
                parallel environments being sampled for during massively
                parallel training, and T is typically the number of time steps
                or observations sampled from the environments. The B and T
                dimensions are typically combined into one dimension during
                application of the view requirements.

        Raises:
            - AssertionError if any of the elements within `batch` are not
                consistent with the model's tensor specs.
            - RuntimeError if the model's view requirements result in an
                ambiguous batch size, making training and sampling impossible.

        """
        # First check that the view requirements all have the same burn size.
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

        # Check the batch elements are all consistent with the tensor specs.
        last_batch = batch[:, -1, ...]

        obs = last_batch[str(Batch.OBS)]
        self.observation_spec.assert_is_in(obs)

        features = last_batch[str(Batch.FEATURES)]
        self.feature_spec.assert_is_in(features)

        actions = last_batch[str(Batch.ACTIONS)]
        self.action_spec.assert_is_in(actions)

    @abstractmethod
    def value_function(self) -> torch.Tensor:
        """Return the value function output for the most recent forward pass.
        Note that a `forward` call has to be performed first before this
        method can return anything.

        This helps prevent extra forward passes from being performed just to
        get a value function output in case the value function and action
        distribution components share parameters.

        """
