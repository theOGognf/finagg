"""Definitions regarding the union of a model and an action distribution."""

from typing import Any

import torch
from tensordict import TensorDict

from ..specs import TensorSpec
from .batch import DEVICE, Batch
from .dist import Distribution
from .model import Model


class Policy:
    """The union of a model and an action distribution.

    This is the main definition used by training algorithms for sampling
    and other data aggregations. It's recommended to use this interface
    when deploying a policy or model such that the action distribution
    is always paired with the model and the model's view requirements are
    always respected.

    Args:
        observation_spec: Spec defining observations from the environment
            and inputs to the model's forward pass.
        action_spec: Spec defining the action distribution's outputs
            and the inputs to the environment.
        model_cls: Model class to use.
        model_config: Model class args.
        dist_cls: Action distribution class.

    """

    #: Hardware device the policy's model is on.
    device: DEVICE

    #: Underlying policy action distribution that's parameterized by
    #: features produced by `model` and the `model` itself.
    dist_cls: type[Distribution]

    #: Underlying policy model that processes environment observations
    #: into a value function approximation and into features to be
    #: consumed by an action distribution for action sampling.
    model: Model

    def __init__(
        self,
        observation_spec: TensorSpec,
        action_spec: TensorSpec,
        model_cls: type[Model],
        model_config: dict[str, Any],
        dist_cls: type[Distribution],
        /,
        *,
        device: DEVICE = "cpu",
    ) -> None:
        self.model = model_cls(observation_spec, action_spec, config=model_config).to(
            device
        )
        self.dist_cls = dist_cls
        self.device = device

    def sample(
        self,
        batch: TensorDict,
        /,
        *,
        kind: str = "last",
        deterministic: bool = False,
        inplace: bool = False,
        requires_grad: bool = False,
        return_logp: bool = False,
        return_values: bool = False,
        return_views: bool = False,
    ) -> TensorDict:
        """Use `batch` to sample from the policy, sampling actions from
        the model and optionally sampling additional values often used for
        training and analysis.

        Args:
            batch: Batch to feed into the policy's underlying model. Expected
                to be of size [B, T, ...] where B is the batch dimension,
                and T is the time or sequence dimension. B is typically
                the number of parallel environments being sampled for during
                massively parallel training, and T is typically the number
                of time steps or observations sampled from the environments.
                The B and T dimensions are typically combined into one dimension
                during batch preprocessing according to the model's view
                requirements.
            kind: String indicating the type of sample to perform. The model's
                view requirements handles preprocessing slightly differently
                depending on the value. Options include:
                    - "last": Sample from `batch` using only the samples
                        necessary to sample for the most recent observations
                        within the `batch`'s T dimension.
                    - "all": Sample from `batch` using all observations within
                        the `batch`'s T dimension.
            deterministic: Whether to sample from the policy deterministically
                (the actions are always the same for the same inputs) or
                stochastically (there is a randomness to the policy's actions).
            inplace: Whether to store policy outputs in the given `batch`
                tensor dict. Otherwise, create a separate tensor dict that
                will only contain policy outputs.
            requires_grad: Whether to enable gradients for the underlying
                model during forward passes. This should only be enabled during
                a training loop or when requiring gradients for explainability
                or other analysis reasons.
            return_logp: Whether to return the log probability of taking the
                sampled actions. Often enabled during a training loop for
                aggregating training data a bit more efficiently.
            return_values: Whether to return the value function approximation
                in the given observations. Often enabled during a training
                loop for aggregating training data a bit more efficiently.
            return_views: Whether to return the observation view requirements
                in the output batch. Even if this flag is enabled, new views
                are only returned if the views are not already present in
                the output batch (i.e., if `inplace` is `True` and the views
                are already in the `batch`, then the returned batch will just
                contain the original views).

        Returns:
            A tensor dict containing AT LEAST actions sampled from the policy.
            See the `Args` section for how the data within the returned tensor
            dict can vary.

        """
        if Batch.VIEWS in batch:
            in_batch = batch[Batch.VIEWS]
        else:
            in_batch = self.model.apply_view_requirements(batch, kind=kind)

        # This is the same mechanism within `torch.no_grad`
        # for enabling/disabling gradients.
        prev = torch.is_grad_enabled()
        torch.set_grad_enabled(requires_grad)

        # Perform inference, sampling, and value approximation.
        features = self.model(in_batch)
        dist = self.dist_cls(features, self.model)
        actions = dist.deterministic_sample() if deterministic else dist.sample()

        # Store required outputs and get/store optional outputs.
        out = (
            batch
            if inplace
            else TensorDict({}, batch_size=in_batch.batch_size, device=batch.device)
        )
        out[Batch.FEATURES] = features
        out[Batch.ACTIONS] = actions
        if return_logp:
            out[Batch.LOGP] = dist.logp(actions)
        if return_values:
            out[Batch.VALUES] = self.model.value_function()
        if return_views:
            out[Batch.VIEWS] = in_batch

        torch.set_grad_enabled(prev)
        return out

    def to(self, device: DEVICE, /) -> "Policy":
        """Move the policy and its attributes to `device`."""
        self.model.to(device)
        self.device = device
        return self
