"""Abstract and canned action distributions."""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import torch
from tensordict import TensorDict
from typing_extensions import Self

from ..specs import (
    CompositeSpec,
    DiscreteTensorSpec,
    TensorSpec,
    UnboundedContinuousTensorSpec,
)
from .model import Model

_ActionSpec = TypeVar("_ActionSpec", bound=TensorSpec)
_FeatureSpec = TypeVar("_FeatureSpec", bound=TensorSpec)


class Distribution(ABC):
    """Policy component that defines a probability distribution over a
    feature set from a model.

    This definition is largely inspired by RLlib's action distribution:
        https://github.com/ray-project/ray/blob/master/rllib/models/action_dist.py

    Most commonly, the feature set is a single vector of logits or log
    probabilities used for defining and sampling from the probability
    distribution. Custom probabiltiy distributions, however, are not
    constrained to just a single vector.

    Args:
        features: Features from `model`'s forward pass.
        model: Model for parameterizing the probability distribution.

    """

    #: Features from `model` forward pass. Simple action distributions
    #: expect one field and corresponding tensor in the tensor dict,
    #: but custom action distributions can return any kind of tensor
    #: dict from `model`.
    features: TensorDict

    #: Model from the parent policy also passed to the action distribution.
    #: This is necessary in case the model has components that're only
    #: used for sampling or probability distribution characteristics
    #: computations.
    model: Model

    def __init__(self, features: TensorDict, model: Model) -> None:
        super().__init__()
        self.features = features
        self.model = model

    @abstractmethod
    def deterministic_sample(self) -> torch.Tensor | TensorDict:
        """Draw a deterministic sample from the probability distribution."""

    @abstractmethod
    def entropy(self) -> torch.Tensor:
        """Compute the probability distribution's entropy (a measurement
        of randomness).

        """

    @abstractmethod
    def kl_div(self, other: Self) -> torch.Tensor:
        """Compute the KL-divergence (a measurement of the difference
        between two distributions) between two distributions (often of the
        same type).

        """

    @abstractmethod
    def logp(self, samples: torch.Tensor | TensorDict) -> torch.Tensor:
        """Compute the log probability of sampling `samples` from the probability
        distribution.

        """

    @abstractmethod
    def sample(self) -> torch.Tensor | TensorDict:
        """Draw a stochastic sample from the probability distribution."""


class TorchDistributionWrapper(Distribution, Generic[_ActionSpec, _FeatureSpec]):
    """Wrapper class for `torch.distributions`.

    This is taken directly from RLlib:
        https://github.com/ray-project/ray/blob/master/rllib/models/torch/torch_action_dist.py

    """

    #: Underlying PyTorch distribution.
    dist: torch.distributions.Distribution

    def entropy(self) -> torch.Tensor:
        return self.dist.entropy()  # type: ignore[no-any-return, no-untyped-call]

    def kl_div(self, other: Self) -> torch.Tensor:
        return torch.distributions.kl.kl_divergence(self.dist, other.dist)

    def logp(self, samples: torch.Tensor) -> torch.Tensor:
        return self.dist.log_prob(samples)  # type: ignore[no-any-return, no-untyped-call]

    @staticmethod
    @abstractmethod
    def required_feature_spec(action_spec: _ActionSpec, /) -> _FeatureSpec:
        """Define feature spec requirements for the distribution given an
        action spec.

        Wrapper distributions only support one action spec type given by
        the generic `_S`.

        """

    def sample(self) -> torch.Tensor:
        return self.dist.sample()  # type: ignore[no-any-return, no-untyped-call]


class Categorical(TorchDistributionWrapper[DiscreteTensorSpec, CompositeSpec]):

    dist: torch.distributions.Categorical

    def __init__(self, features: TensorDict, model: Model) -> None:
        super().__init__(features, model)
        self.dist = torch.distributions.Categorical(logits=features["logits"])  # type: ignore[no-untyped-call]

    def deterministic_sample(self) -> torch.Tensor:
        return self.dist.mode  # type: ignore[no-any-return]

    @staticmethod
    def required_feature_spec(action_spec: DiscreteTensorSpec, /) -> CompositeSpec:
        return CompositeSpec(
            logits=UnboundedContinuousTensorSpec(
                shape=action_spec.space.n, device=action_spec.device
            )
        )  # type: ignore[no-untyped-call]


class DiagGaussian(
    TorchDistributionWrapper[UnboundedContinuousTensorSpec, CompositeSpec]
):

    dist: torch.distributions.Normal

    def __init__(self, features: TensorDict, model: Model) -> None:
        super().__init__(features, model)
        self.dist = torch.distributions.Normal(loc=features["mean"], scale=torch.exp(features["log_std"]))  # type: ignore[no-untyped-call]

    def deterministic_sample(self) -> torch.Tensor:
        return self.dist.mode  # type: ignore[no-any-return]

    def entropy(self) -> torch.Tensor:
        return super().entropy().sum(-1)

    def kl_div(self, other: Self) -> torch.Tensor:
        return super().kl_div(other).sum(-1)

    def logp(self, samples: torch.Tensor) -> torch.Tensor:
        return super().logp(samples).sum(-1)

    @staticmethod
    def required_feature_spec(
        action_spec: UnboundedContinuousTensorSpec, /
    ) -> CompositeSpec:
        return CompositeSpec(
            mean=UnboundedContinuousTensorSpec(
                shape=action_spec.shape, device=action_spec.device
            ),
            log_std=UnboundedContinuousTensorSpec(
                shape=action_spec.shape, device=action_spec.device
            ),
        )  # type: ignore[no-untyped-call]
