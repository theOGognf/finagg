"""Abstract and canned action distributions."""

from abc import ABC, abstractmethod

import torch
from tensordict import TensorDict

from .model import Model


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
    def kl_div(self, other_dist: "Distribution") -> torch.Tensor:
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


class TorchDistributionWrapper(Distribution):
    """Wrapper class for `torch.distributions`.

    This is taken directly from RLlib:
        https://github.com/ray-project/ray/blob/master/rllib/models/torch/torch_action_dist.py

    """

    #: Underlying PyTorch distribution.
    dist: torch.distributions.Distribution

    def entropy(self) -> torch.Tensor:
        return self.dist.entropy()  # type: ignore

    def kl_div(self, other: "TorchDistributionWrapper") -> torch.Tensor:  # type: ignore[override]
        return torch.distributions.kl.kl_divergence(self.dist, other.dist)

    def logp(self, samples: torch.Tensor) -> torch.Tensor:
        return self.dist.log_prob(samples)  # type: ignore

    def sample(self) -> torch.Tensor:
        return self.dist.sample()  # type: ignore


class Categorical(TorchDistributionWrapper):

    dist: torch.distributions.Categorical

    def __init__(self, features: TensorDict, model: Model) -> None:
        super().__init__(features, model)
        self.dist = torch.distributions.Categorical(logits=features["logits"])  # type: ignore

    def deterministic_sample(self) -> torch.Tensor:
        return self.dist.mode  # type: ignore
