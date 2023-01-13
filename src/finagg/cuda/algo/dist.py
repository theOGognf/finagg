from abc import ABC, abstractmethod

import torch
from tensordict import TensorDict

from .model import Model


class Distribution(ABC):
    features: TensorDict

    model: Model

    def __init__(self, features: TensorDict, model: Model) -> None:
        super().__init__()
        self.features = features
        self.model = model

    @abstractmethod
    def kl_div(self, other_dist: "Distribution") -> torch.Tensor:
        ...

    @abstractmethod
    def sample(self, batch: TensorDict) -> TensorDict:
        ...
