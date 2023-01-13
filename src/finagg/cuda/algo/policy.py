from typing import Any

from tensordict import TensorDict
from torchrl.data import TensorSpec

from .dist import Distribution
from .model import Model


class Policy:
    def __init__(
        self,
        observation_spec: TensorSpec,
        feature_spec: TensorSpec,
        action_spec: TensorSpec,
        model_cls: type[Model],
        model_config: dict[str, Any],
        dist_cls: type[Distribution],
    ) -> None:
        self.model = model_cls(
            observation_spec, feature_spec, action_spec, config=model_config
        )
        self.dist_cls = dist_cls

    def sample(
        self,
        batch: TensorDict,
        /,
        *,
        deterministic: bool = False,
        return_values: bool = False,
        return_logp: bool = False,
    ) -> TensorDict:
        ...
