from typing import Any

import torch
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
        inplace: bool = False,
        requires_grad: bool = False,
        return_logp: bool = False,
        return_values: bool = False,
    ) -> TensorDict:
        # This is the same mechanism within `torch.no_grad`.
        prev = torch.is_grad_enabled()
        torch.set_grad_enabled(requires_grad)

        out = batch if inplace else TensorDict({}, batch_size=batch.batch_size)

        # Process view requirements, reshaping tensors as-needed
        # by the model.
        processed_batch = TensorDict({}, batch_size=batch.batch_size)
        for key, view_requirement in self.model.view_requirements.items():
            processed_batch[key] = view_requirement.process(batch)

        # Perform inference, sampling, and value approximation.
        features = self.model(processed_batch)
        dist = self.dist_cls(features, self.model)
        actions = dist.deterministic_sample() if deterministic else dist.sample()
        if return_logp:
            out["logp"] = dist.logp(actions)
        if return_values:
            out["values"] = self.model.value_function()

        torch.set_grad_enabled(prev)
        return out
