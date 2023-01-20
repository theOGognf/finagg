from typing import Any

import gym
import torch
from gym.core import ActType, ObsType
from tensordict import TensorDict


class RolloutWorker:
    def __init__(
        self,
        env_cls: type[gym.Env[ObsType, ActType]],
        /,
        *,
        env_config: None | dict[str, Any] = None,
        horizon: None | int = None,
        num_envs: int = 8192,
        rollout_fragment_length: int = 200,
    ) -> None:
        self.env_cls = env_cls
        self.env_config = {} if env_config is None else env_config
        self.env = self.env_cls()
        if not horizon:
            if self.env.spec and self.env.spec.max_episode_steps:
                horizon = self.env.spec.max_episode_steps
        self.horizon = horizon
        self.num_envs = num_envs
        self.rollout_fragment_length = rollout_fragment_length
        self.buffer = TensorDict()

    def postprocess_obs(self) -> TensorDict:
        ...

    def reset(self) -> TensorDict:
        obs = self.env.reset(
            seed=self.env_config.get("seed", None),
            return_info=False,
            options=self.env_config,
        )
        self.buffer["obs"][..., 0, :] = obs
        return obs

    def step(self, action: torch.Tensor) -> TensorDict:
        ...
