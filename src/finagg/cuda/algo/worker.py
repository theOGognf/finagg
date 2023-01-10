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

    def reset(self, *, seed: None | int = None) -> TensorDict:
        self.env.reset()

    def step(self, action: torch.Tensor) -> TensorDict:
        ...
