import gym
from tensordict import TensorDict


class RolloutWorker:
    def __init__(
        self,
        env_cls: type[gym.Env],
        /,
        *,
        env_config: None | dict = None,
        num_envs: None | int = None,
        horizon: None | int = None,
    ) -> None:
        self.env_cls = env_cls
        self.env_config = {} if env_config is None else env_config
        self.env = self.env_cls(env_config)
        self.num_envs = num_envs
        self.horizon = horizon
        self.buffer = TensorDict()

    def reset(self, *, seed: None | int = None):
        self.env.reset()
