"""Definitions related to RL training (mainly variants of PPO)."""

from typing import Any

from tensordict import TensorDict

from ..specs import CompositeSpec, TensorSpec, UnboundedContinuousTensorSpec
from .batch import DEVICE, Batch
from .dist import Distribution
from .env import Env
from .model import Model
from .policy import Policy


class Algorithm:
    """An optimized PPO (https://arxiv.org/pdf/1707.06347.pdf)algorithm
    with common tricks for stabilizing and accelerating learning.

    This algorithm assumes environments are parallelized much like
    IsaacGym environments (https://arxiv.org/pdf/2108.10470.pdf) and
    are infinite horizon with no terminal conditions. These assumptions
    allow the learning procedure to occur extremely fast even for
    complex, sequence-based models because:
        1) environments occur in parallel and are batched into a contingous
            buffer
        2) all environments are reset in parallel after a predetermined
            horizon is reached
        3) and all operations occur on the same device, removing overhead
            associated with data transfers between devices

    Args:
        ...

    """

    env: Env

    buffer: TensorDict

    policy: Policy

    def __init__(
        self,
        env_cls: type[Env],
        /,
        *,
        env_config: None | dict[str, Any] = None,
        model_cls: None | type[Model] = None,
        model_config: None | dict[str, Any] = None,
        dist_cls: None | type[Distribution] = None,
        horizon: None | int = None,
        num_envs: int = 8192,
    ) -> None:
        pass

    @staticmethod
    def init_buffer(
        num_envs: int,
        horizon: int,
        observation_spec: TensorSpec,
        feature_spec: TensorSpec,
        action_spec: TensorSpec,
        /,
    ) -> TensorDict:
        """Initialize the experience buffer with a batch for each environment
        and transition expected from the environment.

        This only initializes environment transition data and doesn't
        necessarily initialize all the data used for learning.

        Args:
            num_envs: Number of environments being simulated in parallel.
            horizon: Number of timesteps to store for each environment.
            observation_spec: Spec defining the policy's model's forward pass
                input.
            feature_spec: Spec defining the policy's model's forward pass
                output.
            action_spec: Spec defining the policy's action distribution
                output.

        Returns:
            A zeroed-out tensordict used for aggregating environment experience
            data.

        """
        buffer_spec = CompositeSpec(
            {
                Batch.OBS: observation_spec,
                Batch.REWARDS: UnboundedContinuousTensorSpec(1),
                Batch.FEATURES: feature_spec,
                Batch.ACTIONS: action_spec,
                Batch.LOGP: UnboundedContinuousTensorSpec(1),
                Batch.VALUES: UnboundedContinuousTensorSpec(1),
            }
        )  # type: ignore
        return buffer_spec.zero([num_envs, horizon])

    def to(self, device: DEVICE, /) -> "Algorithm":
        """Move the algorithm and its attributes to `device`."""
        self.buffer.to(device)
        self.policy.to(device)
        return self
