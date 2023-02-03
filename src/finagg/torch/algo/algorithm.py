"""Definitions related to RL algorithms (mainly variants of PPO)."""

from dataclasses import dataclass
from typing import Any

import torch.optim as optim
from tensordict import TensorDict

from ..optim import DAdaptAdam
from ..specs import CompositeSpec, TensorSpec, UnboundedContinuousTensorSpec
from .batch import DEVICE, Batch
from .dist import Distribution
from .env import Env
from .model import Model
from .policy import Policy
from .scheduler import EntropyScheduler, LRScheduler


@dataclass
class Losses:
    """Collection of losses returned by `Algorithm.step`."""

    #: Entropy of a probability distribution (a measure of a
    #: probability distribution's randomness) loss. This is zero
    #: if the entropy coefficient is zero.
    entropy: float

    #: KL divergence (a measure of distance between two probability
    #: distributions) loss. This is zero if the KL coefficient is zero.
    kl_div: float

    #: Loss associated with a learning algorithm's policy loss.
    #: For PPO, this is a clipped policy loss ratio weighted by advantages.
    policy: float

    #: Loss associated with a policy's model's ability to predict
    #: state values.
    vf: float


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

    buffer: TensorDict

    device: DEVICE

    env: Env

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
        horizons_per_reset: int = 1,
        num_envs: int = 8192,
        optimizer_cls: type[optim.Optimizer] = DAdaptAdam,
        optimizer_config: None | dict[str, Any] = None,
        lr_schedule: None | list[tuple[int, float]] = None,
        lr_schedule_kind: str = "step",
        entropy_coeff: float = 0.0,
        entropy_coeff_schedule: None | list[tuple[int, float]] = None,
        entropy_coeff_schedule_kind: str = "step",
        kl_coeff: float = 1e-4,
        vf_coeff: float = 1.0,
        max_grad_norm: float = 5.0,
        device: DEVICE = "cpu",
    ) -> None:
        self.env = env_cls(num_envs, config=env_config, device=device)
        self.policy = self.init_policy(
            self.env.observation_spec,
            self.env.action_spec,
            model_cls=model_cls,
            model_config=model_config,
            dist_cls=dist_cls,
        )
        if horizon is None:
            if hasattr(self.env, "max_horizon"):
                horizon = self.env.max_horizon
            else:
                horizon = 32
        else:
            horizon = min(horizon, self.env.max_horizon)
        self.buffer = self.init_buffer(
            num_envs,
            horizon + 1,
            self.env.observation_spec,
            self.policy.feature_spec,
            self.env.action_spec,
        )
        self.optimizer = self.init_optimizer(
            self.policy.model, cls=optimizer_cls, config=optimizer_config
        )
        self.lr_scheduler = LRScheduler(
            self.optimizer, schedule=lr_schedule, kind=lr_schedule_kind
        )
        self.entropy_scheduler = EntropyScheduler(
            entropy_coeff,
            schedule=entropy_coeff_schedule,
            kind=entropy_coeff_schedule_kind,
        )
        self.horizons = 0
        self.horizons_per_reset = horizons_per_reset
        self.kl_coeff = kl_coeff
        self.vf_coeff = vf_coeff
        self.max_grad_norm = max_grad_norm
        self.device = device
        self.buffered = False

    def collect(
        self, *, env_config: None | dict[str, Any] = None, deterministic: bool = False
    ) -> TensorDict:
        """Collect environment transitions and policy samples in a buffer.

        This is one of the main `Algorithm` methods. This is usually called
        immediately prior to `step` to collect experiences used
        for learning.

        The environment is reset immediately prior to collecting
        transitions according to the `horizons_per_reset` attribute. If
        the environment isn't reset, then the last observation is used as
        the initial observation.

        This method sets the `buffered` flag to enable calling of the
        `step` method to assure `step` isn't called with dummy data.

        Args:
            env_config: Optional config to pass to the environment's `reset`
                method. This isn't used if the environment isn't scheduled
                to be reset according to the `horizons_per_reset` attribute.
            deterministic: Whether to sample from the policy deterministically.
                This is usally `False` during learning and `True` during
                evaluation.

        Returns:
            The reference to the buffer full of environment experiences and
            sampled policy data (not a copy). Note, the observation key is
            the only valid final time/sequence element in the returned buffer.
            Other keys will contain null/zeroed data.

        """
        # Gather initial observation.
        if not (self.horizons % self.horizons_per_reset):
            self.buffer[Batch.OBS][:, 0, ...] = self.env.reset(config=env_config)
        else:
            self.buffer[Batch.OBS][:, 0, ...] = self.buffer[Batch.OBS][:, -1, ...]

        for t in range(1, self.horizon):
            # Sample the policy and step the environment.
            in_batch = self.buffer[:, :t, ...]
            sample_batch = self.policy.sample(
                in_batch,
                deterministic=deterministic,
                inplace=False,
                requires_grad=False,
                return_logp=True,
                return_values=True,
                return_views=False,
            )
            out_batch = self.env.step(in_batch[Batch.ACTIONS])

            # Update the buffer using sampled policy data and environment
            # transition data.
            self.buffer[Batch.FEATURES][:, t - 1, ...] = sample_batch[Batch.FEATURES]
            self.buffer[Batch.ACTIONS][:, t - 1, ...] = sample_batch[Batch.ACTIONS]
            self.buffer[Batch.LOGP][:, t - 1] = sample_batch[Batch.LOGP]
            self.buffer[Batch.VALUES][:, t - 1] = sample_batch[Batch.VALUES]
            self.buffer[Batch.REWARDS][:, t - 1] = out_batch[Batch.REWARDS]
            self.buffer[Batch.OBS][:, t, ...] = out_batch[Batch.OBS]

        self.horizons += 1
        self.buffered = True
        return self.buffer

    @property
    def horizon(self) -> int:
        """Max number of transitions to run for each environment."""
        return int(self.buffer.size(1))

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

    @staticmethod
    def init_optimizer(
        model: Model,
        /,
        *,
        cls: type[optim.Optimizer] = DAdaptAdam,
        config: None | dict[str, Any] = None,
    ) -> optim.Optimizer:
        """Initialize the optimizer given `model` and its config.

        Args:
            model: The policy's model to update with the optimizer.
            cls: Type of optimizer to use.
            config: Optimizer parameter default overrides.

        Returns:
            A new optimizer instance.

        """
        config = config or {}
        return cls(model.parameters(), **config)

    @staticmethod
    def init_policy(
        observation_spec: TensorSpec,
        action_spec: TensorSpec,
        /,
        *,
        model_cls: None | type[Model] = None,
        model_config: None | dict[str, Any] = None,
        dist_cls: None | type[Distribution] = None,
    ) -> Policy:
        ...

    @property
    def num_envs(self) -> int:
        """Number of environments ran in parallel."""
        return int(self.buffer.size(0))

    def step(self) -> Losses:
        """Take a step with the algorithm, using collected environment
        experiences to update the policy.

        Returns:
            Losses associated with the step.

        """
        if not self.buffered:
            raise RuntimeError(
                f"{self.__class__.__name__} is not buffered. "
                "Call `collect` once prior to `step`."
            )

    def to(self, device: DEVICE, /) -> "Algorithm":
        """Move the algorithm and its attributes to `device`."""
        self.buffer.to(device)
        self.env.to(device)
        self.policy.to(device)
        self.device = device
        return self
