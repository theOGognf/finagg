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
    """An optimized PPO (https://arxiv.org/pdf/1707.06347.pdf) algorithm
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

    #: Environment experiences buffer used for aggregating environment
    #: transition data and policy sample data. The same buffer object
    #: is shared whenever using `collect`, so it's important to use data
    #: collected by `collect`. Otherwise, it'll be overwritten by
    #: subsequent `collect` calls. Buffer dimensions are constructed
    #: by `num_envs` and `horizon`.
    buffer: TensorDict

    #: Flag indicating whether `collect` has been called at least once
    #: prior to calling `step`. Ensures dummy buffer data isn't used
    #: to update the policy.
    buffered: bool

    #: PPO hyperparameter indicating the max distance the policy can
    #: update away from previously collected policy sample data with
    #: respect to likelihoods of taking actions conditioned on
    #: observations. This is the main innovation of PPO.
    clip_param: float

    #: Device the `env`, `buffer`, and `policy` all reside on.
    device: DEVICE

    #: Entropy scheduler for updating the `entropy_coeff` after each `step`
    #: call based on the number environment transitions collected and
    #: learned on. By default, the entropy scheduler does not actually
    #: update the entropy coefficient. The entropy scheduler only updates
    #: the entropy coefficient if an `entropy_coeff_schedule` is provided.
    entropy_scheduler: EntropyScheduler

    #: Environment used for experience collection within the `collect` method.
    #: It's ultimately on the environment to make learning efficient by
    #: parallelizing simulations.
    env: Env

    #: Generalized Advantage Estimation (GAE) hyperparameter for controlling
    #: the variance and bias tradeoff when estimating the state value
    #: function from collected environment transitions. A higher value
    #: allows higher variance while a lower value allows higher bias
    #: estimation but lower variance.
    gae_lambda: float

    #: Discount reward factor often used in the Bellman operator for
    #: controlling the variance and bias tradeoff in collected experienced
    #: rewards. Note, this does not control the bias/variance of the
    #: state value estimation and only controls the weight future rewards
    #: have on the total discounted return.
    gamma: float

    #: Running count of number of environment horizons reach (number of
    #: calls to `collect`). Used for tracking when to reset `env` based
    #: on `horizons_per_reset`.
    horizons: int

    #: Number of times `collect` can be called before resetting `env`.
    #: Set this to a higher number if you want learning to occur across
    #: horizons. Leave this as the default `1` if it doesn't matter that
    #: experiences and learning only occurs within one horizon.
    horizons_per_reset: int

    kl_coeff: float

    kl_target: float

    #: Learning rate scheduler for updating `optimizer` learning rate after
    #: each `step` call based on the number of environment transitions
    #: collected and learned on. By default, the learning scheduler does not
    #: actually alter the `optimizer` learning rate (it actually leaves it
    #: constant). The learning rate scheduler only alters the learning rate
    #: if a `learning_rate_schedule` is provided.
    lr_scheduler: LRScheduler

    #: Max gradient norm allowed when updating the policy's model within `step`.
    max_grad_norm: float

    #: PPO hyperparameter indicating the number of gradient steps to take
    #: with the whole `buffer` when calling `step`.
    num_sgd_iter: int

    #: Underlying optimizer for updating the policy's model. Constructed from
    #: `optimizer_cls` and `optimizer_config`. Defaults to a generally robust
    #: optimizer that doesn't require much hyperparameter tuning.
    optimizer: optim.Optimizer

    #: Policy constructed from the `model_cls`, `model_config`, and `dist_cls`
    #: kwargs. A default policy is constructed according to the environment's
    #: observation and action specs if these policy args aren't provided.
    #: The policy is what does all the action sampling within `collect` and
    #: is what is updated within `step`.
    policy: Policy

    sgd_minibatch_size: int

    shuffle_minibatches: bool

    vf_clip_param: float

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
        gae_lambda: float = 0.95,
        gamma: float = 0.95,
        sgd_minibatch_size: float = -1,
        num_sgd_iter: int = 4,
        shuffle_minibatches: bool = True,
        clip_param: float = 0.2,
        vf_clip_param: float = 5.0,
        kl_coeff: float = 1e-4,
        kl_target: float = 1e-2,
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
        self.gae_lambda = gae_lambda
        self.gamma = gamma
        self.sgd_minibatch_size = sgd_minibatch_size
        self.num_sgd_iter = num_sgd_iter
        self.shuffle_minibatches = shuffle_minibatches
        self.clip_param = clip_param
        self.vf_clip_param = vf_clip_param
        self.kl_coeff = kl_coeff
        self.kl_target = kl_target
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

        for t in range(self.horizon):
            # Sample the policy and step the environment.
            in_batch = self.buffer[:, : (t + 1), ...]
            sample_batch = self.policy.sample(
                in_batch,
                deterministic=deterministic,
                inplace=False,
                requires_grad=False,
                return_logp=True,
                return_values=True,
                return_views=False,
            )
            out_batch = self.env.step(sample_batch[Batch.ACTIONS])

            # Update the buffer using sampled policy data and environment
            # transition data.
            self.buffer[Batch.FEATURES][:, t, ...] = sample_batch[Batch.FEATURES]
            self.buffer[Batch.ACTIONS][:, t, ...] = sample_batch[Batch.ACTIONS]
            self.buffer[Batch.LOGP][:, t] = sample_batch[Batch.LOGP]
            self.buffer[Batch.VALUES][:, t] = sample_batch[Batch.VALUES]
            self.buffer[Batch.REWARDS][:, t] = out_batch[Batch.REWARDS]
            self.buffer[Batch.OBS][:, t + 1, ...] = out_batch[Batch.OBS]

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
