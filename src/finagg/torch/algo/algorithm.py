"""Definitions related to RL algorithms (mainly variants of PPO)."""

from typing import Any, TypedDict

import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from tensordict import TensorDict
from torch.utils.data import DataLoader

from ..optim import DAdaptAdam
from ..specs import CompositeSpec, TensorSpec, UnboundedContinuousTensorSpec
from .batch import DEVICE, Batch
from .dist import Distribution
from .env import Env
from .model import Model
from .policy import Policy
from .scheduler import SCHEDULE_KIND, EntropyScheduler, KLUpdater, LRScheduler

StepData = TypedDict(
    "StepData",
    {
        "coefficients/entropy": float,
        "coefficients/kl_div": float,
        "coefficients/vf": float,
        "losses/entropy": float,
        "losses/kl_div": float,
        "losses/policy": float,
        "losses/vf": float,
        "losses/total": float,
        "rewards/min": float,
        "rewards/max": float,
        "rewards/mean": float,
        "rewards/std": float,
    },
)


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
        env_cls: Highly parallelized environment for sampling experiences.
            Instantiated with `env_config`. Will be stepped for `horizon`
            each `collect` call.
        env_config: Initial environment config passed to `env_cls` for the
            environment instantiation. This is likely to be overwritten
            on the environment instance if reset with a new config.
        model_cls: Optional custom policy model definition. A model class
            is provided for you based on the environment instance's specs
            if you don't provide one. Defaults to a simple feedforward
            neural network.
        model_config: Optional policy model config unpacked into the model
            during instantiation.
        dist_cls: Custom policy action distribution class. An action
            distribution class is provided for you based on the environment
            instance's specs. Defaults to a categorical action distribution
            for discrete actions and a gaussian action distribution for
            continuous actions. Complex actions are not supported for default
            action distributions.
        horizon: Number of environment transitions to collect during `collect`.
            The environment is reset based on `horizons_per_reset`. The
            buffer's size is [B, T] where T is `horizon`.
        horizons_per_reset: Number of times `collect` can be called before
            resetting `env`. Set this to a higher number if you want learning
            to occur across horizons. Leave this as the default `1` if it
            doesn't matter that experiences and learning only occurs within
            one horizon.
        num_envs: Number of parallelized simulation environments for the
            environment instance. Passed during the environment's
            instantiation. The buffer's size is [B, T] where B is `num_envs`.
        optimizer_cls: Custom optimizer class. Defaults to an optimizer
            that doesn't require much tuning.
        optimizer_config: Custom optimizer config unpacked into `optimizer_cls`
            during optimizer instantiation.
        lr_schedule: Optional schedule that overrides the optimizer's learning rate.
            This deternmines the value of the learning rate according to the
            number of environment transitions experienced during learning.
            The learning rate is constant if this isn't provided.
        lr_schedule_kind: Kind of learning rate scheduler to use if `lr_schedule`
            is provided. Options include:
                - "step": jump to values and hold until a new environment transition
                    count is reached.
                - "interp": jump to values like "step", but interpolate between the
                    current value and the next value.
        entropy_coeff: Entropy coefficient value. Weight of the entropy loss w.r.t.
            other components of total loss. This value is ignored if
            `entropy_coeff_schedule` is provded.
        entropy_coeff_schedule: Optional schedule that overrides `entropy_coeff`. This
            determines values of `entropy_coeff` according to the number of environment
            transitions experienced during learning.
        entropy_coeff_schedule_kind: Kind of entropy scheduler to use. Options include:
            - "step": jump to values and hold until a new environment transition
                count is reached.
            - "interp": jump to values like "step", but interpolate between the
                current value and the next value.
        gae_lambda: Generalized Advantage Estimation (GAE) hyperparameter for controlling
            the variance and bias tradeoff when estimating the state value
            function from collected environment transitions. A higher value
            allows higher variance while a lower value allows higher bias
            estimation but lower variance.
        gamma: Discount reward factor often used in the Bellman operator for
            controlling the variance and bias tradeoff in collected experienced
            rewards. Note, this does not control the bias/variance of the
            state value estimation and only controls the weight future rewards
            have on the total discounted return.
        sgd_minibatch_size: PPO hyperparameter indicating the minibatc size `buffer`
            is split into when updating the policy's model in `step`. It's usually best to
            maximize the minibatch size to reduce the variance associated with
            updating the policy's model, but also accelerate the computations
            when learning (assuming a CUDA device is being used). If `None`,
            the whole buffer is treated as one giant batch.
        num_sgd_iter: PPO hyperparameter indicating the number of gradient steps to take
            with the whole `buffer` when calling `step`.
        shuffle_minibatches: Whether to shuffle minibatches within `step`.
            Recommended, but not necessary if the minibatch size is large enough
            (e.g., the buffer is the batch).
        clip_param: PPO hyperparameter indicating the max distance the policy can
            update away from previously collected policy sample data with
            respect to likelihoods of taking actions conditioned on
            observations. This is the main innovation of PPO.
        vf_clip_param: PPO hyperparameter similar to `clip_param` but for the
            value function estimate. A measure of max distance the model's
            value function is allowed to update away from previous value function
            samples.
        kl_coeff: KL divergence loss coefficient that weighs KL divergence loss w.r.t.
            other loss components. KL divergence loss is ignored and not computed
            unless this is `> 0`. This is updated to make the mean KL divergence loss
            be close to `kl_target`. If the mean KL divergence is higher than `target`,
            then `coeff` is increased to increase the weight of the KL divergence
            in the loss, thus decreasing subsequent sampled mean KL divergence losses.
        kl_target: Target KL divergence. The desired distance between new and old
            policies. Used for updating `kl_coeff`.
        vf_coeff: PPO hyperparameter similar to `clip_param` but for the value function
            estimate. A measure of max distance the model's value function is
            allowed to update away from previous value function samples.
        max_grad_norm: Max gradient norm allowed when updating the policy's model
            within `step`.
        device: Device the `env`, `buffer`, and `policy` all reside on.

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

    #: KL divergence updater used for updating `kl_coeff` to make the sampled
    #: mean KL divergence be close to `kl_target`.
    kl_updater: KLUpdater

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

    #: PPO hyperparameter indicating the minibatc size `buffer` is split into
    #: when updating the policy's model in `step`. It's usually best to
    #: maximize the minibatch size to reduce the variance associated with
    #: updating the policy's model, but also accelerate the computations
    #: when learning (assuming a CUDA device is being used). If `None`,
    #: the whole buffer is treated as one giant batch.
    sgd_minibatch_size: None | int

    #: Whether to shuffle minibatches within `step`. Recommended, but not
    #: necessary if the minibatch size is large enough (e.g., the buffer
    #: is the batch).
    shuffle_minibatches: bool

    #: PPO hyperparameter similar to `clip_param` but for the value function
    #: estimate. A measure of max distance the model's value function is
    #: allowed to update away from previous value function samples.
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
        lr_schedule_kind: SCHEDULE_KIND = "step",
        entropy_coeff: float = 0.0,
        entropy_coeff_schedule: None | list[tuple[int, float]] = None,
        entropy_coeff_schedule_kind: SCHEDULE_KIND = "step",
        gae_lambda: float = 0.95,
        gamma: float = 0.95,
        sgd_minibatch_size: None | int = -1,
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
        self.kl_updater = KLUpdater(kl_coeff, target=kl_target)
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
                kind="last",
                deterministic=deterministic,
                inplace=False,
                requires_grad=False,
                return_actions=True,
                return_logp=True,
                return_values=True,
                return_views=False,
            )
            out_batch = self.env.step(sample_batch[Batch.ACTIONS])

            # Update the buffer using sampled policy data and environment
            # transition data.
            self.buffer[Batch.FEATURES][:, t, ...] = sample_batch[Batch.FEATURES]
            self.buffer[Batch.ACTIONS][:, t, ...] = sample_batch[Batch.ACTIONS]
            self.buffer[Batch.LOGP][:, t, ...] = sample_batch[Batch.LOGP]
            self.buffer[Batch.VALUES][:, t, ...] = sample_batch[Batch.VALUES]
            self.buffer[Batch.REWARDS][:, t, ...] = out_batch[Batch.REWARDS]
            self.buffer[Batch.OBS][:, t + 1, ...] = out_batch[Batch.OBS]

        # Sample features and value function at last observation.
        in_batch = self.buffer[:, :, ...]
        sample_batch = self.policy.sample(
            in_batch,
            kind="last",
            deterministic=deterministic,
            inplace=False,
            requires_grad=False,
            return_actions=False,
            return_logp=False,
            return_values=True,
            return_views=False,
        )
        self.buffer[Batch.FEATURES][:, -1, ...] = sample_batch[Batch.FEATURES]
        self.buffer[Batch.VALUES][:, -1, ...] = sample_batch[Batch.VALUES]

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
                Batch.ADVANTAGES: UnboundedContinuousTensorSpec(1),
                Batch.RETURNS: UnboundedContinuousTensorSpec(1),
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

    def step(self) -> StepData:
        """Take a step with the algorithm, using collected environment
        experiences to update the policy.

        Returns:
            Data associated with the step and current algorithm hyperparameter
            values.

        """
        if not self.buffered:
            raise RuntimeError(
                f"{self.__class__.__name__} is not buffered. "
                "Call `collect` once prior to `step`."
            )

        # Get number of environments and horizon. Remember, there's an extra
        # sample in the horizon because we store the final environment observation
        # for the next `collect` call and value function estimate for bootstrapping.
        B = self.num_envs
        T = self.horizon - 1

        # Generalized Advantage Estimation (GAE) and returns bootstrapping.
        prev_advantage = 0.0
        for t in reversed(range(T)):
            delta = self.buffer[Batch.REWARDS][:, t, ...] + (
                self.gamma * self.buffer[Batch.VALUES][:, t + 1, ...]
                - self.buffer[Batch.VALUES][:, t, ...]
            )
            self.buffer[Batch.ADVANTAGES][:, t, ...] = prev_advantage = delta + (
                self.gamma * self.gae_lambda * prev_advantage
            )
        self.buffer[Batch.RETURNS] = (
            self.buffer[Batch.ADVANTAGES] + self.buffer[Batch.VALUES]
        )

        # Batchify the buffer. Save the last sample for adding it back to the
        # buffer. Remove the last sample afterwards since it contains dummy
        # data.
        final_obs = self.buffer[Batch.OBS][:, -1, ...]
        self.buffer = self.buffer[:, :-1, ...]
        views = self.policy.model.apply_view_requirements(self.buffer, kind="all")
        self.buffer = self.buffer.reshape(-1)
        self.buffer[Batch.VIEWS] = views

        # Main PPO loop.
        step_data: list[StepData] = []
        loader = DataLoader(
            self.buffer,
            batch_size=self.sgd_minibatch_size,
            shuffle=self.shuffle_minibatches,
            collate_fn=lambda x: x,
        )
        for _ in range(self.num_sgd_iter):
            for minibatch in loader:
                sample_batch = self.policy.sample(
                    minibatch,
                    kind="all",
                    deterministic=False,
                    inplace=False,
                    requires_grad=True,
                    return_actions=False,
                    return_logp=False,
                    return_values=True,
                    return_views=False,
                )

                # Get action distributions and their log probability ratios.
                prev_action_dist = self.policy.dist_cls(
                    minibatch[Batch.FEATURES], self.policy.model
                )
                curr_action_dist = self.policy.dist_cls(
                    sample_batch[Batch.FEATURES], self.policy.model
                )
                logp_ratio = torch.exp(
                    curr_action_dist.logp(minibatch[Batch.ACTIONS])
                    - minibatch[Batch.LOGP]
                )

                # Compute main, required losses.
                vf_loss = torch.clamp(
                    torch.pow(
                        sample_batch[Batch.VALUES] - minibatch[Batch.VALUES], 2.0
                    ),
                    0.0,
                    self.vf_clip_param,
                ).mean()
                policy_loss = torch.min(
                    minibatch[Batch.ADVANTAGES] * logp_ratio,
                    minibatch[Batch.ADVANTAGES]
                    * torch.clamp(logp_ratio, 1 - self.clip_param, 1 + self.clip_param),
                ).mean()
                entropy_loss = curr_action_dist.entropy().mean()

                # Maximize entropy, maximize policy actions associated with high advantages,
                # minimize discounted return estimation error.
                total_loss = (
                    self.vf_coeff * vf_loss
                    - policy_loss
                    - self.entropy_scheduler.coeff * entropy_loss
                )

                # Optional KL divergence loss.
                if self.kl_updater.initial_coeff > 0:
                    kl_div_loss = prev_action_dist.kl_div(curr_action_dist).mean()
                    total_loss += self.kl_updater.coeff * kl_div_loss
                    self.kl_updater.step(float(kl_div_loss))
                else:
                    kl_div_loss = torch.tensor(0.0, device=self.device)

                # Optimize.
                self.optimizer.zero_grad()
                total_loss.backward()
                nn.utils.clip_grad_norm_(
                    self.policy.model.parameters(), self.max_grad_norm
                )
                self.optimizer.step()

                # Update step data.
                step_data.append(
                    {
                        "coefficients/entropy": float(self.entropy_scheduler.coeff),
                        "coefficients/kl_div": float(self.kl_updater.coeff),
                        "coefficients/vf": float(self.vf_coeff),
                        "losses/entropy": float(entropy_loss),
                        "losses/kl_div": float(kl_div_loss),
                        "losses/policy": float(policy_loss),
                        "losses/vf": float(vf_loss),
                        "losses/total": float(total_loss),
                        "rewards/min": float(0.0),
                        "rewards/max": float(0.0),
                        "rewards/mean": float(0.0),
                        "rewards/std": float(0.0),
                    }
                )

        # Update schedulers.
        self.lr_scheduler.step(B * T)
        self.entropy_scheduler.step(B * T)

        # Reset the buffer and buffered flag.
        self.buffer = self.init_buffer(
            B,
            T + 1,
            self.env.observation_spec,
            self.policy.feature_spec,
            self.env.action_spec,
        )
        self.buffer[:, -1, ...] = final_obs
        self.buffered = False

        return pd.DataFrame(step_data).mean(axis=0).to_dict()  # type: ignore

    def to(self, device: DEVICE, /) -> "Algorithm":
        """Move the algorithm and its attributes to `device`."""
        self.buffer.to(device)
        self.env.to(device)
        self.policy.to(device)
        self.device = device
        return self
