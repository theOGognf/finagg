"""Definitions regarding applying views to batches of tensors or tensor dicts."""

from typing import Protocol

import torch
from tensordict import TensorDict


class View(Protocol):
    """A view requirement protocol for processing batch elements during policy
    sampling and training.

    Supports applying methods to a batch of size [B, T, ...] (where B is the
    batch dimension, and T is the time or sequence dimension) for all elements
    of B and T or just the last elements of T for all B.

    """

    @staticmethod
    def apply_all(
        x: torch.Tensor | TensorDict, size: int, /
    ) -> torch.Tensor | TensorDict:
        """Apply the view to all elements of B and T in a batch of size
        [B, T, ...] such that the returned batch is of shape [B_NEW, size, ...]
        where B_NEW <= B * T.

        """

    @staticmethod
    def apply_last(
        x: torch.Tensor | TensorDict, size: int, /
    ) -> torch.Tensor | TensorDict:
        """Apply the view to the last elements of T for all B in a batch
        of size [B, T, ...] such that the returned batch is of shape
        [B, size, ...].

        """

    @staticmethod
    def burn_size(size: int, /) -> int:
        """Return the amount of samples along the time or sequence dimension
        that's dropped for each batch element.

        This is used to determine batch size reshaping during training to make
        batch components have the same size.

        """


def rolling_window(x: torch.Tensor, size: int, /, *, step: int = 1) -> torch.Tensor:
    """Unfold the given tensor `x` along the time or sequence dimension such
    that the tensor's time or sequence dimension is mapped into two
    additional dimensions that represent a rolling window of size `size`
    and step `step` over the time or sequence dimension.

    See https://pytorch.org/docs/stable/generated/torch.Tensor.unfold.html
    for details on PyTorch's vanilla unfolding that does most of the work.

    Args:
        x: Tensor of size [B, T, ...] where B is the batch dimension, and
            T is the time or sequence dimension. B is typically the number
            of parallel environments, and T is typically the number of time
            steps or observations sampled from each environment.
        size: Size of the rolling window to create over `x`'s T dimension.
            The new sequence dimension is placed in the 2nd dimension.
        step: Number of steps to take when iterating over `x`'s T dimension
            to create a new sequence of size `size`.

    Returns:
        A new tensor of shape [B, (T - size) / step + 1, size, ...].

    """
    dims = [i for i in range(x.dim())]
    dims.insert(2, -1)
    return x.unfold(1, size, step).permute(*dims)


class RollingWindow(View):
    """A view that creates a rolling window of an item's time or sequence
    dimension without masking (at the expense of losing some samples at
    the beginning of each sequence).

    """

    @staticmethod
    def apply_all(
        x: torch.Tensor | TensorDict, size: int, /
    ) -> torch.Tensor | TensorDict:
        """Unfold the given tensor or tensor dict along the time or sequence
        dimension such that the the time or sequence dimension becomes a
        rolling window of size `size`. The new time or sequence dimension is
        also expanded into the batch dimension such that each new sequence
        becomes an additional batch element.

        Args:
            x: Tensor or tensor dict of size [B, T, ...] where B is the
                batch dimension, and T is the time or sequence dimension.
                B is typically the number of parallel environments, and T
                is typically the number of time steps or observations sampled
                from each environment.
            size: Size of the rolling window to create over `x`'s T dimension.
                The new sequence dimension is placed in the 2nd dimension.

        Returns:
            A new tensor or tensor dict of shape
            [B * (T - size + 1), size, ...].

        """
        if isinstance(x, torch.Tensor):
            E = x.shape[2:]
            return rolling_window(x, size, step=1).reshape(-1, size, *E)
        else:
            B_OLD, T_OLD = x.shape[:2]
            T_NEW = T_OLD - size + 1
            return x.apply(
                lambda x: rolling_window(x, size, step=1), batch_size=[B_OLD, T_NEW]
            ).reshape(-1)

    @staticmethod
    def apply_last(
        x: torch.Tensor | TensorDict, size: int, /
    ) -> torch.Tensor | TensorDict:
        """Grab the last `size` elements of `x` along the time or sequence
        dimension.

        Args:
            x: Tensor or tensor dict of size [B, T, ...] where B is the
                batch dimension, and T is the time or sequence dimension.
                B is typically the number of parallel environments, and T
                is typically the number of time steps or observations sampled
                from each environment.
            size: Number of "last" samples to grab along the time or sequence
                dimension T.

        Returns:
            A new tensor or tensor dict of shape [B, size, ...].

        """
        if isinstance(x, torch.Tensor):
            return x[:, -size:, ...]
        else:
            B = x.size(0)
            return x.apply(lambda x: x[:, -size:, ...], batch_size=[B, size])

    @staticmethod
    def burn_size(size: int, /) -> int:
        """This view doesn't perform any padding or masking and instead
        burns-off a small amount of samples at the beginning of each
        sequence in order to create sequences of the same length.

        """
        return size - 1


class MaskedRollingWindow(View):
    @staticmethod
    def apply_all(x: torch.Tensor | TensorDict, size: int, /) -> TensorDict:
        ...

    @staticmethod
    def apply_last(x: torch.Tensor | TensorDict, size: int, /) -> TensorDict:
        ...

    @staticmethod
    def burn_size(size: int, /) -> int:
        """This view pads the beginning of each sequence and provides masking
        to avoid burning-off samples.

        """
        return size - size


class ViewRequirement:
    """Batch preprocessing for creating overlapping time series or sequential
    environment observations that's applied prior to feeding samples into a
    policy's model.

    This component is purely for convenience. Its functionality can optionally
    be replicated within an environment's observation function. However, because
    this functionaltiy is fairly common, it's recommended to use this
    component where simple time or sequence shifting is required for
    sequence-based observations.

    Args:
        key: Key to apply the view requirement to for a given batch.
        shift: Number of additional previous samples in the time or sequence
            dimension to include in the view requirement's output.
        method: Method for applying a nonzero shift view requirement.
            Options include:
                - "rolling_window": Create a rolling window over a tensor's
                    time or sequence dimension at the cost of dropping
                    samples early into the sequence in order to force all
                    sequences to be the same size.
                - "masked_rolling_window": The same as "rolling_window" but
                    pad the beginning of each sequence to avoid dropping
                    samples and provide a mask indicating which element is
                    padding.

    """

    #: Key to apply the view requirement to for a given batch. The key
    #: can be any key that is compatible with a `TensorDict` key.
    #: E.g., a key can be a tuple of strings such that the item in the
    #: batch is accessed like `batch[("obs", "prices")]`.
    key: str | tuple[str, ...]

    #: Method for applying a nonzero shift view requirement. Each method
    #: has its own advantage. Options include:
    #:  - `RollingWindow`: Create a rolling window over a tensor's
    #:      time or sequence dimension. This method is memory-efficient
    #:      and fast, but it drops samples in order for each new batch
    #:      element to have the same sequence size. Only use this method
    #:      if the view requirement's shift is much smaller than an
    #:      environment's horizon.
    #:  - `MaskedRollingWindow`: The same as `RollingWindow`, but it pads
    #:      the beginning of each sequence so no samples are dropped. This
    #:      method also provides a padding mask for each tensor or tensor
    #:      dict to indicate which sequence element is padding.
    method: type[View]

    #: Number of additional previous samples in the time or sequence dimension
    #: to include in the view requirement's output. E.g., if shift is 1,
    #: then the last two samples in the time or sequence dimension will be
    #: included for each batch element.
    shift: int

    def __init__(
        self,
        key: str | tuple[str, ...],
        /,
        *,
        shift: int = 0,
        method: str = "rolling_window",
    ) -> None:
        self.key = key
        self.shift = shift
        if shift < 0:
            raise ValueError(f"{self.__class__.__name__} `shift` must be non-negative.")
        match method:
            case "rolling_window":
                self.method = RollingWindow
            case "masked_rolling_window":
                self.method = MaskedRollingWindow

    def apply_all(self, batch: TensorDict) -> torch.Tensor | TensorDict:
        """Apply the view to all of the time or sequence elements.

        This method expands the elements of `batch`'s first two dimensions
        together to allow parallel batching of `batch`'s elements in the batch
        and time or sequence dimension together. This method is typically
        used within a training loop and isn't typically used for sampling
        a policy's actions or environment interaction.

        Args:
            batch: Tensor dict of size [B, T, ...] where B is the batch
                dimension, and T is the time or sequence dimension. B is
                typically the number of parallel environments, and T is
                typically the number of time steps or observations sampled
                from each environment.

        Returns:
            A tensor or tensor dict of size [B_NEW, `self.shift`, ...]
            where B_NEW <= B * T, depending on the view requirement method
            applied. In the case where `self.shift` is `0`, the return tensor
            or tensor dict has size [B * T, ...].

        """
        item = batch[self.key]

        with torch.no_grad():
            if not self.shift:
                return item.reshape(-1)

            return self.method.apply_all(item, self.shift + 1)

    def apply_last(self, batch: TensorDict) -> torch.Tensor | TensorDict:
        """Apply the view to just the last time or sequence elements.

        This method is typically used for sampling a model's features
        and eventual sampling of a policy's actions for parallel environments.

        Args:
            batch: Tensor dict of size [B, T, ...] where B is the batch
                dimension, and T is the time or sequence dimension. B is
                typically the number of parallel environments, and T is
                typically the number of time steps or observations sampled
                from each environment.

        Returns:
            A tensor or tensor dict of size [B, `self.shift + 1`, ...]. In the
            case where `self.shift` is `0`, the returned tensor or tensor dict
            has size [B, ...].

        """
        item = batch[self.key]

        with torch.no_grad():
            if not self.shift:
                return item[:, -1, ...]

            return self.method.apply_last(item, self.shift + 1)

    def burn_size(self) -> int:
        """Return the burn size of the underlying view requirement method."""
        return self.method.burn_size(self.shift + 1)
