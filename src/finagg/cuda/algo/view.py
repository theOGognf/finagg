"""Definitions regarding applying views to batches of tensors or tensor dicts."""

import torch
from tensordict import TensorDict


def unfold(x: torch.Tensor, size: int, /, *, step: int = 1) -> torch.Tensor:
    dims = [i for i in range(x.dim())]
    dims.insert(2, -1)
    return x.unfold(1, size, step).permute(*dims)


class ViewRequirement:
    #: Key to apply the view requirement to for a given batch. The key
    #: can be any key that is compatible with a `TensorDict` key.
    #: E.g., a key can be a tuple of strings such that the item in the
    #: batch is accessed like `batch[("obs", "prices")]`.
    key: str | tuple[str, ...]

    #: Number of additional previous samples in the time or sequence dimension
    #: to include in the view requirement's output. E.g., if shift is -1,
    #: then the last two samples in the time or sequence dimension will be
    #: included for each batch element. A shift of `None` means no shift is
    #: applied and all previous samples are returned.
    shift: None | int

    def __init__(
        self,
        key: str | tuple[str, ...],
        /,
        *,
        shift: None | int = 0,
    ) -> None:
        self.key = key
        self.shift = shift
        if shift is not None and shift > 0:
            raise ValueError(
                f"{self.__class__.__name__} `shift` must be 'None' or <= 0"
            )

    def process_all(self, batch: TensorDict) -> torch.Tensor | TensorDict:
        ...

    def process_last(self, batch: TensorDict) -> torch.Tensor | TensorDict:
        """"""
        item = batch[self.key]
        if self.shift is None:
            return item
        if isinstance(item, TensorDict):
            out = item.apply(lambda x: x[:, (self.shift - 1) :, ...])
        else:
            out = item[:, (self.shift - 1) :, ...]
        return out
