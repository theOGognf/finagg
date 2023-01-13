import torch
from tensordict import TensorDict


def unfold(x: torch.Tensor, size: int, step: int) -> torch.Tensor:
    dims = [i for i in range(x.dim())]
    dims.insert(2, -1)
    return x.unfold(1, size, step).permute(*dims)


class ViewRequirement:
    ...
