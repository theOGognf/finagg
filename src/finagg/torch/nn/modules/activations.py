"""Activation function registry for convenience."""

import torch
import torch.nn as nn
import torch.nn.functional as F


class SquaredReLU(nn.Module):
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.pow(F.relu(x), 2)


ACTIVATIONS: dict[str, type[nn.Module]] = {
    "elu": nn.ELU,
    "gelu": nn.GELU,
    "hard_shrink": nn.Hardshrink,
    "hard_sigmoid": nn.Hardsigmoid,
    "hard_swish": nn.Hardswish,
    "hard_tanh": nn.Hardtanh,
    "identity": nn.Identity,
    "leaky_relu": nn.LeakyReLU,
    "log_sigmoid": nn.LogSigmoid,
    "log_softmax": nn.LogSoftmax,
    "relu": nn.ReLU,
    "relu6": nn.ReLU6,
    "sigmoid": nn.Sigmoid,
    "squared_relu": SquaredReLU,
    "softmax": nn.Softmax,
    "swish": nn.SiLU,
    "tanh": nn.Tanh,
}


def get_activation(name: str, /) -> nn.Module:
    """Return an activation instance by its `name`."""
    return ACTIVATIONS[name]()
