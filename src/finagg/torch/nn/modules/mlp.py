from typing import Sequence

import torch
import torch.nn as nn

from .activations import get_activation
from .module import Module

nn.LayerNorm


class MLP(nn.Sequential, Module[[torch.Tensor], torch.Tensor]):
    """Simple implementation of a multi-layer perceptron.

    Args:
        input_dim: Input layer dimension.
        hiddens: Hidden layer dimensions.
        activation_fn: Hidden activation function that immediately follows
            the linear layer or the norm layer (if one exists).
        norm_layer: Optional normalization layer type that immediately
            follows the linear layer.
        bias: Whether to include a bias for each layer in the MLP.
        dropout: Optional dropout that after the activation function.
        inplace: Whether activation functions occur in-place.

    """

    def __init__(
        self,
        input_dim: int,
        hiddens: Sequence[int],
        /,
        *,
        activation_fn: str = "relu",
        norm_layer: None | type[nn.BatchNorm1d | nn.LayerNorm] = None,
        bias: bool = True,
        dropout: float = 0.0,
        inplace: bool = True,
    ) -> None:
        params = {"inplace": inplace} if inplace else {}
        layers: list[nn.Module] = []
        in_dim = input_dim
        for hidden_dim in hiddens[:-1]:
            layers.append(nn.Linear(in_dim, hidden_dim, bias=bias))
            if norm_layer is not None:
                layers.append(norm_layer(hidden_dim))
            layers.append(get_activation(activation_fn, **params))
            if dropout:
                layers.append(nn.Dropout(p=dropout, **params))
            in_dim = hidden_dim
        layers.append(nn.Linear(in_dim, hiddens[-1], bias=bias))
        super().__init__(*layers)
