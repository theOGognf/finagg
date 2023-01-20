"""Skip connection module definitions."""

import torch
import torch.nn as nn

from ..functional import skip_connection


class SequentialSkipConnection(nn.Module):
    """Sequential skip connection.

    Apply a skip connection to an input and the output of a layer that
    uses that input.

    Args:
        dim: Original input feature size.
        kind: Type of skip connection to apply.
            Options include:
                - "residual" for a standard residual connection (summing outputs)
                - "cat" for concatenating outputs
                - `None` for no skip connection
        fan_in: Whether to apply a linear layer after each skip connection
            automatically such that the output of the forward pass will
            always have dimension `dim`.

    """

    #: Number of input features for each module.
    _in_features: list[int]

    #: Modules associated with the sequential forward passes.
    _layers: nn.ModuleList

    #: Whether to fan-in the outputs of each skip connection automatically.
    #: The output features of the forward pass will always be `dim` in this
    #: case.
    fan_in: bool

    #: Kind of skip connection. "residual" for a standard residual connection
    #: (summing outputs), "cat" for concatenating outputs, and `None` for no
    #: skip connection (reduces to a regular, sequential module).
    kind: str

    def __init__(
        self, dim: int, kind: None | str = "cat", fan_in: bool = False
    ) -> None:
        super().__init__()
        self._in_features = [dim]
        self._layers = torch.nn.ModuleList([])
        self.kind = kind
        self.fan_in = fan_in

    def append(self, module: nn.Module, /) -> int:
        """Append `module` to the skip connection.

        If `fan_in` is `True`, then a fan-in layer is also appended
        after `module` to reduce the number of output features back
        to `dim`.

        Args:
            module: Module to append and apply a skip connection to.

        Returns:
            Number of output features from the sequential skip connection.

        """
        self._in_features.append(self.skip_features)
        self._layers.append(module)
        if self.fan_in:
            linear = nn.Linear(self._in_features[-1], self._in_features[0])
            self._in_features.append(linear.out_features)
            self._layers.append(linear)
        return self.out_features

    def forward(self, x1: torch.Tensor, x2: torch.Tensor, /) -> torch.Tensor:
        """Perform a sequential skip connection, first applying a skip
        connection to `x1` and `x2`, and then sequentially applying skip
        connections to the output and the output of the next layer.

        Args:
            x1: Skip connection seed with shape [B, T, ...].
            x2: Skip connection seed with same shape as `x1`.

        Returns:
            A tensor with shape depending on `fan_in` and `kind`.

        """
        y = skip_connection(x1, x2, self.kind)
        for i, layer in enumerate(self._layers):
            if self.fan_in:
                if not (i % 2):
                    y = skip_connection(y, layer(y), self.kind)
                else:
                    y = layer(y)
            else:
                y = skip_connection(y, layer(y), self.kind)
        return y

    @property
    def in_features(self) -> int:
        """Return the first number of input features."""
        return self._in_features[0]

    @property
    def out_features(self) -> int:
        """Return the number of output features according to the number of input
        features, the kind of skip connection, and whether there's a fan-in
        layer.

        """
        if self.fan_in:
            return self._in_features[0]

        return self.skip_features

    @property
    def skip_features(self) -> int:
        """Return the number of output features according to the number of input
        features and the kind of skip connection.

        """
        match self.kind:
            case "residual":
                return self._in_features[-1]
            case "cat":
                return 2 * self._in_features[-1]
            case None:
                return self._in_features[-1]
