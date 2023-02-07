"""Embeddings for sequences."""

import math

import torch
import torch.nn as nn


class PositionalEmbedding(nn.Module):
    """Apply positional embeddings to an input sequence.

    Positional embeddings that help distinguish values at different parts
    of a sequence. Beneficial if an entire sequence is attended to.

    Args:
        embed_dim: Input feature dimension.
        max_len: Max input sequence length.
        dropout: Dropout on the output of `forward`.

    """

    #: Dropout on the output of `forward`.
    dropout: nn.Dropout

    #: Positional embedding tensor.
    pe: torch.Tensor

    def __init__(
        self, embed_dim: int, max_len: int, /, *, dropout: float = 0.0
    ) -> None:
        super().__init__()
        pos = torch.arange(max_len).unsqueeze(1)
        div = (-math.log(10_000) / embed_dim * torch.arange(0, embed_dim, 2)).exp()
        pe = torch.zeros(1, max_len, embed_dim)
        pe[0, :, 0::2] = torch.sin(pos * div)
        pe[0, :, 1::2] = torch.cos(pos * div)
        self.dropout = nn.Dropout(p=dropout)
        self.register_buffer("pe", pe)

    def forward(self, x: torch.Tensor, /) -> torch.Tensor:
        """Add positional embeddings to `x`.

        Args:
            x: Tensor with shape [B, T, E] where B is the batch dimension,
                T is the time or sequence dimension, and E is a feature
                dimension.

        Returns:
            Tensor with added positional embeddings.

        """
        return self.dropout(x + self.pe[0, : x.size(1)])  # type: ignore[no-any-return]
