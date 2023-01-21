import math

import torch
import torch.nn as nn


class PositionalEmbedding(nn.Module):
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
        return self.dropout(x + self.pe[0, : x.size(1)])
