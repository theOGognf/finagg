"""Perceiver definitions as described by https://arxiv.org/pdf/2107.14795.pdf."""

import torch
import torch.nn as nn

from .attention import CrossAttention, SelfAttention, SelfAttentionStack


class PerceiverLayer(nn.Module):
    def __init__(
        self,
        input_dim: int,
        embed_dim: int,
        /,
        *,
        hidden_dim: int = 128,
        num_heads: int = 2,
        num_layers: int = 2,
        activation_fn: str = "relu",
        attention_dropout: float = 0.0,
        hidden_dropout: float = 0.0,
        skip_kind: str = "cat",
        fan_in: bool = True,
    ) -> None:
        super().__init__()
        self.embedder = nn.Linear(input_dim, embed_dim)
        self.cross_attention = CrossAttention(
            embed_dim,
            num_heads=num_heads,
            hidden_dim=hidden_dim,
            activation_fn=activation_fn,
            attention_dropout=attention_dropout,
            hidden_dropout=hidden_dropout,
            skip_kind=skip_kind,
            fan_in=fan_in,
        )
        self.self_attention = SelfAttentionStack(
            SelfAttention(
                embed_dim,
                num_heads=num_heads,
                hidden_dim=hidden_dim,
                activation_fn=activation_fn,
                attention_dropout=attention_dropout,
                hidden_dropout=hidden_dropout,
                skip_kind=skip_kind,
                fan_in=fan_in,
            ),
            num_layers,
        )

    def forward(
        self,
        q: torch.Tensor,
        kv: torch.Tensor,
        /,
        *,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        kv = self.embedder(kv)
        latent = self.cross_attention(
            q, kv, key_padding_mask=key_padding_mask, attention_mask=attention_mask
        )
        return self.self_attention(latent)  # type: ignore


class PerceiverIOLayer(nn.Module):
    def __init__(
        self,
        input_dim: int,
        embed_dim: int,
        output_seq_dim: int,
        output_dim: int,
        /,
        *,
        hidden_dim: int = 128,
        num_heads: int = 2,
        num_layers: int = 2,
        activation_fn: str = "relu",
        attention_dropout: float = 0.0,
        hidden_dropout: float = 0.0,
        skip_kind: str = "cat",
        fan_in: bool = True,
    ) -> None:
        super().__init__()
        self.perceiver_layer = PerceiverLayer(
            input_dim,
            embed_dim,
            hidden_dim=hidden_dim,
            num_heads=num_heads,
            num_layers=num_layers,
            activation_fn=activation_fn,
            attention_dropout=attention_dropout,
            hidden_dropout=hidden_dropout,
            skip_kind=skip_kind,
            fan_in=fan_in,
        )
        self.output_query = nn.Parameter(torch.zeros([output_seq_dim, embed_dim]))
        with torch.no_grad():
            nn.init.xavier_uniform_(self.output_query)
        self.decoder = CrossAttention(
            embed_dim,
            num_heads=num_heads,
            hidden_dim=hidden_dim,
            activation_fn=activation_fn,
            attention_dropout=attention_dropout,
            hidden_dropout=hidden_dropout,
            skip_kind=skip_kind,
            fan_in=fan_in,
        )
        self.debedder = nn.Linear(embed_dim, output_dim)

    def forward(
        self,
        q: torch.Tensor,
        kv: torch.Tensor,
        /,
        *,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        B = q.size(0)
        output_query = self.output_query.unsqueeze(0).expand(
            B, *self.output_query.shape
        )
        latent = self.perceiver_layer(
            q, kv, key_padding_mask=key_padding_mask, attention_mask=attention_mask
        )
        output = self.decoder(output_query, latent)
        return self.debedder(output)  # type: ignore
