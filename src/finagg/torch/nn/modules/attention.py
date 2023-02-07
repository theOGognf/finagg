"""Attention module definitions."""

import copy

import torch
import torch.nn as nn

from ..functional import masked_log_softmax
from .activations import get_activation
from .module import Module
from .skip import SequentialSkipConnection


class PointerNetwork(
    Module[[torch.Tensor, torch.Tensor, None | torch.Tensor], torch.Tensor]
):
    """3D attention applied to sequence encoders and decoders for selecting the
    next element from the encoder's sequence to be appended to the decoder's
    sequence.

    An implementation of https://arxiv.org/pdf/1506.03134.pdf adapted from
    https://towardsdatascience.com/pointer-networks-with-transformers-1a01d83f7543
    which also adapted from
    https://github.com/ast0414/pointer-networks-pytorch/blob/master/model.py.

    Args:
        embed_dim: Feature dimension of the encoders/decoders.

    """

    #: Weights applied to the encoder's output.
    W1: nn.Linear

    #: Weights applied to the decoder's output.
    W2: nn.Linear

    #: Weights applied to the blended encoder-decoder selection matrix.
    VT: nn.Linear

    def __init__(self, embed_dim: int, /) -> None:
        super().__init__()
        self.W1 = nn.Linear(embed_dim, embed_dim, bias=False)
        self.W2 = nn.Linear(embed_dim, embed_dim, bias=False)
        self.VT = nn.Linear(embed_dim, 1, bias=False)

    def forward(
        self,
        decoder_out: torch.Tensor,
        encoder_out: torch.Tensor,
        mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        """Select valid values from `encoder_out` as indicated by `mask` using
        features from `decoder_out`.

        Args:
            decoder_out: Sequence decoder output with shape [B, D, C].
            encoder_out: Sequence encoder output with shape [B, E, C].
            mask: Mask with shape [B, D, E] indicating the sequence element of
                `encoder_out` that can be selected.

        Returns:
            Logits with shape [B, D, E] indicating the likelihood of selecting
            an encoded sequence element in E for each decoder sequence element in D.
            The last item in the D dimension, [:, -1, :], typically indicates
            the likelihoods of selecting each encoder sequence element for the
            next decoder sequence element (which is usually the desired output).

        """
        # (B, D, E, C) <- (B, E, C)
        encoder_proj = (
            self.W1(encoder_out).unsqueeze(1).expand(-1, decoder_out.size(1), -1, -1)
        )
        # (B, D, 1, C) <- (B, D, C)
        decoder_proj = self.W2(decoder_out).unsqueeze(2)
        # (B, D, E) <- (B, D, 1, C) + (B, D, E, C)
        weights = self.VT(torch.tanh(decoder_proj + encoder_proj)).squeeze(-1)
        return masked_log_softmax(weights, mask=mask, dim=-1)


class CrossAttention(
    Module[
        [torch.Tensor, torch.Tensor, None | torch.Tensor, None | torch.Tensor],
        torch.Tensor,
    ]
):
    """Perform multihead attention keys to a query, mapping the keys of
    sequence length K to the query of sequence length Q.

    Args:
        embed_dim: Key and query feature dimension.
        num_heads: Number of attention heads.
        hidden_dim: Number of hidden features in the hidden layers
            of the feedforward network that's after performing attention.
        activation_fn: Activation function ID.
        attention_dropout: Sequence dropout in the attention heads.
        hidden_dropout: Feedforward dropout after performing attention.
        skip_kind: Kind of residual or skip connection to make between
            the output of the multihead attention and the feedforward
            module.
        fan_in: Whether to apply downsampling within the skip connection
            when using a `skip_kind` that increases hidden feature
            dimensions.

    """

    #: Underlying multihead attention mechanism.
    attention: nn.MultiheadAttention

    #: Norm for the keys.
    kv_norm: nn.LayerNorm

    #: Norm for the queries.
    q_norm: nn.LayerNorm

    #: Skip connection for applying special residual connections.
    skip_connection: SequentialSkipConnection

    def __init__(
        self,
        embed_dim: int,
        /,
        num_heads: int = 2,
        hidden_dim: int = 128,
        activation_fn: str = "relu",
        attention_dropout: float = 0.0,
        hidden_dropout: float = 0.0,
        skip_kind: None | str = "cat",
        fan_in: bool = True,
    ) -> None:
        super().__init__()
        self.q_norm = nn.LayerNorm(embed_dim)
        self.kv_norm = nn.LayerNorm(embed_dim)
        self.attention = nn.MultiheadAttention(
            embed_dim, num_heads, dropout=attention_dropout, batch_first=True
        )
        self.skip_connection = SequentialSkipConnection(
            embed_dim, kind=skip_kind, fan_in=fan_in
        )
        mlp = torch.nn.Sequential(
            nn.LayerNorm(self.skip_connection.out_features),
            nn.Linear(self.skip_connection.out_features, hidden_dim),
            get_activation(activation_fn),
            nn.Dropout(p=hidden_dropout),
            nn.Linear(hidden_dim, self.skip_connection.out_features),
        )
        self.skip_connection.append(mlp)

    def _attention_block(
        self,
        q: torch.Tensor,
        kv: torch.Tensor,
        /,
        *,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        """Cross-attention helper."""
        attention, _ = self.attention(
            q,
            kv,
            kv,
            key_padding_mask=key_padding_mask,
            attn_mask=attention_mask,
            need_weights=False,
        )
        return attention  # type: ignore[no-any-return]

    def forward(
        self,
        q: torch.Tensor,
        kv: torch.Tensor,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        """Apply multihead attention keys to a query, mapping the keys of
        sequence length K to the query of sequence length Q.

        Args:
            q: Query with shape [B, Q, E].
            kv: Keys with shape [B, K, E].
            key_padding_mask: Mask with shape [B, K] indicating sequence
                elements of `kv` that are PADDED or INVALID values.
            attention_mask: Mask with shape [Q, K] that indicates whether
                elements in Q can attend to elements in K.

        Returns:
            Values with shape [B, Q, E].

        """
        qkv = self._attention_block(
            self.q_norm(q),
            self.kv_norm(kv),
            key_padding_mask=key_padding_mask,
            attention_mask=attention_mask,
        )
        return self.skip_connection(q, qkv)


class SelfAttention(
    Module[
        [torch.Tensor, None | torch.Tensor, None | torch.Tensor],
        torch.Tensor,
    ]
):
    """Perform multihead attention keys to a a sequence, using it for the
    queries, keys, and values.

    Args:
        embed_dim: Key and query feature dimension.
        num_heads: Number of attention heads.
        hidden_dim: Number of hidden features in the hidden layers
            of the feedforward network that's after performing attention.
        activation_fn: Activation function ID.
        attention_dropout: Sequence dropout in the attention heads.
        hidden_dropout: Feedforward dropout after performing attention.
        skip_kind: Kind of residual or skip connection to make between
            the output of the multihead attention and the feedforward
            module.
        fan_in: Whether to apply downsampling within the skip connection
            when using a `skip_kind` that increases hidden feature
            dimensions.

    """

    #: Underlying multihead attention mechanism.
    attention: nn.MultiheadAttention

    #: Skip connection for applying special residual connections.
    skip_connection: SequentialSkipConnection

    #: Norm for the queries/keys/values.
    x_norm: nn.LayerNorm

    def __init__(
        self,
        embed_dim: int,
        /,
        num_heads: int = 2,
        hidden_dim: int = 128,
        activation_fn: str = "relu",
        attention_dropout: float = 0.0,
        hidden_dropout: float = 0.0,
        skip_kind: None | str = "cat",
        fan_in: bool = True,
    ) -> None:
        super().__init__()
        self.x_norm = nn.LayerNorm(embed_dim)
        self.attention = nn.MultiheadAttention(
            embed_dim, num_heads, dropout=attention_dropout, batch_first=True
        )
        self.skip_connection = SequentialSkipConnection(
            embed_dim, kind=skip_kind, fan_in=fan_in
        )
        mlp = torch.nn.Sequential(
            nn.LayerNorm(self.skip_connection.out_features),
            nn.Linear(self.skip_connection.out_features, hidden_dim),
            get_activation(activation_fn),
            nn.Dropout(p=hidden_dropout),
            nn.Linear(hidden_dim, self.skip_connection.out_features),
        )
        self.skip_connection.append(mlp)

    def _attention_block(
        self,
        x: torch.Tensor,
        /,
        *,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        """Self-attention helper."""
        attention, _ = self.attention(
            x,
            x,
            x,
            key_padding_mask=key_padding_mask,
            attn_mask=attention_mask,
            need_weights=False,
        )
        return attention  # type: ignore[no-any-return]

    def forward(
        self,
        x: torch.Tensor,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        """Apply self-attention to `x`, attending sequence elements to
        themselves.

        Args:
            x: Query with shape [B, X, E].
            key_padding_mask: Mask with shape [B, X] indicating sequence
                elements of `kv` that are PADDED or INVALID values.
            attention_mask: Mask with shape [X, X] that indicates whether
                elements in X can attend to other elements in X.

        Returns:
            Values with shape [B, X, E].

        """
        qkv = self._attention_block(
            self.x_norm(x),
            key_padding_mask=key_padding_mask,
            attention_mask=attention_mask,
        )
        return self.skip_connection(x, qkv)


class SelfAttentionStack(
    Module[[torch.Tensor, None | torch.Tensor, None | torch.Tensor], torch.Tensor]
):
    """Stacks of self-attention to iteratively attend over a sequence.

    Args:
        module: Self-attention module to repeat.
        num_layers: Number of layers of `module` to repeat.

    """

    def __init__(self, module: SelfAttention, num_layers: int, /) -> None:
        super().__init__()
        self.layers = nn.ModuleList([copy.deepcopy(module) for _ in range(num_layers)])

    def forward(
        self,
        x: torch.Tensor,
        key_padding_mask: None | torch.Tensor = None,
        attention_mask: None | torch.Tensor = None,
    ) -> torch.Tensor:
        """Iteratively apply self-attention to `x`, attending sequence
        elements to themselves.

        Args:
            x: Query with shape [B, X, E].
            key_padding_mask: Mask with shape [B, X] indicating sequence
                elements of `kv` that are PADDED or INVALID values.
            attention_mask: Mask with shape [X, X] that indicates whether
                elements in X can attend to other elements in X.

        Returns:
            Values with shape [B, X, E].

        """
        out = x
        for layer in self.layers:
            out = layer(
                out, key_padding_mask=key_padding_mask, attention_mask=attention_mask
            )
        return out
