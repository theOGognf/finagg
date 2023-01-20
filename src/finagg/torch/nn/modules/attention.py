"""Attention module definitions."""

import torch
import torch.nn as nn

from ..functional import masked_log_softmax


class SequenceSelection(nn.Module):
    """3D attention applied to sequence encoders and decoders for selecting the
    next element from the encoder's sequence to be appended to the decoder's
    sequence.

    Args:
        dim: Feature dimension of the encoders/decoders.

    """

    #: Weights applied to the encoder's output.
    We: nn.Linear

    #: Weights applied to the decoder's output.
    Wd: nn.Linear

    #: Weights applied to the blended encoder-decoder selection matrix.
    Wde: nn.Linear

    def __init__(self, dim: int, /) -> None:
        super().__init__()
        self.We = nn.Linear(dim, dim, bias=False)
        self.Wd = nn.Linear(dim, dim, bias=False)
        self.Wde = nn.Linear(dim, 1, bias=False)

    def forward(
        self,
        decoder_out: torch.Tensor,
        encoder_out: torch.Tensor,
        /,
        *,
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
            an encoded sequence element for each decoder sequence element.
            The last item in the D dimension, [:, -1, :], typically indicates
            the likelihoods of selecting each encoder sequence element for the
            next decoder sequence element (which is usually the desired output).

        """
        encoder_proj = (
            self.We(encoder_out).unsqueeze(1).expand(-1, decoder_out.size(1), -1, -1)
        )
        decoder_proj = self.Wd(decoder_out).unsqueeze(2)
        weights = self.Wde(torch.tanh(decoder_proj + encoder_proj)).squeeze(-1)
        return masked_log_softmax(weights, mask=mask, dim=-1)
