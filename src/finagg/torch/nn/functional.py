"""Functional PyTorch definitions."""

import torch
import torch.nn.functional as F

FINFO = torch.finfo()


def binary_mask_to_float_mask(mask: torch.Tensor, /) -> torch.Tensor:
    """Convert `0` and `1` elements in a binary mask to `-inf` and `0`,
    respectively.

    Args:
        mask: Binary mask tensor:

    Returns:
        Float mask tensor where `0` indicates an UNPADDED or VALID value.

    """
    return (
        mask.float()
        .masked_fill(mask == 0, float("-inf"))
        .masked_fill(mask == 1, float(0.0))
    )


def float_mask_to_binary_mask(mask: torch.Tensor, /) -> torch.Tensor:
    """Convert `0` and `-inf` elements into a boolean mask of `True` and
    `False`, respectively.

    Args:
        mask: Float mask tensor.

    Returns:
        Boolean mask tensor where `True` indicates an UNPADDED or VALID value.

    """
    return (
        mask.float()
        .masked_fill(mask == float("-inf"), False)
        .masked_fill(mask == 0, True)
        .bool()
    )


def mask_from_lengths(x: torch.Tensor, lengths: torch.Tensor, /) -> torch.Tensor:
    """Return sequence mask that indicates UNPADDED or VALID values
    according to tensor lengths.

    Args:
        x: Tensor with shape [B, T, ...].
        lengths: Lengths of the T sequence for each B element in `x`.

    Returns:
        Sequence mask of shape [B, T].

    """
    B, T = x.shape[:2]
    lengths = lengths.long().view(-1, 1).expand(B, T)
    range_tensor = torch.arange(T, device=lengths.device, dtype=lengths.dtype).expand(
        B, T
    )
    return range_tensor < lengths


def masked_avg(
    x: torch.Tensor,
    /,
    *,
    mask: None | torch.Tensor = None,
    dim: int = 1,
    keepdim: bool = False,
) -> torch.Tensor:
    """Apply a masked average to `x` along `dim`.

    Useful for pooling potentially padded features.

    Args:
        x: Tensor with shape [B, T, ...] to apply pooling to.
        mask: Mask with shape [B, T] indicating UNPADDED or VALID values.
        dim: Dimension to pool along.
        keepdim: Whether to keep the pooled dimension.

    Returns:
        Masked max of `x` along `dim` and the indices of those maximums.

    """
    if mask is not None:
        while mask.dim() < x.dim():
            mask = mask.unsqueeze(-1)
        masksum = mask.sum(dim=dim, keepdim=True)
        x = mask * x
        avg = x.sum(dim=dim, keepdim=True) / masksum
    else:
        avg = x.mean(dim=dim, keepdim=True)
    if not keepdim:
        avg = avg.squeeze(dim)
    return avg


def masked_categorical_sample(
    x: torch.Tensor, /, *, mask: None | torch.Tensor = None, dim: int = 1
) -> tuple[torch.Tensor, torch.Tensor]:
    """Masked categorical sampling of `x`.

    Typically used for sampling from outputs of `masked_log_softmax`.

    Args:
        x: Logits with shape [B, T, ...] to sample from.
        mask: Mask with shape [B, T] indicating UNPADDED or VALID values.
        dim: Dimension to gather sampled values along.

    Returns:
        Sampled logits and the indices of those sampled logits.

    """
    if mask is not None:
        while mask.dim() < x.dim():
            mask = mask.unsqueeze(-1)
        x = x + torch.clamp(torch.log(mask), FINFO.min, FINFO.max)
    dist = torch.distributions.Categorical(logits=x)  # type: ignore
    samples = dist.sample().unsqueeze(-1)  # type: ignore
    return x.gather(dim, samples), samples


def masked_log_softmax(
    x: torch.Tensor, /, *, mask: None | torch.Tensor = None, dim: int = -1
) -> torch.Tensor:
    """Apply a masked log softmax to `x` along `dim`.

    Typically used for getting logits from a model that predicts a sequence.
    The output of this function is typically passed to `masked_categorical_sample`.

    Args:
        x: Tensor with shape [B, T, ...].
        mask: Mask with shape [B, T] indicating UNPADDED or VALID values.
        dim: Dimension to apply log softmax along.

    Returns:
        Logits.

    """
    if mask is not None:
        while mask.dim() < x.dim():
            mask = mask.unsqueeze(-1)
        x = x + torch.clamp(torch.log(mask), FINFO.min, FINFO.max)
    return F.log_softmax(x, dim=dim)


def masked_max(
    x: torch.Tensor,
    /,
    *,
    mask: None | torch.Tensor = None,
    dim: int = 1,
    keepdim: bool = False,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Apply a masked max to `x` along `dim`.

    Useful for pooling potentially padded features.

    Args:
        x: Tensor with shape [B, T, ...] to apply pooling to.
        mask: Mask with shape [B, T] indicating UNPADDED or VALID values.
        dim: Dimension to pool along.
        keepdim: Whether to keep the pooled dimension.

    Returns:
        Masked max of `x` along `dim` and the indices of those maximums.

    """
    if mask is not None:
        while mask.dim() < x.dim():
            mask = mask.unsqueeze(-1)
        x = x.masked_fill(~mask.bool(), FINFO.min)
    return x.max(dim=dim, keepdim=keepdim)  # type: ignore


def skip_connection(
    x: torch.Tensor,
    y: torch.Tensor,
    /,
    *,
    kind: None | str = "cat",
    dim: int = -1,
) -> torch.Tensor:
    """Perform a skip connection for `x` and `y`.

    Args:
        x: Skip connection seed with shape [B, T, ...].
        y: Skip connection seed with same shape as `x`.
        kind: Type of skip connection to use.
            Options include:
                - "residual" for a standard residual connection (summing outputs)
                - "cat" for concatenating outputs
                - `None` for no skip connection
        dim: Dimension to apply concatentation along. Only valid when
            `kind` is "cat"

    Returns:
        A tensor with shape depending on `kind`.

    """
    match kind:
        case "residual":
            return x + y
        case "cat":
            return torch.cat([x, y], dim=dim)
        case None:
            return y
    raise NotImplementedError(f"No skip connection type for {kind}")
