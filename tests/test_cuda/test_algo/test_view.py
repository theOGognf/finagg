import pytest
import torch
from tensordict import TensorDict

from finagg.cuda.algo.batch import Batch
from finagg.cuda.algo.view import pad_last_sequence

SIZE = 2

B = 4
T = 1
TOTAL = B * T
PAD_LAST_SEQUENCE_CASE_0 = (
    torch.arange(B * T).reshape(B, T),
    TensorDict(
        {
            Batch.INPUTS.value: torch.tensor([[0, 0], [0, 1], [0, 2], [0, 3]]),
            Batch.PADDING_MASK.value: torch.tensor([[1, 0], [1, 0], [1, 0], [1, 0]]),
        },
        batch_size=[B, SIZE],
    ),
)

B = 2
T = 2
TOTAL = B * T * 2
PAD_LAST_SEQUENCE_CASE_1 = (
    torch.arange(TOTAL).reshape(B, T, 2),
    TensorDict(
        {
            Batch.INPUTS.value: torch.arange(TOTAL).reshape(B, T, 2),
            Batch.PADDING_MASK.value: torch.zeros(B, SIZE),
        },
        batch_size=[B, SIZE],
    ),
)

B = 2
T = 4
TOTAL = B * T
PAD_LAST_SEQUENCE_CASE_2 = (
    torch.arange(TOTAL).reshape(B, T, 1, 1, 1),
    TensorDict(
        {
            Batch.INPUTS.value: torch.arange(TOTAL).reshape(B, T, 1, 1, 1)[
                :, -SIZE:, ...
            ],
            Batch.PADDING_MASK.value: torch.zeros(B, SIZE),
        },
        batch_size=[B, SIZE],
    ),
)


@pytest.mark.parametrize(
    "inputs,expected",
    [
        PAD_LAST_SEQUENCE_CASE_0,
        PAD_LAST_SEQUENCE_CASE_1,
        PAD_LAST_SEQUENCE_CASE_2,
    ],
)
def test_pad_last_sequence(
    inputs: torch.Tensor | TensorDict, expected: torch.Tensor | TensorDict
) -> None:
    assert (pad_last_sequence(inputs, SIZE) == expected).all()


# PAD_WHOLE_SEQUENCE_CASE_0 = (
#     torch.arange(4).reshape(4, 1),
#     TensorDict(
#         {
#             Batch.INPUTS.value: torch.tensor([[0, 0], [0, 1], [0, 2], [0, 3]]),
#             Batch.PADDING_MASK.value: torch.tensor([[1, 0], [1, 0], [1, 0], [1, 0]]),
#         },
#         batch_size=[4, SIZE],
#     ),
# )

# PAD_WHOLE_SEQUENCE_CASE_1 = (
#     torch.arange(8).reshape(2, 2, 2),
#     TensorDict(
#         {
#             Batch.INPUTS.value: torch.cat([torch.zeros(2, 2, 2), torch.arange(8).reshape(2, 2, 2)], dim=1),
#             Batch.PADDING_MASK.value: torch.zeros(2, 3),
#         },
#         batch_size=[2, SIZE + 1],
#     ),
# )

# PAD_WHOLE_SEQUENCE_CASE_2 = (
#     torch.arange(8).reshape(2, 4, 1, 1, 1),
#     TensorDict(
#         {
#             Batch.INPUTS.value: torch.cat([torch.zeros(2, 4, 1, 1, 1), torch.arange(8).reshape(2, 4, 1, 1, 1)], dim=1),
#             Batch.PADDING_MASK.value: torch.zeros(2, 3),
#         },
#         batch_size=[2, SIZE + 1],
#     ),
# )


# def test_pad_whole_sequence() -> None:
#     ...


# def test_padded_rolling_window_apply_all() -> None:
#     ...


# def test_padded_rolling_window_apply_last() -> None:
#     ...


# def test_rolling_window() -> None:
#     ...


# def test_rolling_window_apply_all() -> None:
#     ...


# def test_rolling_window_apply_last() -> None:
#     ...


# def test_view_requirement_apply_all() -> None:
#     ...


# def test_view_requirement_apply_last() -> None:
#     ...
