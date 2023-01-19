import pytest
import torch
from tensordict import TensorDict

from finagg.cuda.algo.batch import Batch
from finagg.cuda.algo.view import (
    PaddedRollingWindow,
    RollingWindow,
    pad_last_sequence,
    pad_whole_sequence,
    rolling_window,
)

SIZE = 2

B = 4
T = 1
TOTAL = B * T
INPUTS = torch.tensor([[0, 0], [0, 1], [0, 2], [0, 3]]).float()
PADDING_MASK = torch.tensor([[1, 0], [1, 0], [1, 0], [1, 0]]).bool()
PAD_LAST_SEQUENCE_CASE_0 = (
    torch.arange(B * T).reshape(B, T).float(),
    TensorDict(
        {
            Batch.INPUTS.value: INPUTS,
            Batch.PADDING_MASK.value: PADDING_MASK,
        },
        batch_size=[B, SIZE],
    ),
)

B = 2
T = 2
TOTAL = B * T * 2
INPUTS = torch.arange(TOTAL).reshape(B, T, 2).float()
PADDING_MASK = torch.zeros(B, SIZE).bool()
PAD_LAST_SEQUENCE_CASE_1 = (
    torch.arange(TOTAL).reshape(B, T, 2).float(),
    TensorDict(
        {
            Batch.INPUTS.value: INPUTS,
            Batch.PADDING_MASK.value: PADDING_MASK,
        },
        batch_size=[B, SIZE],
    ),
)

B = 2
T = 4
TOTAL = B * T
INPUTS = torch.arange(TOTAL).reshape(B, T, 1, 1, 1)[:, -SIZE:, ...].float()
PADDING_MASK = torch.zeros(B, SIZE).bool()
PAD_LAST_SEQUENCE_CASE_2 = (
    torch.arange(TOTAL).reshape(B, T, 1, 1, 1).float(),
    TensorDict(
        {
            Batch.INPUTS.value: INPUTS,
            Batch.PADDING_MASK.value: PADDING_MASK,
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
    inputs: torch.Tensor, expected: torch.Tensor | TensorDict
) -> None:
    assert (pad_last_sequence(inputs, SIZE) == expected).all()


B = 4
T = 1
TOTAL = B * T
INPUTS = torch.tensor([[0, 0], [0, 1], [0, 2], [0, 3]]).float()
PADDING_MASK = torch.tensor([[1, 0], [1, 0], [1, 0], [1, 0]]).bool()
PAD_WHOLE_SEQUENCE_CASE_0 = (
    torch.arange(TOTAL).reshape(B, T).float(),
    TensorDict(
        {
            Batch.INPUTS.value: INPUTS,
            Batch.PADDING_MASK.value: PADDING_MASK,
        },
        batch_size=[B, T + (SIZE - 1)],
    ),
)

B = 2
T = 2
TOTAL = B * T * 2
INPUTS = torch.cat(
    [torch.zeros(B, (SIZE - 1), 2), torch.arange(TOTAL).reshape(B, T, 2)], dim=1
).float()
PADDING_MASK = torch.zeros(B, T + (SIZE - 1)).bool()
PADDING_MASK[:, : (SIZE - 1)] = True
PAD_WHOLE_SEQUENCE_CASE_1 = (
    torch.arange(TOTAL).reshape(B, T, 2).float(),
    TensorDict(
        {
            Batch.INPUTS.value: INPUTS,
            Batch.PADDING_MASK.value: PADDING_MASK,
        },
        batch_size=[B, T + (SIZE - 1)],
    ),
)

B = 2
T = 4
TOTAL = B * T
INPUTS = torch.cat(
    [torch.zeros(B, (SIZE - 1), 1, 1, 1), torch.arange(TOTAL).reshape(B, T, 1, 1, 1)],
    dim=1,
).float()
PADDING_MASK = torch.zeros(B, T + (SIZE - 1)).bool()
PADDING_MASK[:, : (SIZE - 1)] = True
PAD_WHOLE_SEQUENCE_CASE_2 = (
    torch.arange(TOTAL).reshape(B, T, 1, 1, 1).float(),
    TensorDict(
        {
            Batch.INPUTS.value: INPUTS,
            Batch.PADDING_MASK.value: PADDING_MASK,
        },
        batch_size=[B, T + (SIZE - 1)],
    ),
)


@pytest.mark.parametrize(
    "inputs,expected",
    [
        PAD_WHOLE_SEQUENCE_CASE_0,
        PAD_WHOLE_SEQUENCE_CASE_1,
        PAD_WHOLE_SEQUENCE_CASE_2,
    ],
)
def test_pad_whole_sequence(
    inputs: torch.Tensor, expected: torch.Tensor | TensorDict
) -> None:
    assert (pad_whole_sequence(inputs, SIZE) == expected).all()


# PADDED_ROLLING_WINDOW_APPLY_ALL_CASE_0 = (
#     ROLLING_WINDOW_CASE_0[0],
#     ROLLING_WINDOW_CASE_0[1].reshape(-1, SIZE),
# )

# PADDED_ROLLING_WINDOW_APPLY_ALL_CASE_1 = (
#     ROLLING_WINDOW_CASE_1[0],
#     ROLLING_WINDOW_CASE_1[1].reshape(-1, SIZE, 1),
# )


# @pytest.mark.parametrize(
#     "inputs,expected",
#     [
#         ROLLING_WINDOW_APPLY_ALL_CASE_0,
#         ROLLING_WINDOW_APPLY_ALL_CASE_1,
#     ],
# )
# def test_padded_rolling_window_apply_all(inputs: torch.Tensor, expected: TensorDict) -> None:
#     assert (PaddedRollingWindow.apply_all(inputs, SIZE) == expected).all()


B = 2
T = 1
TOTAL = B * T
INPUTS = TensorDict({"x": torch.arange(TOTAL).reshape(B, T).float()}, batch_size=[B, T])
EXPECTED_INPUT = TensorDict({}, batch_size=[B, SIZE])
EXPECTED_INPUT["x"] = TensorDict({}, batch_size=[B, SIZE])
EXPECTED_INPUT["x"][Batch.INPUTS.value] = torch.cat(
    [torch.zeros(B, SIZE - 1), INPUTS["x"].squeeze(-1)], dim=1
)
EXPECTED_INPUT["x"][Batch.PADDING_MASK.value] = torch.zeros(B, SIZE).bool()
EXPECTED_INPUT["x"][Batch.PADDING_MASK.value][:, 0] = True
PADDED_ROLLING_WINDOW_APPLY_LAST_CASE_0 = (
    INPUTS,
    EXPECTED_INPUT,
)

B = 2
T = 4
TOTAL = B * T
INPUTS = TensorDict(
    {"x": torch.arange(TOTAL).reshape(B, T, 1).float()}, batch_size=[B, T]
)
EXPECTED_INPUT = TensorDict({}, batch_size=[B, SIZE])
EXPECTED_INPUT["x"] = TensorDict({}, batch_size=[B, SIZE])
EXPECTED_INPUT["x"][Batch.INPUTS.value] = INPUTS["x"][:, -SIZE:, ...]
EXPECTED_INPUT["x"][Batch.PADDING_MASK.value] = torch.zeros(B, SIZE).bool()
PADDED_ROLLING_WINDOW_APPLY_LAST_CASE_1 = (
    INPUTS,
    EXPECTED_INPUT,
)


@pytest.mark.parametrize(
    "inputs,expected",
    [
        PADDED_ROLLING_WINDOW_APPLY_LAST_CASE_0,
        PADDED_ROLLING_WINDOW_APPLY_LAST_CASE_1,
    ],
)
def test_padded_rolling_window_apply_last(
    inputs: TensorDict, expected: TensorDict
) -> None:
    assert (PaddedRollingWindow.apply_last(inputs, SIZE) == expected).all()


ROLLING_WINDOW_CASE_0 = (
    torch.arange(8).reshape(2, 4).float(),
    torch.tensor([[[0, 1], [1, 2], [2, 3]], [[4, 5], [5, 6], [6, 7]]]).float(),
)

ROLLING_WINDOW_CASE_1 = (
    torch.arange(8).reshape(2, 4, 1).float(),
    torch.tensor(
        [[[[0], [1]], [[1], [2]], [[2], [3]]], [[[4], [5]], [[5], [6]], [[6], [7]]]]
    ).float(),
)


@pytest.mark.parametrize(
    "inputs,expected",
    [
        ROLLING_WINDOW_CASE_0,
        ROLLING_WINDOW_CASE_1,
    ],
)
def test_rolling_window(inputs: torch.Tensor, expected: torch.Tensor) -> None:
    assert (rolling_window(inputs, SIZE) == expected).all()


ROLLING_WINDOW_APPLY_ALL_CASE_0 = (
    ROLLING_WINDOW_CASE_0[0],
    ROLLING_WINDOW_CASE_0[1].reshape(-1, SIZE),
)

ROLLING_WINDOW_APPLY_ALL_CASE_1 = (
    ROLLING_WINDOW_CASE_1[0],
    ROLLING_WINDOW_CASE_1[1].reshape(-1, SIZE, 1),
)


@pytest.mark.parametrize(
    "inputs,expected",
    [
        ROLLING_WINDOW_APPLY_ALL_CASE_0,
        ROLLING_WINDOW_APPLY_ALL_CASE_1,
    ],
)
def test_rolling_window_apply_all(inputs: torch.Tensor, expected: torch.Tensor) -> None:
    assert (RollingWindow.apply_all(inputs, SIZE) == expected).all()


B = 2
T = 4
TOTAL = B * T
INPUTS = TensorDict({"x": torch.arange(TOTAL).reshape(B, T).float()}, batch_size=[B, T])
ROLLING_WINDOW_APPLY_LAST_CASE_0 = (
    INPUTS,
    INPUTS[:, -SIZE:, ...],
)

B = 2
T = 4
TOTAL = B * T
INPUTS = TensorDict(
    {"x": torch.arange(TOTAL).reshape(B, T, 1).float()}, batch_size=[B, T]
)
ROLLING_WINDOW_APPLY_LAST_CASE_1 = (
    INPUTS,
    INPUTS[:, -SIZE:, ...],
)


@pytest.mark.parametrize(
    "inputs,expected",
    [
        ROLLING_WINDOW_APPLY_LAST_CASE_0,
        ROLLING_WINDOW_APPLY_LAST_CASE_1,
    ],
)
def test_rolling_window_apply_last(inputs: TensorDict, expected: TensorDict) -> None:
    assert (RollingWindow.apply_last(inputs, SIZE) == expected).all()


# def test_view_requirement_apply_all() -> None:
#     ...


# def test_view_requirement_apply_last() -> None:
#     ...
