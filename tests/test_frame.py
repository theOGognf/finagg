import pytest

from finagg.frame import FiscalDelta, FiscalFrame, is_valid_fiscal_seq


@pytest.mark.parametrize(
    "frame,delta,expected",
    [
        (FiscalFrame(2000, 1), FiscalDelta(quarters=1), FiscalFrame(2000, 2)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=2), FiscalFrame(2000, 3)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=3), FiscalFrame(2000, 4)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=4), FiscalFrame(2001, 1)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=5), FiscalFrame(2001, 2)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=1), FiscalFrame(2001, 1)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=2), FiscalFrame(2001, 2)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=3), FiscalFrame(2001, 3)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=4), FiscalFrame(2001, 4)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=5), FiscalFrame(2002, 1)),
    ],
)
def test_fiscal_frame_add_delta(
    frame: FiscalFrame, delta: FiscalDelta, expected: FiscalFrame
) -> None:
    assert (frame + delta) == expected


@pytest.mark.parametrize(
    "frame,delta,expected",
    [
        (FiscalFrame(2000, 1), 1, FiscalFrame(2000, 2)),
        (FiscalFrame(2000, 1), 2, FiscalFrame(2000, 3)),
        (FiscalFrame(2000, 1), 3, FiscalFrame(2000, 4)),
        (FiscalFrame(2000, 1), 4, FiscalFrame(2001, 1)),
        (FiscalFrame(2000, 1), 5, FiscalFrame(2001, 2)),
        (FiscalFrame(2000, 4), 1, FiscalFrame(2001, 1)),
        (FiscalFrame(2000, 4), 2, FiscalFrame(2001, 2)),
        (FiscalFrame(2000, 4), 3, FiscalFrame(2001, 3)),
        (FiscalFrame(2000, 4), 4, FiscalFrame(2001, 4)),
        (FiscalFrame(2000, 4), 5, FiscalFrame(2002, 1)),
    ],
)
def test_fiscal_frame_add_int(
    frame: FiscalFrame, delta: int, expected: FiscalFrame
) -> None:
    assert (frame + delta) == expected


@pytest.mark.parametrize(
    "frame,delta,expected",
    [
        (FiscalFrame(2000, 1), FiscalDelta(quarters=1), FiscalFrame(1999, 4)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=2), FiscalFrame(1999, 3)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=3), FiscalFrame(1999, 2)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=4), FiscalFrame(1999, 1)),
        (FiscalFrame(2000, 1), FiscalDelta(quarters=5), FiscalFrame(1998, 4)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=1), FiscalFrame(2000, 3)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=2), FiscalFrame(2000, 2)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=3), FiscalFrame(2000, 1)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=4), FiscalFrame(1999, 4)),
        (FiscalFrame(2000, 4), FiscalDelta(quarters=5), FiscalFrame(1999, 3)),
    ],
)
def test_fiscal_frame_sub_delta(
    frame: FiscalFrame, delta: FiscalDelta, expected: FiscalFrame
) -> None:
    assert (frame - delta) == expected


@pytest.mark.parametrize(
    "frame,delta,expected",
    [
        (FiscalFrame(2000, 1), 1, FiscalFrame(1999, 4)),
        (FiscalFrame(2000, 1), 2, FiscalFrame(1999, 3)),
        (FiscalFrame(2000, 1), 3, FiscalFrame(1999, 2)),
        (FiscalFrame(2000, 1), 4, FiscalFrame(1999, 1)),
        (FiscalFrame(2000, 1), 5, FiscalFrame(1998, 4)),
        (FiscalFrame(2000, 4), 1, FiscalFrame(2000, 3)),
        (FiscalFrame(2000, 4), 2, FiscalFrame(2000, 2)),
        (FiscalFrame(2000, 4), 3, FiscalFrame(2000, 1)),
        (FiscalFrame(2000, 4), 4, FiscalFrame(1999, 4)),
        (FiscalFrame(2000, 4), 5, FiscalFrame(1999, 3)),
    ],
)
def test_fiscal_frame_sub_int(
    frame: FiscalFrame, delta: int, expected: FiscalFrame
) -> None:
    assert (frame - delta) == expected


@pytest.mark.parametrize(
    "seq,expected",
    [
        ([1, 1, 2, 1, 1, 2, 1], True),
        ([2, 1, 1, 2, 1, 1], True),
        ([1, 2, 1, 1, 2], True),
        ([2, 2, 1, 1], False),
        ([1, 2, 1, 2, 1, 1], False),
    ],
)
def test_is_valid_fiscal_seq(seq: list[int], expected: bool) -> None:
    assert is_valid_fiscal_seq(seq) == expected
