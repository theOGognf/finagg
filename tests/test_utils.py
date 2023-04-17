import pandas as pd
import pytest
import sqlalchemy as sa

import finagg


@pytest.mark.parametrize(
    "s,expected", [("foo_bar", "FooBar"), ("FooBar", "Foobar"), ("fooBar", "Foobar")]
)
def test_CamelCase(s: str, expected: str) -> None:
    assert finagg.utils.CamelCase(s) == expected


def test_get_func_cols() -> None:
    table = sa.Table(
        "test",
        sa.MetaData(),
        sa.Column("a", sa.String, primary_key=True),
        sa.Column("LOWER(b)", sa.String, nullable=False),
        sa.Column("c", sa.String, nullable=False),
        sa.Column("UPPER(d)", sa.String, nullable=False),
    )
    assert tuple(finagg.utils.get_func_cols(table)) == ("LOWER(b)", "UPPER(d)")


@pytest.mark.parametrize(
    "s,expected",
    [
        ("LOG_CHANGE(high, open)", ("LOG_CHANGE", ["high", "open"])),
        ("LOG_CHANGE(open)", ("LOG_CHANGE", ["open"])),
    ],
)
def test_parse_func_call(s: str, expected: tuple[str, list[str]]) -> None:
    expected_name, expected_args = expected
    name, args = finagg.utils.parse_func_call(s)
    assert name == expected_name
    assert len(args) == len(expected_args)
    assert all([x == y for x, y in zip(args, expected_args)])


def test_resolve_col_order() -> None:
    table = sa.Table(
        "test",
        sa.MetaData(),
        sa.Column("a", sa.String, primary_key=True),
        sa.Column("b", sa.String, nullable=False),
        sa.Column("c", sa.String, nullable=False),
        sa.Column("d", sa.String, nullable=False),
    )
    df1 = pd.DataFrame(
        {"a": ["foo"], "c": ["cow"], "d": ["yes"], "b": ["bar"]}
    ).set_index("a")
    df2 = finagg.utils.resolve_col_order(table, df1)
    assert tuple(df2.columns) == ("b", "c", "d")


def test_resolve_col_order_extra_ignore() -> None:
    table = sa.Table(
        "test",
        sa.MetaData(),
        sa.Column("q", sa.String, primary_key=True),
        sa.Column("r", sa.String, nullable=False),
        sa.Column("s", sa.String, nullable=False),
        sa.Column("t", sa.String, nullable=False),
    )
    df1 = pd.DataFrame(
        {"t": ["foo"], "s": ["cow"], "r": ["yes"], "q": ["bar"]}
    ).set_index(["q", "t"])
    df2 = finagg.utils.resolve_col_order(table, df1, extra_ignore=["t"])
    assert tuple(df2.columns) == ("r", "s")


def test_safe_log_change() -> None:
    series = pd.Series([1, 2, 1])
    assert finagg.utils.safe_log_change(series).sum() == 0


def test_safe_pct_change() -> None:
    series = pd.Series([1, 2, 1])
    assert finagg.utils.safe_pct_change(series).sum() == 0.5


@pytest.mark.parametrize(
    "s,expected", [("foo_bar", "foo_bar"), ("FooBar", "foo_bar"), ("fooBar", "foo_bar")]
)
def test_snake_case(s: str, expected: str) -> None:
    assert finagg.utils.snake_case(s) == expected
