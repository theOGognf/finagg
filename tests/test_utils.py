import pytest

import finagg


@pytest.mark.parametrize(
    "s,expected", [("foo_bar", "FooBar"), ("FooBar", "Foobar"), ("fooBar", "Foobar")]
)
def test_CamelCase(s: str, expected: str) -> None:
    assert finagg.utils.CamelCase(s) == expected


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


@pytest.mark.parametrize(
    "s,expected", [("foo_bar", "foo_bar"), ("FooBar", "foo_bar"), ("fooBar", "foo_bar")]
)
def test_snake_case(s: str, expected: str) -> None:
    assert finagg.utils.snake_case(s) == expected
