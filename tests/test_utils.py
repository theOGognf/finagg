import pytest

from finagg.utils import CamelCase, join_with, snake_case


@pytest.mark.parametrize(
    "s,expected", [("foo_bar", "FooBar"), ("FooBar", "Foobar"), ("fooBar", "Foobar")]
)
def test_CamelCase(s: str, expected: str) -> None:
    assert CamelCase(s) == expected


@pytest.mark.parametrize(
    "s,delim,expected", [(("foo", "bar"), ",", "foo,bar"), ("foo", "-", "foo")]
)
def test_join_with(s: str | list[str], delim: str, expected: str) -> None:
    assert join_with(s, delim=delim) == expected


@pytest.mark.parametrize(
    "s,expected", [("foo_bar", "foo_bar"), ("FooBar", "foo_bar"), ("fooBar", "foo_bar")]
)
def test_snake_case(s: str, expected: str) -> None:
    assert snake_case(s) == expected
