import re


def CamelCase(s: str) -> str:
    """Convert a string to CamelCase.

    Credit:
        https://stackoverflow.com/a/1176023

    Examples:
        >>> CamelCase("snakes_are_dope")
        "SnakesAreDope"

        >>> CamelCase("bar")
        "Bar"

    """
    return "".join(word.title() for word in s.split("_"))


def join_with(s: str | list[str], delim: str) -> str:
    """Join a sequence of strings with the delimiter `delim`.

    Examples:
        >>> join_with(["foo", "bar"], ",")
        "foo,bar"

        >>> join_with("foo")
        "foo"

    """
    if isinstance(s, str):
        s = [s]
    return delim.join(s)


def snake_case(s: str) -> str:
    """Convert a string to snake_case.

    Credit:
        https://stackoverflow.com/a/1176023

    Examples:
        >>> snake_case("CamelsAreCool")
        "camels_are_cool"

        >>> snake_case("Foo")
        "foo"

    """
    s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub("__([A-Z])", r"_\1", s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()
