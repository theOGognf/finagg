import re


def CamelCase(s: str) -> str:
    """Convert a string to CamelCase."""
    return "".join(word.title() for word in s.split("_"))


def snake_case(s: str) -> str:
    """Convert a string to snake_case."""
    s = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    s = re.sub("__([A-Z])", r"_\1", s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()
