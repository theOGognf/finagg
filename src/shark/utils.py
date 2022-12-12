"""Generic utils used by subpackages."""

import os
import pathlib
import platform
import re
import subprocess


def CamelCase(s: str, /) -> str:
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


def join_with(s: str | list[str], /, delim: str) -> str:
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


def setenv(name: str, value: str, /, *, exist_ok: bool = False) -> None | pathlib.Path:
    """Set the value of the environment variable `name` to `value`.

    The environment variable is permanently set in the environment
    and in the current process.

    Args:
        name: Environment variable name.
        value: Environment variable value.
        exist_ok: Whether it's okay if an environment variable of the
            same name already exists. If `True`, it will be overwritten.

    Raises:
        RuntimeError:
            - If `exist_ok` is `False` and an environment variable
                of the same name already exists
            - If the environment variable couldn't be set.

    """
    if not exist_ok and name in os.environ:
        raise RuntimeError(
            f"The env variable `{name}` already exists. "
            "Set `exist_ok` to `True` to overwrite it."
        )

    os.environ[name] = value
    match platform.system():
        case "Linux" | "Mac":
            home = pathlib.Path.home()
            env_files = (".bashrc", ".bash_profile", ".profile")
            for f in env_files:
                p = home / f
                if pathlib.Path.exists(p):
                    with open(p, "a") as env_file:
                        env_file.write(f"export {name}={value}\n")

                    with open(p, "r") as env_file:
                        eof = env_file.readlines()[-1]
                        if f"export {name}={value}" not in eof:
                            continue

                    subprocess.run(["source", str(p)])
                    return p
            raise RuntimeError(
                f"Unable to set `{name}` in {env_files}. "
                "Try manually setting the environment variable yourself."
            )

        case "Windows":
            subprocess.run(["setx", name, value])
    return None


def snake_case(s: str, /) -> str:
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
