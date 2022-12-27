"""CLI and tools for aggregating mixed features."""

import argparse

from . import install


class Command:
    """Mixed features subcommand."""

    def __init__(self, parent: argparse._SubParsersAction) -> None:
        self.parser: argparse.ArgumentParser = parent.add_parser(
            "mixed", help="Mixed feature tools."
        )
        subparser = self.parser.add_subparsers(dest="mixed_cmd")
        self.install_parser = subparser.add_parser(
            "install",
            help="Drop and recreate tables, and install features into the SQL database.",
        )

    def run(self, cmd: str) -> None:
        """Run the mixed features command specified by `cmd`."""
        match cmd:
            case "install":
                install.run()

            case _:
                raise ValueError(f"{cmd} is not supported")
