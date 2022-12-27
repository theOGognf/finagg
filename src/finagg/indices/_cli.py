"""CLI and tools for aggregating tickers in common indices."""

import argparse

from . import install


class Command:
    """Indices subcommand."""

    def __init__(self, parent: argparse._SubParsersAction) -> None:
        self.parser: argparse.ArgumentParser = parent.add_parser(
            "indices", help="Indices tools."
        )
        subparser = self.parser.add_subparsers(dest="indices_cmd")
        self.install_parser = subparser.add_parser(
            "install",
            help="Drop and recreate tables, and scrape tickers into the SQL database.",
        )

    def run(self, cmd: str) -> None:
        """Run the indices command specified by `cmd`."""
        match cmd:
            case "install":
                install.run()

            case _:
                raise ValueError(f"{cmd} is not supported")
