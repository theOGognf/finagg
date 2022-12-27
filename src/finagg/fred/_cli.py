"""FRED CLI and tools."""

import argparse

from . import install, scrape, sql


class Command:
    """FRED CLI subcommand."""

    def __init__(self, parent: argparse._SubParsersAction) -> None:
        self.fred_parser: argparse.ArgumentParser = parent.add_parser(
            "fred", help="Federal Reserve Economic Data (FRED) tools."
        )
        subparser = self.fred_parser.add_subparsers(dest="fred_cmd")
        self.install_parser = subparser.add_parser(
            "install",
            help="Set the FRED API key, drop and recreate tables, "
            "and scrape the recommended datasets and features into the SQL database.",
        )
        self.install_parser.add_argument(
            "--features",
            action="store_true",
            help="Whether to install features with the recommended datasets.",
        )
        self.ls_parser = subparser.add_parser(
            "ls", help="List all economic data series within the SQL database."
        )
        self.scrape_parser = subparser.add_parser(
            "scrape",
            help="Scrape a specified economic data series into the SQL database.",
        )
        self.scrape_parser.add_argument(
            "--series", action="append", required=True, help="Series IDs to scrape."
        )

    def run(self, cmd: str) -> None:
        """Run the FRED command specified by `cmd`."""
        match cmd:
            case "install":
                args, _ = self.install_parser.parse_known_args()
                install.run(install_features=args.features)

            case "ls":
                for series in sorted(sql.get_series_set()):
                    print(series)

            case "scrape":
                args, _ = self.scrape_parser.parse_known_args()
                scrape.run(args.series)

            case _:
                raise ValueError(f"{cmd} is not supported")
