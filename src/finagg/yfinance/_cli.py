"""CLI and tools for yfinance."""

import argparse

from . import install, scrape, sql


class Command:
    """Yahoo! finance CLI subcommand."""

    def __init__(self, parent: argparse._SubParsersAction) -> None:
        self.parser: argparse.ArgumentParser = parent.add_parser(
            "yfinance", help="Yahoo! finance tools."
        )
        subparser = self.parser.add_subparsers(dest="yfinance_cmd")
        self.install_parser = subparser.add_parser(
            "install",
            help="Drop and recreate tables, "
            "and scrape the recommended datasets and features into the SQL database.",
        )
        self.install_parser.add_argument(
            "--features",
            action="store_true",
            help="Whether to install features with the recommended datasets.",
        )
        self.ls_parser = subparser.add_parser(
            "ls", help="List all tickers within the SQL database."
        )
        self.scrape_parser = subparser.add_parser(
            "scrape", help="Scrape a specified ticker history into the SQL database."
        )
        self.scrape_parser.add_argument(
            "--ticker", action="append", required=True, help="Ticker to scrape."
        )

    def run(self, cmd: str) -> None:
        """Run the Yahoo! finance command specified by `cmd`."""
        match cmd:
            case "install":
                args, _ = self.install_parser.parse_known_args()
                install.run(install_features=args.features)

            case "ls":
                for ticker in sorted(sql.get_ticker_set()):
                    print(ticker)

            case "scrape":
                args, _ = self.scrape_parser.parse_known_args()
                scrape.run(args.ticker)
