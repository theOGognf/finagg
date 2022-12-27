"""Main CLI entry points."""

import argparse

from . import fred, indices, install, mixed, sec, yfinance


def main() -> int:
    """Create and run parsers according to the given commands."""
    parser = argparse.ArgumentParser(
        description="finagg command line utilities. Scrape APIs, train models, query models, and more."
    )
    subparsers = parser.add_subparsers(dest="base_cmd")

    # finagg indices ...
    indices_cmd = indices._cli.Command(subparsers)

    # finagg install ...
    install_parser = subparsers.add_parser(
        "install",
        help="Set API keys/user agents, drop and recreate tables, "
        "and scrape the recommended datasets and features into the SQL database.",
    )
    install_parser.add_argument(
        "--features",
        action="store_true",
        help="Whether to install features with the recommended datasets.",
    )

    # finagg fred ...
    fred_cmd = fred._cli.Command(subparsers)

    # finagg mixed ...
    mixed_cmd = mixed._cli.Command(subparsers)

    # finagg sec ...
    sec_cmd = sec._cli.Command(subparsers)

    # finagg yfinance ...
    yfinance_cmd = yfinance._cli.Command(subparsers)

    args = parser.parse_args()
    match args.base_cmd:
        case "fred":
            fred_cmd.run(args.fred_cmd)

        case "indices":
            indices_cmd.run(args.indices_cmd)

        case "install":
            install_args, _ = install_parser.parse_known_args()
            install.run(install_features=install_args.features)

        case "mixed":
            mixed_cmd.run(args.mixed_cmd)

        case "sec":
            sec_cmd.run(args.sec_cmd)

        case "yfinance":
            yfinance_cmd.run(args.yfinance_cmd)

    return 0


if __name__ == "__main__":
    SystemExit(main())
