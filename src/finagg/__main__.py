import argparse

from . import fred, indices, install, sec, yfinance


def main() -> int:
    parser = argparse.ArgumentParser(
        description="finagg command line utilities. Scrape APIs, train models, query models, and more."
    )
    subparsers = parser.add_subparsers(dest="base_cmd")

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
    mixed_parser = subparsers.add_parser(
        "mixed", help="Tools for features built from several submodules."
    )
    mixed_subparsers = mixed_parser.add_subparsers(dest="mixed")
    mixed_install = mixed_subparsers.add_parser(
        "install",
        help="Drop and recreate tables, and scrape the recommended features into the SQL database.",
    )

    # finagg sec ...
    sec_cmd = sec._cli.Command(subparsers)

    # finagg yfinance ...
    yfinance_cmd = yfinance._cli.Command(subparsers)

    args = parser.parse_args()
    match args.base_cmd:
        case "install":
            install_args, _ = install_parser.parse_known_args()
            install.run(install_features=install_args.features)

        case "fred":
            fred_cmd.run(args.fred_cmd)

        case "sec":
            sec_cmd.run(args.sec_cmd)

        case "yfinance":
            yfinance_cmd.run(args.yfinance_cmd)

    return 0


if __name__ == "__main__":
    SystemExit(main())
