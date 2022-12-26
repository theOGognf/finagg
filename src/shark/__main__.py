import argparse

from . import fred, indices, sec, yfinance


def main() -> int:
    parser = argparse.ArgumentParser(
        description="shark command line utilities. Scrape APIs, train models, query models, and more."
    )
    subparsers = parser.add_subparsers(
        help="Subcommands for the main package and other submodules."
    )
    install_parser = subparsers.add_parser("install", help="Main installation methods.")
    fred_parser = subparsers.add_parser(
        "fred", help="Federal Reserve Economic Data (FRED) tools."
    )


if __name__ == "__main__":
    SystemExit(main())
