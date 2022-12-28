"""Main CLI entry points."""

import click

from . import fred, indices, mixed, sec, yfinance


@click.group()
def cli() -> None:
    ...


@cli.command(
    help="Set API keys/user agents, drop and recreate tables, "
    "and scrape the recommended datasets and features into the SQL database.",
)
@click.option(
    "--install-features",
    is_flag=True,
    default=False,
    help="Whether to install features with the recommended datasets.",
)
def install(install_features: bool = False) -> None:
    from . import install

    install.run(install_features=install_features)


cli.add_command(fred._cli.entry_point, "fred")
cli.add_command(indices._cli.entry_point, "indices")


def main() -> int:
    """Create and run parsers according to the given commands."""
    cli()
    return 0


if __name__ == "__main__":
    SystemExit(main())
