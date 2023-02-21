"""Main CLI entry points."""

import multiprocessing as mp

import click

from . import fred, fundam, indices, sec, yfinance


@click.group()
def cli() -> None:
    ...


cli.add_command(fred._cli.entry_point, fred.__name__)
cli.add_command(indices._cli.entry_point, indices.__name__)
cli.add_command(fundam._cli.entry_point, fundam.__name__)
cli.add_command(sec._cli.entry_point, sec.__name__)
cli.add_command(yfinance._cli.entry_point, yfinance.__name__)


@cli.command(
    help="Set API keys/user agents, drop and recreate tables, "
    "and install the recommended datasets into the SQL database.",
)
@click.option(
    "--processes",
    "-n",
    type=int,
    default=mp.cpu_count() - 1,
    help="Number of background processes to use for installing feature data.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Sets the log level to DEBUG to show installation errors for each ticker.",
)
@click.pass_context
def install(
    ctx: click.Context,
    processes: int = mp.cpu_count() - 1,
    verbose: bool = False,
) -> None:
    ctx.invoke(indices._cli.install, all_=True)
    ctx.invoke(sec._cli.install, all_=True, processes=processes, verbose=verbose)
    ctx.invoke(yfinance._cli.install, all_=True, processes=processes, verbose=verbose)


def main() -> int:
    """Create and run parsers according to the given commands."""
    cli()
    return 0


if __name__ == "__main__":
    SystemExit(main())
