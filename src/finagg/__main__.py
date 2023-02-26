"""Main CLI entry points."""

import logging
import multiprocessing as mp
import os
import pathlib

import click

from . import fred, fundam, indices, sec, utils, yfinance

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    ...


cli.add_command(fred._cli.entry_point, "fred")
cli.add_command(fundam._cli.entry_point, "fundam")
cli.add_command(indices._cli.entry_point, "indices")
cli.add_command(sec._cli.entry_point, "sec")
cli.add_command(yfinance._cli.entry_point, "yfinance")


@cli.command(
    help="Set API keys/user agents, drop and recreate tables, "
    "and install the recommended datasets into the SQL database.",
)
@click.option(
    "--raw",
    "-r",
    is_flag=True,
    default=False,
    help="Whether to install raw data (data directly from APIs as-is).",
)
@click.option(
    "--all",
    "-a",
    "all_",
    is_flag=True,
    default=False,
    help="Whether to install all defined tables (including all refined tables).",
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
    raw: bool = False,
    all_: bool = False,
    processes: int = mp.cpu_count() - 1,
    verbose: bool = False,
) -> None:
    if "FINAGG_ROOT_PATH" not in os.environ:
        entered_root_path = input(
            "Enter the path to your finagg root directory. finagg data will "
            "be written to /path/to/root/findata/.\n\n"
            "Root path (hit ENTER to use your current working directory): "
        ).strip()
        if not entered_root_path:
            root_path = pathlib.Path.cwd()
        else:
            root_path = pathlib.Path(entered_root_path).expanduser().resolve()
        p = utils.setenv("FINAGG_ROOT_PATH", str(root_path))
        logger.info(f"FINAGG_ROOT_PATH written to {p}")
    else:
        logger.info("FINAGG_ROOT_PATH found in the environment")

    if all_ or raw:
        ctx.invoke(fred._cli.install, raw=raw, all_=all_, verbose=verbose)
        ctx.invoke(indices._cli.install, all_=all_)
        ctx.invoke(
            sec._cli.install, raw=raw, all_=all_, processes=processes, verbose=verbose
        )
        ctx.invoke(
            yfinance._cli.install,
            raw=raw,
            all_=all_,
            processes=processes,
            verbose=verbose,
        )
        ctx.invoke(fundam._cli.install, raw=raw, all_=all_, processes=processes)
    else:
        logger.info(
            "Skipping installation because no installation options are provided"
        )


def main() -> int:
    """Create and run parsers according to the given commands."""
    cli()
    return 0


if __name__ == "__main__":
    SystemExit(main())
