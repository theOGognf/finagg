"""SEC CLI and tools."""

import logging
import os

import click

from .. import utils

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


@click.group(help="Bureau of Economic Analysis (BEA) tools.")
def entry_point() -> None:
    ...


@entry_point.command(help="Set the BEA API key.")
def install() -> int:
    if "BEA_API_KEY" not in os.environ:
        api_key = input(
            "Enter your BEA API key below.\n\n"
            "You can request a BEA API key at\n"
            "https://apps.bea.gov/api/signup/.\n\n"
            "BEA API key (leave blank and hit ENTER to skip): "
        ).strip()
        if not api_key:
            logger.warning(
                "An empty BEA API key was given. Skipping finagg.bea installation."
            )
            return 0
        p = utils.setenv("BEA_API_KEY", api_key)
        logger.info(f"BEA API key written to {p}")
    else:
        logger.info("BEA API key found in the environment")
    return 0
