"""Main package interface."""

from importlib.metadata import PackageNotFoundError, version

from dotenv import load_dotenv

load_dotenv()

from . import bea, config, fred, sec, testing, utils

try:
    __version__ = version("finagg")
except PackageNotFoundError:
    pass
