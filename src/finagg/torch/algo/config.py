"""Definitions related to grouping algorithm config params."""

from dataclasses import dataclass

from ... import utils


class Tags:
    ...


@dataclass
class AlgorithmConfig(utils.Taggable):
    ...


@dataclass
class TrainerConfig(utils.Taggable):
    ...
