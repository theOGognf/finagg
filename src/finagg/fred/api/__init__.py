"""FRED top-level interface."""

from ._api import get
from ._category import category
from ._release import release, releases
from ._series import series
from ._source import source, sources
from ._tags import related_tags, tags
