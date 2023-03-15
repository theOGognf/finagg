"""FRED top-level interface."""

from ._api import get
from .category_ import category
from .release_ import release, releases
from .series_ import series
from .source_ import source, sources
from .tags_ import related_tags, tags
