"""Taggable dataclass utils."""

from dataclasses import Field, dataclass
from typing import Any, Sequence


@dataclass
class Taggable:
    """A dataclass that allows dataclass attributes to be grouped according
    to their respective `Field.metadata["tag"]` value.

    Useful for keeping dataclasses with many fields flat and organized.

    """

    def group(self, tags: str | Sequence[str], /) -> dict[str, Any]:
        """Return dataclass attributes whose metadata `tag` that match any of
        `tags`.

        Args:
            tags: Tag names to match. Return an attribute if it matches any of
                these provided tags.

        Returns:
            Mapping of attribute name to its current value.

        """
        if isinstance(tags, str):
            tags = [tags]

        out = {}
        for k, v in self.__dataclass_fields__:
            if isinstance(v, Field):
                if "tags" in v.metadata:
                    if any([t in v.metadata["tags"] for t in tags]):
                        out[k] = getattr(self, k)
        return out
