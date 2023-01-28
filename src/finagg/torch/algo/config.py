from dataclasses import Field, dataclass
from typing import Sequence


class Tags:
    ...


@dataclass
class Taggable:
    def group(self, tags: str | Sequence[str]) -> dict[str, Field]:
        if isinstance(tags, str):
            tags = [tags]

        out = {}
        for k, v in self.__dataclass_fields__:
            if isinstance(v, Field):
                if "tags" in v.metadata:
                    if any([t in v.metadata["tags"] for t in tags]):
                        out[k] = v
        return out


@dataclass
class AlgorithmConfig(Taggable):
    ...


@dataclass
class TrainerConfig(Taggable):
    ...
