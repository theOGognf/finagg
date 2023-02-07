"""Typing help for `torch.nn.Module`."""

from abc import ABC, abstractmethod
from typing import Generic, ParamSpec, TypeVar

import torch.nn as nn

_P = ParamSpec("_P")
_T = TypeVar("_T")


class Module(ABC, nn.Module, Generic[_P, _T]):
    """Workaround for `torch.nn.Module` variadic generics."""

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> _T:
        return super().__call__(*args, **kwargs)  # type: ignore[no-any-return]

    @abstractmethod
    def forward(self, *args: _P.args, **kwargs: _P.kwargs) -> _T:
        """Subclasses implement this method. This is called by `__call__`."""
