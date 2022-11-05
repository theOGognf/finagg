from typing import ClassVar

import pandas as pd

from ._api import _Dataset, get


class _Category(_Dataset):
    """Collection of `fred/category` APIs."""

    url: ClassVar[str] = "https://api.stlouisfed.org/fred/category"

    @classmethod
    def get(cls, category_id: int = 0, *, api_key: None | str = None) -> pd.DataFrame:
        params = {"category_id": category_id}
        response = get(cls.url, params, api_key=api_key)
        return response
