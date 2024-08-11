from typing import Callable

import pytest
from requests import HTTPError

import finagg


def skip_if_503(f: Callable) -> None:
    """The BEA API is notorious for being unreliable. We don't really
    support maintaining the BEA API's Python client implementation anymore,
    so we just skip if they error out. We'll keep them just in case we
    feel like supporting it in the future.

    """
    try:
        f()
    except HTTPError as e:
        if e.response.status_code == 503:
            pytest.skip()


def test_fixed_assets_get_parameter_list() -> None:
    skip_if_503(finagg.bea.api.fixed_assets.get_parameter_list)


def test_gdp_by_industry_get_parameter_list() -> None:
    skip_if_503(finagg.bea.api.gdp_by_industry.get_parameter_list)


def test_input_output_get_parameter_list() -> None:
    skip_if_503(finagg.bea.api.input_output.get_parameter_list)


def test_nipa_get_parameter_list() -> None:
    skip_if_503(finagg.bea.api.nipa.get_parameter_list)
