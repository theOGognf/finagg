import finagg


def test_fixed_assets_get_parameter_list() -> None:
    finagg.bea.api.fixed_assets.get_parameter_list()


def test_gdp_by_industry_get_parameter_list() -> None:
    finagg.bea.api.gdp_by_industry.get_parameter_list()


def test_input_output_get_parameter_list() -> None:
    finagg.bea.api.input_output.get_parameter_list()


def test_nipa_get_parameter_list() -> None:
    finagg.bea.api.nipa.get_parameter_list()
