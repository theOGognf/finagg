import pytest

from finagg.portfolio import Portfolio, Position

CHANGE_MULTIPLE = 0.5
INITIAL_CASH = 10_000
INITIAL_COST = 100
INITIAL_QUANTITY = 10


@pytest.fixture
def portfolio() -> Portfolio:
    return Portfolio(INITIAL_CASH)


@pytest.fixture
def position() -> Position:
    return Position(INITIAL_COST, INITIAL_QUANTITY)


def test_portfolio_buy(portfolio: Portfolio, position: Position) -> None:
    portfolio.buy("TEST", INITIAL_COST, INITIAL_QUANTITY)
    assert portfolio.cash == (INITIAL_CASH - (INITIAL_COST * INITIAL_QUANTITY))
    assert portfolio["TEST"] == position
    assert portfolio["TEST"].cost_basis_total == position.cost_basis_total
    assert portfolio["TEST"].quantity == position.quantity


def test_portfolio_sell(portfolio: Portfolio) -> None:
    portfolio.buy("TEST", INITIAL_COST, INITIAL_QUANTITY)
    portfolio.sell("TEST", CHANGE_MULTIPLE * INITIAL_COST, INITIAL_QUANTITY)
    assert portfolio.cash == (
        INITIAL_CASH - (CHANGE_MULTIPLE * INITIAL_COST * INITIAL_QUANTITY)
    )
    assert "TEST" not in portfolio


def test_portfolio_total_dollar_change(portfolio: Portfolio) -> None:
    portfolio.buy("TEST", INITIAL_CASH, 1)
    assert portfolio.total_dollar_change({"TEST": CHANGE_MULTIPLE * INITIAL_CASH}) == (
        -CHANGE_MULTIPLE * INITIAL_CASH
    )
    assert portfolio.total_dollar_change({"TEST": 0.0}) == -INITIAL_CASH


def test_portfolio_total_percent_change(portfolio: Portfolio) -> None:
    portfolio.buy("TEST", INITIAL_CASH, 1)
    assert (
        portfolio.total_percent_change({"TEST": CHANGE_MULTIPLE * INITIAL_CASH}) == -0.5
    )
    assert portfolio.total_percent_change({"TEST": 0.0}) == -1


def test_position_buy(position: Position) -> None:
    assert position.average_cost_basis == INITIAL_COST
    assert position.cost_basis_total == (INITIAL_COST * INITIAL_QUANTITY)
    position.buy(CHANGE_MULTIPLE * INITIAL_COST, INITIAL_QUANTITY)
    assert position.average_cost_basis == (
        INITIAL_COST - INITIAL_COST * (CHANGE_MULTIPLE / 2)
    )
    assert position.cost_basis_total == (
        (1 + CHANGE_MULTIPLE) * INITIAL_COST * INITIAL_QUANTITY
    )


def test_position_sell(position: Position) -> None:
    assert position.average_cost_basis == INITIAL_COST
    assert position.cost_basis_total == (INITIAL_COST * INITIAL_QUANTITY)
    position.sell(
        CHANGE_MULTIPLE * INITIAL_COST,
        CHANGE_MULTIPLE * INITIAL_QUANTITY,
    )
    assert position.average_cost_basis == INITIAL_COST
    assert position.cost_basis_total == (
        (1 - CHANGE_MULTIPLE) * INITIAL_COST * INITIAL_QUANTITY
    )


def test_position_total_dollar_change(position: Position) -> None:
    assert position.average_cost_basis == INITIAL_COST
    assert position.cost_basis_total == (INITIAL_COST * INITIAL_QUANTITY)
    assert (
        position.total_dollar_change(CHANGE_MULTIPLE * INITIAL_COST)
        == -CHANGE_MULTIPLE * position.average_cost_basis * position.quantity
    )


def test_position_total_percent_change(position: Position) -> None:
    assert position.average_cost_basis == INITIAL_COST
    assert position.cost_basis_total == (INITIAL_COST * INITIAL_QUANTITY)
    assert (
        position.total_percent_change(CHANGE_MULTIPLE * INITIAL_COST)
        == -CHANGE_MULTIPLE
    )
