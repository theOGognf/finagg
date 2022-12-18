import pytest

from shark.portfolio import Portfolio, Position

INITIAL_CASH = 1000
INITIAL_COST = 100
INITIAL_QUANTITY = 10
CHANGE_MULTIPLE = 0.5


@pytest.fixture
def portfolio() -> Portfolio:
    return Portfolio(INITIAL_CASH)


@pytest.fixture
def position() -> Position:
    return Position(INITIAL_COST, INITIAL_QUANTITY)


def test_portfolio_buy() -> None:
    ...


def test_portfolio_sell() -> None:
    ...


def test_portfolio_total_dollar_change() -> None:
    ...


def test_portfolio_total_percent_change() -> None:
    ...


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
