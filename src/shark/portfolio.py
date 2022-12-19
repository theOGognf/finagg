"""Definitions related to tracking an investment portfolio of cash and stocks."""

from functools import total_ordering
from typing import Generic, TypeVar


@total_ordering
class Position:
    """A position in holding a security.

    Args:
        cost: Initial purchase cost.
        quantity: Shares held at `cost`.

    """

    #: Average dollar cost for each share in the position.
    average_cost_basis: float

    #: Total dollar cost for all shares in the position.
    #: The amount of dollars or cash invested in this security.
    cost_basis_total: float

    #: Current number of shares owned in the position.
    quantity: float

    def __init__(self, cost: float, quantity: float) -> None:
        self.cost_basis_total = cost * quantity
        self.average_cost_basis = cost
        self.quantity = quantity

    def __eq__(self, __o: object) -> bool:
        """Compare the position's cost basis."""
        if not isinstance(__o, float | Position):
            raise NotImplementedError(
                "Can only compare " f"{self.__class__.__name__} to [float, Position]"
            )

        if isinstance(__o, Position):
            return self.average_cost_basis == __o.average_cost_basis

        return self.average_cost_basis == __o

    def __lt__(self, __o: object) -> bool:
        """Compare the position's cost basis."""
        if not isinstance(__o, float | Position):
            raise NotImplementedError(
                "Can only compare "
                f"{self.__class__.__name__} to [{float.__name__}, {Position.__name__}]"
            )

        if isinstance(__o, Position):
            return self.average_cost_basis < __o.average_cost_basis

        return self.average_cost_basis < __o

    def buy(self, cost: float, quantity: float) -> float:
        """Buy `quantity` of the position for `cost`.

        Args:
            cost: Cost to buy at.
            quantity: Number of shares to buy.

        Returns:
            Value of the bought position.

        """
        self.quantity += quantity
        self.cost_basis_total += cost * quantity
        self.average_cost_basis = self.cost_basis_total / self.quantity
        return cost * quantity

    def sell(self, cost: float, quantity: float) -> float:
        """Sell `quantity` of the position for `cost`.

        Args:
            cost: Cost to sell at.
            quantity: Number of shares to sell.

        Returns:
            Value of the sold position.

        Raises:
            ValueError if there aren't enough shares
            to sell in the position.

        """
        if self.quantity < quantity:
            raise ValueError("Invalid order - not enough shares")
        self.quantity -= quantity
        self.cost_basis_total = self.average_cost_basis * self.quantity
        return cost * quantity

    def total_dollar_change(self, cost: float) -> float:
        """Compute the total dollar change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total dollar change in value.

        """
        return (cost - self.average_cost_basis) * self.quantity

    def total_percent_change(self, cost: float) -> float:
        """Compute the total percent change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        """
        return (cost / self.average_cost_basis) - 1


_Symbol = TypeVar("_Symbol", bound=str)
_Position = TypeVar("_Position", bound=Position)


class Portfolio(Generic[_Symbol, _Position]):
    """A collection of cash and security positions.

    Args:
        cash: Starting cash position.

    """

    #: Total liquid cash on-hand.
    cash: float

    #: Total cash deposited since starting the portfolio.
    deposits_total: float

    #: Existing positions for each security.
    positions: dict[str, Position]

    #: Total cash withdrawn since starting the portfolio.
    withdrawals_total: float

    def __init__(self, cash: float) -> None:
        self.cash = cash
        self.deposits_total = cash
        self.withdrawals_total = 0
        self.positions = {}

    def __contains__(self, symbol: str) -> bool:
        """Return whether the portfolio contains a position
        in `symbol`.

        """
        return symbol in self.positions

    def __getitem__(self, symbol: str) -> Position:
        """Return the portfolio's position in the security
        identified by `symbol`.

        """
        return self.positions[symbol]

    def buy(self, symbol: str, cost: float, quantity: float) -> float:
        """Buy `quantity` of security with `symbol` for `cost`.

        Args:
            symbol: Security ticker.
            cost: Cost to buy the symbol at.
            quantity: Number of shares to purchase.

        Returns:
            Value of the symbol's bought position in the
            portfolio.

        Raises:
            ValueError if the portfolio doesn't have enough cash
            to execute the buy order.

        """
        current_value = cost * quantity
        if self.cash < current_value:
            raise ValueError("Invalid order - not enough cash")
        self.cash -= current_value
        if symbol not in self.positions:
            self.positions[symbol] = Position(cost, quantity)
            return current_value
        else:
            return self.positions[symbol].buy(cost, quantity)

    def deposit(self, cash: float) -> float:
        """Deposit more cash into the portfolio.

        Args:
            cash: Cash to deposit.

        Returns:
            Total cash in the portfolio.

        """
        self.cash += cash
        self.deposits_total += cash
        return self.cash

    def sell(self, symbol: str, cost: float, quantity: float) -> float:
        """Sell `quantity` of security with `symbol` for `cost`.

        Args:
            symbol: Security ticker.
            cost: Cost to sell the symbol at.
            quantity: Number of shares to sell.

        Returns:
            Value of the symbol's sold position in the
            portfolio.

        """
        current_value = self.positions[symbol].sell(cost, quantity)
        if not self.positions[symbol].quantity:
            self.positions.pop(symbol)
        self.cash += cost * quantity
        return current_value

    def total_dollar_change(self, costs: dict[str, float]) -> float:
        """Compute the total dollar change relative to the total
        deposits made into the portfolio.

        Args:
            costs: Mapping of symbol to its current value of one share.

        Returns:
            Total dollar change in value.

        """
        return self.total_dollar_value(costs) - self.deposits_total

    def total_dollar_value(self, costs: dict[str, float]) -> float:
        """Compute the total dollar value of the portfolio.

        Args:
            costs: Mapping of symbol to its current value of one share.

        Returns:
            Total dollar value.

        """
        dollar_value_total = self.cash
        for symbol, cost in costs.items():
            if symbol in self.positions:
                dollar_value_total += cost * self.positions[symbol].quantity
        return dollar_value_total

    def total_percent_change(self, costs: dict[str, float]) -> float:
        """Compute the total percent change relative to the total
        deposits made into the portfolio.

        Args:
            costs: Mapping of symbol to its current value of one share.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        """
        return (self.total_dollar_value(costs) / self.deposits_total) - 1

    def withdraw(self, cash: float) -> float:
        """Withdraw cash from the portfolio.

        Args:
            cash: Cash to withdraw.

        Returns:
            Total cash in the portfolio.

        Raises:
            ValueError if there's not enough cash to withdraw.

        """
        if self.cash < cash:
            raise ValueError("Not enough cash to withdraw")
        self.cash -= cash
        self.withdrawals_total += cash
        return self.cash
