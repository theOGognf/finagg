"""Definitions related to tracking an investment portfolio of cash and stocks."""

from typing import Generic, TypeVar


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

    def buy(self, cost: float, quantity: float) -> float:
        """Buy `quantity` of the position for `cost`.

        Args:
            cost: Cost to buy at.
            quantity: Number of shares to buy.

        Returns:
            Current total value of the position.

        """
        self.quantity += quantity
        self.cost_basis_total += cost * quantity
        self.average_cost_basis = self.cost_basis_total / self.quantity
        return cost * self.quantity

    def compute_total_percent_change(self, cost: float) -> float:
        """Compute the total percent change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        """
        return (cost / self.average_cost_basis) - 1

    def compute_total_dollar_change(self, cost: float) -> float:
        """Compute the total dollar change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total dollar change in value.

        """
        return (cost - self.average_cost_basis) * self.quantity

    def sell(self, cost: float, quantity: float) -> float:
        """Sell `quantity` of the position for `cost`.

        Args:
            cost: Cost to sell at.
            quantity: Number of shares to sell.

        Returns:
            Current total value of the position.

        Raises:
            ValueError if there aren't enough shares
            to sell in the position.

        """
        if self.quantity < quantity:
            raise ValueError("Invalid order - not enough shares")
        self.quantity -= quantity
        self.cost_basis_total = self.average_cost_basis * self.quantity
        return cost * self.quantity


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
    withdraws_total: float

    def __init__(self, cash: float) -> None:
        self.cash = cash
        self.deposits_total = cash
        self.withdraws_total = 0
        self.positions = {}

    def __getitem__(self, symbol: str) -> Position:
        return self.positions[symbol]

    def buy(self, symbol: str, cost: float, quantity: float) -> float:
        """Buy `quantity` of security with `symbol` for `cost`.

        Args:
            symbol: Security ticker.
            cost: Cost to buy the symbol at.
            quantity: Number of shares to purchase.

        Returns:
            Current total value of the symbol's position in the
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
            Current total value of the symbol's position in the
            portfolio.

        """
        current_value = self.positions[symbol].sell(cost, quantity)
        if not self.positions[symbol].quantity:
            self.positions.pop(symbol)
        self.cash += cost * quantity
        return current_value

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
        self.withdraws_total += cash
        return self.cash
