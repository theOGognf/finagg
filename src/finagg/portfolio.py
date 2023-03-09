"""Definitions related to tracking an investment portfolio of cash and stocks.
Underlying arithmetic uses exact decimal representations for max precision.

"""

from decimal import Decimal
from functools import total_ordering


@total_ordering
class Position:
    """A position in holding a security.

    Args:
        cost: Initial purchase cost.
        quantity: Number of shares held at ``cost``.

    """

    # Average dollar cost for each share in the position.
    _average_cost_basis: Decimal

    # Total dollar cost for all shares in the position.
    # The amount of dollars or cash invested in this security.
    _cost_basis_total: Decimal

    # Current number of shares owned in the position.
    _quantity: Decimal

    def __init__(self, cost: float, quantity: float, /) -> None:
        self._average_cost_basis = Decimal(cost)
        self._quantity = Decimal(quantity)
        self._cost_basis_total = self._average_cost_basis * self._quantity

    def __eq__(self, __o: object) -> bool:
        """Compare the position's cost basis."""
        if not isinstance(__o, float | Position):
            raise TypeError(
                "Can only compare "
                f"{self.__class__.__name__} to [{float.__name__}, {Position.__name__}]"
            )

        if isinstance(__o, Position):
            return self._average_cost_basis == __o._average_cost_basis

        return self._average_cost_basis == __o

    def __lt__(self, __o: object) -> bool:
        """Compare the position's cost basis."""
        if not isinstance(__o, float | Position):
            raise TypeError(
                "Can only compare "
                f"{self.__class__.__name__} to [{float.__name__}, {Position.__name__}]"
            )

        if isinstance(__o, Position):
            return self._average_cost_basis < __o._average_cost_basis

        return self._average_cost_basis < __o

    @property
    def average_cost_basis(self) -> float:
        """Average dollar cost for each share in the position."""
        return float(self._average_cost_basis)

    @property
    def cost_basis_total(self) -> float:
        """Total dollar cost for all shares in the position. The amount of
        dollars or cash invested in this security.

        """
        return float(self._cost_basis_total)

    def buy(self, cost: float, quantity: float, /) -> float:
        """Buy ``quantity`` of the position for ``cost``.

        Args:
            cost: Cost to buy at.
            quantity: Number of shares to buy.

        Returns:
            Value of the bought position.

        Examples:
            >>> from finagg.portfolio import Position
            >>> pos = Position(100.0, 1)
            >>> pos.buy(50.0, 1)
            50.0
            >>> pos.cost_basis_total
            150.0
            >>> pos.average_cost_basis
            75.0
            >>> pos.quantity
            2.0

        """
        exact_cost = Decimal(cost)
        exact_quantity = Decimal(quantity)
        self._quantity += exact_quantity
        self._cost_basis_total = self._cost_basis_total + exact_cost * exact_quantity
        self._average_cost_basis = self._cost_basis_total / self._quantity
        return float(exact_cost * exact_quantity)

    @property
    def quantity(self) -> float:
        """Current number of shares owned in the position."""
        return float(self._quantity)

    def sell(self, cost: float, quantity: float, /) -> float:
        """Sell ``quantity`` of the position for ``cost``.

        Args:
            cost: Cost to sell at.
            quantity: Number of shares to sell.

        Returns:
            Value of the sold position.

        Raises:
            `ValueError`: If there aren't enough shares
                to sell in the position.

        Examples:
            >>> from finagg.portfolio import Position
            >>> pos = Position(100.0, 2)
            >>> pos.sell(50.0, 1)
            50.0
            >>> pos.cost_basis_total
            100.0
            >>> pos.average_cost_basis
            100.0
            >>> pos.quantity
            1.0

        """
        exact_cost = Decimal(cost)
        exact_quantity = Decimal(quantity)
        if self._quantity < exact_quantity:
            raise ValueError("Invalid order - not enough shares.")
        self._quantity -= exact_quantity
        self._cost_basis_total = self._average_cost_basis * self._quantity
        return float(exact_cost * exact_quantity)

    def total_dollar_change(self, cost: float, /) -> float:
        """Compute the total dollar change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total dollar change in value.

        Examples:
            >>> from finagg.portfolio import Position
            >>> pos = Position(100.0, 1)
            >>> pos.total_dollar_change(50.0)
            -50.0

        """
        return float((Decimal(cost) - self._average_cost_basis) * self._quantity)

    def total_percent_change(self, cost: float, /) -> float:
        """Compute the total percent change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        Examples:
            >>> from finagg.portfolio import Position
            >>> pos = Position(100.0, 1)
            >>> pos.total_percent_change(50.0)
            -0.5

        """
        return float((Decimal(cost) / self._average_cost_basis) - 1)


class Portfolio:
    """A collection of cash and security positions.

    Args:
        cash: Starting cash position.

    """

    # Total liquid cash on-hand.
    _cash: Decimal

    # Total cash deposited since starting the portfolio.
    _deposits_total: Decimal

    #: Existing positions for each security.
    positions: dict[str, Position]

    # Total cash withdrawn since starting the portfolio.
    _withdrawals_total: Decimal

    def __init__(self, cash: float, /) -> None:
        self._cash = Decimal(cash)
        self._deposits_total = self._cash
        self._withdrawals_total = Decimal(0)
        self.positions = {}

    def __contains__(self, symbol: str) -> bool:
        """Return whether the portfolio contains a position
        in ``symbol``.

        """
        return symbol in self.positions

    def __getitem__(self, symbol: str) -> Position:
        """Return the portfolio's position in the security
        identified by `symbol`.

        """
        return self.positions[symbol]

    @property
    def cash(self) -> float:
        """Total liquid cash on-hand."""
        return float(self._cash)

    def buy(self, symbol: str, cost: float, quantity: float, /) -> float:
        """Buy ``quantity`` of security with ``symbol`` for ``cost``.

        Args:
            symbol: Security ticker.
            cost: Cost to buy the symbol at.
            quantity: Number of shares to purchase.

        Returns:
            Value of the symbol's bought position in the
            portfolio.

        Raises:
            `ValueError`: If the portfolio doesn't have enough cash
                to execute the buy order.

        Examples:
            >>> from finagg.portfolio import Portfolio
            >>> port = Portfolio(1000.0)
            >>> port.buy("AAPL", 100.0, 1)
            100.0
            >>> pos = port["AAPL"]
            >>> pos.cost_basis_total
            100.0
            >>> pos.average_cost_basis
            100.0
            >>> pos.quantity
            1.0

        """
        current_value = Decimal(cost) * Decimal(quantity)
        if self._cash < current_value:
            raise ValueError("Invalid order - not enough cash.")
        self._cash -= current_value
        if symbol not in self.positions:
            self.positions[symbol] = Position(cost, quantity)
            return float(current_value)
        else:
            return self.positions[symbol].buy(cost, quantity)

    def deposit(self, cash: float, /) -> float:
        """Deposit more cash into the portfolio.

        Args:
            cash: Cash to deposit.

        Returns:
            Total cash in the portfolio.

        """
        exact_cash = Decimal(cash)
        self._cash += exact_cash
        self._deposits_total += exact_cash
        return float(self._cash)

    @property
    def deposits_total(self) -> float:
        """Total cash deposited since starting the portfolio."""
        return float(self._deposits_total)

    def sell(self, symbol: str, cost: float, quantity: float, /) -> float:
        """Sell ``quantity`` of security with `symbol` for ``cost``.

        Args:
            symbol: Security ticker.
            cost: Cost to sell the symbol at.
            quantity: Number of shares to sell.

        Returns:
            Value of the symbol's sold position in the
            portfolio.

        Examples:
            >>> from finagg.portfolio import Portfolio
            >>> port = Portfolio(1000.0)
            >>> port.buy("AAPL", 100.0, 2)
            200.0
            >>> port.sell("AAPL", 50.0, 1)
            50.0
            >>> pos = port["AAPL"]
            >>> pos.cost_basis_total
            100.0
            >>> pos.average_cost_basis
            100.0
            >>> pos.quantity
            1.0

        """
        current_value = self.positions[symbol].sell(cost, quantity)
        if not self.positions[symbol]._quantity:
            self.positions.pop(symbol)
        self._cash += Decimal(cost) * Decimal(quantity)
        return float(current_value)

    def total_dollar_change(self, costs: dict[str, float], /) -> float:
        """Compute the total dollar change relative to the total
        deposits made into the portfolio.

        Args:
            costs: Mapping of symbol to its current value of one share.

        Returns:
            Total dollar change in value.

        Examples:
            >>> from finagg.portfolio import Portfolio
            >>> port = Portfolio(1000.0)
            >>> port.buy("AAPL", 100.0, 1)
            100.0
            >>> port.total_dollar_change({"AAPL": 50.0})
            -50.0

        """
        return float(Decimal(self.total_dollar_value(costs)) - self._deposits_total)

    def total_dollar_value(self, costs: dict[str, float], /) -> float:
        """Compute the total dollar value of the portfolio.

        Args:
            costs: Mapping of symbol to its current value of one share.

        Returns:
            Total dollar value.

        Examples:
            >>> from finagg.portfolio import Portfolio
            >>> port = Portfolio(1000.0)
            >>> port.buy("AAPL", 100.0, 1)
            100.0
            >>> port.total_dollar_value({"AAPL": 50.0})
            950.0

        """
        dollar_value_total = self._cash
        for symbol, cost in costs.items():
            if symbol in self.positions:
                dollar_value_total += Decimal(cost) * self.positions[symbol]._quantity
        return float(dollar_value_total)

    def total_percent_change(self, costs: dict[str, float], /) -> float:
        """Compute the total percent change relative to the total
        deposits made into the portfolio.

        Args:
            costs: Mapping of symbol to its current value of one share.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        Examples:
            >>> from finagg.portfolio import Portfolio
            >>> port = Portfolio(1000.0)
            >>> port.buy("AAPL", 100.0, 1)
            100.0
            >>> port.total_percent_change({"AAPL": 50.0})
            -0.05

        """
        return float(
            (Decimal(self.total_dollar_value(costs)) / self._deposits_total) - 1
        )

    def withdraw(self, cash: float, /) -> float:
        """Withdraw cash from the portfolio.

        Args:
            cash: Cash to withdraw.

        Returns:
            Total cash in the portfolio.

        Raises:
            `ValueError`: If the portfolio doesn't have at least ``cash``
                liquid cash to withdraw.

        Examples:
            >>> from finagg.portfolio import Portfolio
            >>> port = Portfolio(1000.0)
            >>> port.withdraw(100.0)
            900.0

        """
        exact_cash = Decimal(cash)
        if self._cash < exact_cash:
            raise ValueError("Not enough cash to withdraw.")
        self._cash -= exact_cash
        self._withdrawals_total += exact_cash
        return float(self._cash)

    @property
    def withdraws_total(self) -> float:
        """Total cash withdrawn since starting the portfolio."""
        return float(self._withdrawals_total)
