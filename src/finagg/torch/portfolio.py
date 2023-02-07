"""Definitions related to tracking an investment portfolio of cash and stocks."""

from functools import total_ordering

import torch


@total_ordering
class Position:
    """A position in holding a security.

    Args:
        cost: Initial purchase cost.
        quantity: Shares held at `cost`.

    """

    #: Average dollar cost for each share in the position.
    average_cost_basis: torch.Tensor

    #: Total dollar cost for all shares in the position.
    #: The amount of dollars or cash invested in this security.
    cost_basis_total: torch.Tensor

    #: Current number of shares owned in the position.
    quantity: torch.Tensor

    def __init__(self, cost: torch.Tensor, quantity: torch.Tensor) -> None:
        self.cost_basis_total = cost * quantity
        self.average_cost_basis = cost
        self.quantity = quantity

    def __eq__(self, __o: object) -> torch.Tensor:  # type: ignore[override]
        """Compare the position's cost basis."""
        if not isinstance(__o, torch.Tensor | Position):
            raise NotImplementedError(
                "Can only compare "
                f"{self.__class__.__name__} to [{torch.Tensor.__name__}, {Position.__name__}]"
            )

        if isinstance(__o, Position):
            return self.average_cost_basis == __o.average_cost_basis

        return self.average_cost_basis == __o

    def __lt__(self, __o: object) -> torch.Tensor:
        """Compare the position's cost basis."""
        if not isinstance(__o, torch.Tensor | Position):
            raise NotImplementedError(
                "Can only compare "
                f"{self.__class__.__name__} to [{torch.Tensor.__name__}, {Position.__name__}]"
            )

        if isinstance(__o, Position):
            return self.average_cost_basis < __o.average_cost_basis

        return self.average_cost_basis < __o

    def buy(self, cost: torch.Tensor, quantity: torch.Tensor) -> torch.Tensor:
        """Buy `quantity` of the position for `cost`.

        Args:
            cost: Cost to buy at.
            quantity: Number of shares to buy.

        Returns:
            Value of the bought position.

        """
        self.quantity = self.quantity + quantity
        self.cost_basis_total = self.cost_basis_total + cost * quantity
        self.average_cost_basis = self.cost_basis_total / self.quantity
        return cost * quantity

    def sell(self, cost: torch.Tensor, quantity: torch.Tensor) -> torch.Tensor:
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
        if torch.any(self.quantity < quantity):
            raise ValueError("Invalid order - not enough shares.")
        self.quantity = self.quantity - quantity
        self.cost_basis_total = self.average_cost_basis * self.quantity
        return cost * quantity

    def total_dollar_change(self, cost: torch.Tensor) -> torch.Tensor:
        """Compute the total dollar change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total dollar change in value.

        """
        return (cost - self.average_cost_basis) * self.quantity

    def total_percent_change(self, cost: torch.Tensor) -> torch.Tensor:
        """Compute the total percent change relative to the average
        cost basis and the current value of the security.

        Args:
            cost: Current value of one share.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        """
        return (cost / self.average_cost_basis) - 1


class Portfolio:
    """A collection of cash and security positions.

    Args:
        cash: Starting cash position.

    """

    #: Total liquid cash on-hand.
    cash: torch.Tensor

    #: Total cash deposited since starting the portfolio.
    deposits_total: torch.Tensor

    #: Existing positions for each security.
    position: Position

    #: Total cash withdrawn since starting the portfolio.
    withdrawals_total: torch.Tensor

    def __init__(self, cash: torch.Tensor) -> None:
        self.cash = cash
        self.deposits_total = cash
        self.withdrawals_total = torch.zeros_like(cash)
        self.position = Position(torch.zeros_like(cash), torch.zeros_like(cash))

    def buy(self, cost: torch.Tensor, quantity: torch.Tensor) -> torch.Tensor:
        """Buy `quantity` of security for `cost`.

        Args:
            cost: Cost to buy at.
            quantity: Number of shares to purchase.

        Returns:
            Value of the additional position in the portfolio.

        Raises:
            ValueError if the portfolio doesn't have enough cash
            to execute the buy order.

        """
        current_value = cost * quantity
        if torch.any(self.cash < current_value):
            raise ValueError("Invalid order - not enough cash.")
        self.cash = self.cash - current_value
        return self.position.buy(cost, quantity)

    def deposit(self, cash: torch.Tensor) -> torch.Tensor:
        """Deposit more cash into the portfolio.

        Args:
            cash: Cash to deposit.

        Returns:
            Total cash in the portfolio.

        """
        self.cash = self.cash + cash
        self.deposits_total = self.deposits_total + cash
        return self.cash

    def sell(self, cost: torch.Tensor, quantity: torch.Tensor) -> torch.Tensor:
        """Sell `quantity` of security for `cost`.

        Args:
            cost: Cost to sell at.
            quantity: Number of shares to sell.

        Returns:
            Value of the portfolio's sold position.

        """
        current_value = self.position.sell(cost, quantity)
        self.cash = self.cash + cost * quantity
        return current_value

    def total_dollar_change(self, cost: torch.Tensor) -> torch.Tensor:
        """Compute the total dollar change relative to the total
        deposits made into the portfolio.

        Args:
            cost: Current security cost.

        Returns:
            Total dollar change in value.

        """
        return self.total_dollar_value(cost) - self.deposits_total

    def total_dollar_value(self, cost: torch.Tensor) -> torch.Tensor:
        """Compute the total dollar value of the portfolio.

        Args:
            cost: Current security cost.

        Returns:
            Total dollar value.

        """
        dollar_value_total = self.cash
        dollar_value_total = dollar_value_total + cost * self.position.quantity
        return dollar_value_total

    def total_percent_change(self, cost: torch.Tensor) -> torch.Tensor:
        """Compute the total percent change relative to the total
        deposits made into the portfolio.

        Args:
            cost: Current security cost.

        Returns:
            Total percent change in value. Negative indicates loss
            in value, positive indicates gain in value.

        """
        return (self.total_dollar_value(cost) / self.deposits_total) - 1

    def withdraw(self, cash: torch.Tensor) -> torch.Tensor:
        """Withdraw cash from the portfolio.

        Args:
            cash: Cash to withdraw.

        Returns:
            Total cash in the portfolio.

        Raises:
            ValueError if there's not enough cash to withdraw.

        """
        if torch.any(self.cash < cash):
            raise ValueError("Not enough cash to withdraw.")
        self.cash = self.cash - cash
        self.withdrawals_total = self.withdrawals_total + cash
        return self.cash
