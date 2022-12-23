"""Main datbase initialization/installation script."""

from . import fred, indices, sec, yfinance


def run(install_features: bool = False) -> None:
    """Run all installation scripts for submodules."""
    fred.install.run(install_features=install_features)
    indices.install.run()
    sec.install.run(install_features=install_features)
    yfinance.install.run(install_features=install_features)
