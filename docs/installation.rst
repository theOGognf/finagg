Installation
============

Install **finagg** from GitHub directly.

.. code:: console

    git clone https://github.com/theOGognf/finagg.git
    pip install ./finagg/

Optionally install the recommended datasets from 3rd party APIs into a local
SQL database.

.. code:: console

    finagg install -a

The installation will point you where to get free API keys for each API that
requires one and write those API keys to a local ``.env`` file for storage.
``finagg install -a`` is effectively an alias for installing the
recommended datasets from each 3rd party API individually.
``finagg install -a`` is equivalent to:

.. code:: console

    finagg bea install
    finagg fred install -a
    finagg indices install -a
    finagg sec install -a
    finagg yfinance install -a
