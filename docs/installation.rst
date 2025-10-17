Installation
============

Installing from PyPI
--------------------

Install with pip for the latest stable version.

.. code:: console

  pip install finagg

Installing from GitHub
----------------------

Install from GitHub for the latest unstable version.

.. code:: console

    git clone https://github.com/theOGognf/finagg.git
    pip install ./finagg/

Installing Datasets
-------------------

Optionally install the recommended datasets (economic data, company
financials, stock histories, etc.) from 3rd party APIs into a local SQL
database.

.. code:: console

    finagg install -ss economic -ts sec -z -r

The installation will point you where to get free API keys for each API that
requires one and will write those API keys to a local ``.env`` file for storage.
``finagg install`` is effectively an alias for installing the recommended
datasets from each 3rd party API individually. ``finagg install`` is equivalent
to:

.. code:: console

    finagg bea install
    finagg fred install -a ...
    finagg sec install -a ...

Installation will enable offline usage of aggregated and refined financial
features without internet access or API keys (the recommended way to explore
data uninterrupted). See the :doc:`CLI docs <cli>` for more **finagg** CLI
details.
