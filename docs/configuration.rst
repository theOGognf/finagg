Configuration
=============

API Keys and User Agents
------------------------

API keys and user agent declarations are required for most of the APIs.
You can set environment variables to expose your API keys and user agents
to **finagg**, or you can pass your API keys and user agents to the implemented
APIs programmatically. The following environment variables are used for
configuring API keys and user agents:

* ``BEA_API_KEY`` is for the Bureau of Economic Analysis's API key. You can get
  a free API key from the `BEA API site`_.
* ``FRED_API_KEY`` is for the Federal Reserve Economic Data API key. You can get
  a free API key from the `FRED API site`_.
* ``SEC_API_USER_AGENT`` is for the Securities and Exchange Commission's API. This
  should be of the format ``FIRST_NAME LAST_NAME E_MAIL``.

The ``finagg install`` CLI will point you where to get free API keys for each
of the APIs that require one and write those API keys to a local ``.env`` file
for storage. See the :doc:`installation docs <installation>` and
:doc:`CLI docs <cli>` for more installation CLI details.

Data Locations
--------------

**finagg**'s root path, HTTP cache path, and database path are all configurable
through environment variables. By default, all data related to **finagg** is put
in a ``./findata`` directory relative to a root directory. You can change these
locations by modifying the respective environment variables:

* ``FINAGG_ROOT_PATH`` points to the parent directory of the ``./findata`` directory.
  Defaults to your current working directory.
* ``FINAGG_HTTP_CACHE_PATH`` points to the HTTP requests cache SQLite storage.
  Defaults to ``./findata/http_cache.sqlite``.
* ``FINAGG_DATABASE_URL`` points to the **finagg** data storage. Defaults to
  ``./findata/finagg.sqlite``.

Other
-----

You can change some **finagg** behavior with other environment variables:

* ``FINAGG_DISABLE_HTTP_CACHE``: Set this to ``"1"`` or ``"True"`` to disable the
  HTTP requests cache. Instead of a cachable session, a default, uncached user
  session will be used for all requests.

.. _`BEA API site`: https://apps.bea.gov/API/signup/
.. _`FRED API site`: https://fredaccount.stlouisfed.org/login/secure/
