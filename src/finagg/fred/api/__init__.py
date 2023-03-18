"""An implementation of the Federal Reserve Economic Data (FRED) API.

A FRED API key is required to use this API. You can request a FRED API key
at the `FRED API webpage`_. You can pass your FRED API key directly to the
implemented API getters, or you can set the ``FRED_API_KEY`` environment
variable to have the FRED API key be passed to the implemented API getters
for you.

Alternatively, running ``finagg fred install`` (or the broader
``finagg install``) will prompt you where to acquire a FRED KEY and will
automatically store it in an ``.env`` file in your current working directory.
The environment variables set in that ``.env`` file will be loaded into your
shell upon using ``finagg`` (whether that be through the Python interface or
through the CLI tools).

See the official `FRED API docs`_ for more info on the FRED API.

.. _`FRED API docs`: https://fred.stlouisfed.org/docs/api/fred/
.. _`FRED API webpage`: https://fred.stlouisfed.org/docs/api/api_key.html

"""

from .category_ import category
from .release_ import release, releases
from .series_ import series
from .source_ import source, sources
from .tags_ import related_tags, tags
