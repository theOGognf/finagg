"""An implementation of the Federal Reserve Economic Data (FRED) API.

The FRED API provides methods for retrieving, searching, and describing
economic data from a variety of sources. The FRED API is one of the most
popular APIs in the financial industry.

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

from .category_ import Category
from .release_ import Release, Releases
from .series_ import Series, popular_series
from .source_ import Source, Sources
from .tags_ import RelatedTags, Tags

category = Category()
"""The most popular way for accessing the :class:`~finagg.fred.api.category_.Category`.

:meta hide-value:
"""

releases = Releases()
"""The most popular way for accessing :class:`~finagg.fred.api.release_.Releases`.

:meta hide-value:
"""

release = Release()
"""The most popular way for accessing :class:`~finagg.fred.api.release_.Release`.

:meta hide-value:
"""
series = Series()
"""The most popular way for accessing :class:`~finagg.fred.api.series_.Series`.

:meta hide-value:
"""

source = Source()
"""The most popular way for accessing :class:`~finagg.fred.api.source_.Source`.

:meta hide-value:
"""

sources = Sources()
"""The most popular way for accessing :class:`~finagg.fred.api.source_.Sources`.

:meta hide-value:
"""

tags = Tags()
"""The most popular way for accessing :class:`~finagg.fred.api.tags_.Tags`.

:meta hide-value:
"""

related_tags = RelatedTags()
"""The most popular way for accessing :class:`~finagg.fred.api.tags_.RelatedTags`.

:meta hide-value:
"""
