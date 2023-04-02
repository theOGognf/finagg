Walkthroughs
============

This page is a collection of walkthroughs that distinguish the usages of
API subpackages vs feature subpackages, and a collection of workflows for
aggregating data for a subset of companies, industries, and economic data
series (depending on the subpackage being used).

These walkthroughs assume your environment contains the necessary environment
variables to enable using the respective APIs. See the
:doc:`configuration docs <configuration>` for more info on configuring your
environment.

Using the FRED API Subpackages
------------------------------

Let's say we're interested in finding economic data series corresponding
to various US treasuries. The FRED series search API is a good place to start.
The FRED series search API allows us to search through economic data series
that're provided by the FRED series observations API (the main API used for
retrieving economic data). We can search for economic data series using search
terms/words with :data:`finagg.fred.api.series.search` (the FRED series
search API implementation):

>>> finagg.fred.api.series.search.get(
...   "treasury yield",
...   order_by="popularity"
... ).head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
             id ...                                             title observation_start ...
0        T10Y2Y ... 10-Year Treasury Constant Maturity Minus 2-Yea...        1976-06-01 ...
1  BAMLH0A0HYM2 ... ICE BofA US High Yield Index Option-Adjusted S...        1996-12-31 ...
2        T10Y3M ... 10-Year Treasury Constant Maturity Minus 3-Mon...        1982-01-04 ...
3         DGS10 ... Market Yield on U.S. Treasury Securities at 10...        1962-01-02 ...
4        DFII10 ... Market Yield on U.S. Treasury Securities at 10...        2003-01-02 ...

Notice we order by popularity using the ``order_by`` arg to help filter through
irrelevant economic data series. The returned dataframe provides
key info for further exploring and aggregating the economic data series:

* the ``id`` column indicates the economic data series ID corresponding to
  each returned series (IDs are args for other FRED series API methods)
* the ``title`` column indicates the series name (useful for verifying the
  series are relevant)
* the ``observation_start`` column indicates the oldest date with data for
  each series (we may want to ignore series that don't have as much history)

Let's assume we're only interested in the economic data series corresponding
to economic series ID ``"DGS10"`` under the ``id`` column. We can get the
economic data series observations through the FRED series observations API
(implemented by :data:`finagg.fred.api.series.observations`):

>>> finagg.fred.api.series.observations.get(
...     "DGS10",
...     output_type=1
... ).head(5)  # doctest: +NORMALIZE_WHITESPACE
  realtime_start realtime_end        date value series_id
0     2023-04-01   2023-04-01  1962-01-02  4.06     DGS10
1     2023-04-01   2023-04-01  1962-01-03  4.03     DGS10
2     2023-04-01   2023-04-01  1962-01-04  3.99     DGS10
3     2023-04-01   2023-04-01  1962-01-05  4.02     DGS10
4     2023-04-01   2023-04-01  1962-01-08  4.03     DGS10

It should be noted some trial-and-error can be required to retrieve data using
the FRED series observations API. Most FRED API methods have ``realtime_start``,
``realtime_end``, and ``output_type`` parameters that control the reporting
period for the returned data. The `FRED API docs have good explanations and examples`_
that clarify the use cases for these parameters. For most FRED API use cases,
these parameters are usually set to ``0``, ``-1``, and ``4``, respectively.
These values (when used together) effectively retrieve economic data series
observations as they first occur for their entire history. That is, the
returned values will be the "truth" values from the perspective of economists
and analysts at the time the values were originally published. This is usally
what most people want since we typically want to build models and/or strategies
based on *currently known* values rather than *future* values.

However, the ``"DGS10"`` economic data series does not support retrieving
initial release observations. To successfully retrieve data for the ``"DGS10"``
series (and most treasury yield series), the ``realtime_start`` and
``realtime_end`` parameters must be today's date (the default) and the
``output_type`` parameter must be ``1`` (also the default).

So now we have an economic data series we're interested in and can retrieve
observation values for. We can download and store (or install) the economic
data series observation values in our own SQL table to reduce the number of
requests to the FRED API (so we don't get throttled), and to get a slight
speed boost when performing offline analysis (it'll be slightly faster to
get data that's on our own machine rather than some server).

Unfortunately, to avoid the complexity of reimplementing the entire FRED API
when retrieving data from local SQL tables, **finagg**'s installation methods
only support economic data series that have initial release observation data
available. Searching for other treasury yield series reveals economic data
series with similar names and values that support initial releases (e.g.,
``"GS10"``); we'll need to use these series in-place of our previously found
series to use **finagg**'s installation methods.

Installing a treasury yield economic data series is extremely straightforward
with the :mod:`finagg.fred.feat` subpackage and :data:`finagg.fred.feat.series`
member. We can also verify the series is installed correctly using the
:meth:`finagg.fred.sql.get_id_set` method.

>>> finagg.fred.feat.series.install({"GS10"})
>>> id_set = finagg.fred.feat.sql.get_id_set()
>>> "GS10" in id_set
True

We can then retrieve the original, raw economic data series we installed using
the :meth:`finagg.fred.feat.series.from_raw` method.

>>> finagg.fred.feat.series.from_raw("GS10").head(5)  # doctest: +NORMALIZE_WHITESPACE
            value
date
1996-12-01   6.30
1997-01-01   6.58
1997-02-01   6.42
1997-03-01   6.69
1997-04-01   6.89

All of these steps aren't exactly obvious when using **finagg** for the first time.
However, **finagg**'s purpose is to streamline popular financial data
aggregation, so obviously there are some shortcuts when it comes to popular
economic data series. That's where :data:`finagg.fred.feat.economic` comes
in for the FRED API subpackage. :data:`finagg.fred.feat.economic` assumes
a fixed set of popular economic series IDs that also support initial release
observations for all its methods. For example, the
:meth:`finagg.fred.feat.economic.install` method doesn't allow specification
of economic data series IDs. :meth:`finagg.fred.feat.economic.install` will,
by default, only install a handful of economic data series.

It's important to note that once an economic data series is supported by
:data:`finagg.fred.feat.economic`, it will never be removed. However,
additional economic data series may be added as a default depending on popularity.

This restriction comes with the benefit of simplifying our download-then-retrieve
workflow. Repeating the download-then-retrieve workflow we used for the treasury
FRED economic data series but with :data:`finagg.fred.feat.economic` looks
like the following:

>>> finagg.fred.feat.economic.install()
>>> "FEDFUNDS" in finagg.fred.feat.economic.get_id_set()
True
>>> finagg.fred.feat.economic.from_refined()["FEDFUNDS"].head(5)  # doctest: +NORMALIZE_WHITESPACE
date
2014-10-06    0.09
2014-10-08    0.09
2014-10-13    0.09
2014-10-15    0.09
2014-10-20    0.09
Name: FEDFUNDS, dtype: float64

On top of this simplification, :data:`finagg.fred.feat.economic` provides
another method and convenience for normalizing the default economic data series.
Economic data series whose scales drift over time (e.g., gross domestic product)
are converted to percent changes while economic data series whose scales are
consistent over time (e.g., unemployment rate) are normalized to be zero mean
and unit variance. The normalized economic data series can also have a similar
workflow to the above using :data:`finagg.fred.feat.economic.normalized`.

>>> finagg.fred.feat.economic.normalized.install()
>>> finagg.fred.economic.normalized.from_refined()["FEDFUNDS"].head(5)  # doctest: +NORMALIZE_WHITESPACE
date
2014-10-06   -0.896754
2014-10-08   -0.896754
2014-10-13   -0.896754
2014-10-15   -0.896754
2014-10-20   -0.896754
Name: FEDFUNDS, dtype: float64

Lastly, it's useful to mention that any download/installation step in the
common download-then-retrieve workflow for raw or refined data
with **finagg**'s Python interface can probably be replicated using
**finagg**'s CLI. For example, the following:

>>> finagg.fred.feat.series.install({"GS10"})  # doctest: +SKIP

is equivalent to:

.. code:: console

    finagg fred install -r series -sid GS10

Similarly, the following:

>>> finagg.fred.feat.series.install()
>>> finagg.fred.feat.economic.install()  # doctest: +SKIP

is equivalent to:

.. code:: console

    finagg fred install -r series -ref economic -ss economic

The installation CLIs just further simplify the process for downloading
data by replacing one-off installation Python scripts.

Using the SEC API Subpackages
-----------------------------

.. _`FRED API docs have good explanations and examples`: https://fred.stlouisfed.org/docs/api/fred/realtime_period.html
