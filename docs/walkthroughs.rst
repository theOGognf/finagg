Walkthroughs
============

This page is a collection of walkthroughs that distinguish the usages of
API subpackages vs feature subpackages, and a collection of workflows for
aggregating data for a subset of companies, industries, and economic data
series (depending on the subpackage being used).

Although these walkthroughs will provide you with enough information to use
**finagg** to aggregate data from the implemented APIs, it's recommended
you still explore the respective official API documentations to get a good
understanding of how each API is organized and what data is provided through
each API.

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
terms/words with :attr:`finagg.fred.api.Series.search` (the FRED series
search API implementation):

>>> finagg.fred.api.series.search.get(
...   "treasury yield",
...   order_by="popularity"
... ).head(5)  # doctest: +SKIP
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
(implemented by :attr:`finagg.fred.api.Series.observations`):

>>> finagg.fred.api.series.observations.get(
...     "DGS10",
...     output_type=1
... ).head(5)  # doctest: +SKIP
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
:meth:`finagg.fred.feat.Series.get_id_set` method.

>>> finagg.fred.feat.series.install({"GS10"})  # doctest: +SKIP
>>> id_set = finagg.fred.feat.series.get_id_set()
>>> "GS10" in id_set
True

We can then retrieve the original, raw economic data series we installed using
the :meth:`finagg.fred.feat.Series.from_raw` method.

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
:meth:`finagg.fred.feat.Economic.install` method doesn't allow
specification of economic data series IDs.
:meth:`finagg.fred.feat.Economic.install` will, by default, only
install a handful of economic data series.

It's important to note that once an economic data series is supported by
:data:`finagg.fred.feat.economic`, it will never be removed. However,
additional economic data series may be added as a default depending on popularity.

This restriction comes with the benefit of simplifying our download-then-retrieve
workflow. Repeating the download-then-retrieve workflow we used for the treasury
FRED economic data series but with :data:`finagg.fred.feat.economic` looks
like the following:

>>> finagg.fred.feat.economic.install()  # doctest: +SKIP
>>> finagg.fred.feat.economic.from_refined()["FEDFUNDS"].head(5)  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
date
2014-10-06    0.09
2014-10-08    0.09
2014-10-13    0.09
2014-10-15    0.09
2014-10-20    0.09
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

>>> finagg.fred.feat.series.install()  # doctest: +SKIP
>>> finagg.fred.feat.economic.install()  # doctest: +SKIP

is equivalent to:

.. code:: console

    finagg fred install --raw series --refined economic -ss economic

Using the SEC API Subpackages
-----------------------------

Let's say we're interested in a specific company. The SEC EDGAR API is a good
place to start accessing a company's financials. However, not all companies
have all their financial data accessible through the SEC EDGAR API. The best
way to start out and see what financials are available for a particular
company is to look at a company's facts through
:data:`finagg.sec.api.company_facts`.

Let's assume we're interested in Microsoft. We can access all the financial
publications associated with Microsoft by simply passing Microsoft's ticker,
MSFT, to the company facts API implementation. We can look at the columns
to get a good understanding of the API implementation and the returned
dataframe:

>>> df = finagg.sec.api.company_facts.get(ticker="MSFT")
>>> df.columns.tolist()  # doctest: +ELLIPSIS, +NORMALIZE_WHITESPACE
['end', 'value', ..., 'fy', 'fp', 'form', 'filed', ..., 'tag', ..., 'units', ...]

The main columns that most use cases care about are:

* ``fy``, ``fp``, and ``filed``; these are the fiscal year, fiscal period
  (i.e., quarter), and filing date, respectively for each row of financial data
* ``form`` is the type of SEC form the row was submitted with (e.g., 10-Q,
  10-K, etc.)
* ``tag`` is the ID of the financial (e.g., ``"EarningsPerShareBasic"``)
* ``value`` is the financial's actual value
* ``units`` is the financial's unit (e.g., USD/shares)

The company's financials can be further filtered from the company facts
dataframe directly, or a specific financial can be accessed with the
:data:`finagg.sec.api.company_concept` API implementation. For example,
we can access all of Microsoft's earnings per share financial publications
with the following:

>>> df = finagg.sec.api.company_concept.get(
...     "EarningsPerShareBasic",
...     ticker="MSFT",
...     units="USD/shares"
... )
>>> df.head(5)  # doctest: +SKIP
        start         end  value                  accn    fy  fp  form       filed ...
0  2007-07-01  2007-09-30   0.46  0001193125-10-171791  2010  FY  10-K  2010-07-30 ...
1  2007-10-01  2007-12-31   0.50  0001193125-10-171791  2010  FY  10-K  2010-07-30 ...
2  2008-01-01  2008-03-31   0.47  0001193125-10-171791  2010  FY  10-K  2010-07-30 ...
3  2007-07-01  2008-06-30   1.90  0001193125-10-171791  2010  FY  10-K  2010-07-30 ...
4  2008-04-01  2008-06-30   0.46  0001193125-10-171791  2010  FY  10-K  2010-07-30 ...

However, the SEC EDGAR company concept API implementation returns all the
earnings per share publications for Microsft, including amendments. We may
not necessarily care about amendments because we may be building strategies
or models that use *current* data and not *future* data. Fortunately, **finagg**
provides :meth:`finagg.sec.api.get_unique_filings` to further select
original financial publication data from specific forms:

>>> finagg.sec.api.get_unique_filings(df, form="10-Q").head(5)  # doctest: +SKIP
     fy  fp                    tag       start         end  value ...
0  2010  Q1  EarningsPerShareBasic  2008-07-01  2008-09-30   0.48 ...
1  2010  Q2  EarningsPerShareBasic  2008-07-01  2008-12-31   0.95 ...
2  2010  Q3  EarningsPerShareBasic  2008-07-01  2009-03-31   1.29 ...
3  2011  Q1  EarningsPerShareBasic  2009-07-01  2009-09-30   0.40 ...
4  2011  Q2  EarningsPerShareBasic  2009-07-01  2009-12-31   1.15 ...

Unfortunately, the SEC EDGAR API is still relatively new and a lot of the
financial data publications are unaudited, so not all financials are available
for all companies through the SEC EDGAR API. I.e., a workflow for retrieving
Microsoft's financial data may not work for retrieving another company's
financial data. It requires some trial-and-error to find a set of tags that
are popular and available for the majority of companies. In addition, the
workflow for exploring these tags and filtering forms can be cumbersome.

Fortunately again, **finagg** exists for a reason besides implementing these
useful APIs. **finagg** provides additional conveniences that makes these
common workflows even easier.

First, **finagg** provides :data:`finagg.sec.api.popular_concepts` for listing
company concepts (combinations of financial data tags and other parameters)
that're popular and widely available for companies. Second, it's extremely
straightforward to filter and install widely popular and available quarterly
financial data for a set of companies using the :mod:`finagg.sec.feat`
subpackage and :data:`finagg.sec.feat.quarterly` member. The
:data:`finagg.sec.feat.quarterly` member also goes a step further by somewhat
normalizing the installed financial data (e.g., total asset value is converted
to percent change of total asset value on a quarter-over-quarter basis), making
the process for aggregating company financial data and comparing company
financial data painless.

We can give this streamlined process a try with Microsoft again, and we can
verify Microsoft's financial data is successfully installed using the
:meth:`finagg.sec.feat.Quarterly.get_ticker_set` method.

>>> finagg.sec.feat.quarterly.install({"MSFT"})  # doctest: +SKIP
>>> ticker_set = finagg.sec.feat.quarterly.get_ticker_set()
>>> "MSFT" in ticker_set
True

We can then retrieve Microsoft's quarterly financial data using the
:meth:`finagg.sec.feat.Quarterly.from_refined` method.

>>> finagg.sec.feat.quarterly.from_refined("MSFT").head(5)  # doctest: +SKIP
                    LOG_CHANGE(Assets)  LOG_CHANGE(AssetsCurrent) ...
fy   fp filed                                                     ...
2010 Q1 2010-01-25            0.182629                  -0.023676 ...
     Q2 2010-04-21            0.000000                   0.000000 ...
     Q3 2010-07-21            0.000000                   0.000000 ...
2011 Q1 2011-01-19            0.459174                   0.278241 ...
     Q2 2011-04-21            0.000000                   0.000000 ...

On top of this simplification, :data:`finagg.sec.feat.quarterly` provides
another method and convenience for normalizing quarterly financial data.
:attr:`finagg.sec.feat.Quarterly.normalized` normalizes
quarterly financial data using quarterly financial data from all the other
companies within the target company's industry. For example, Lowe's'
financial data would be used to normalize Home Depot's financial data such
that all columns have zero mean and unit variance.
:attr:`finagg.sec.feat.Quarterly.normalized` also has
similar workflow to :data:`finagg.sec.feat.quarterly`.

>>> finagg.sec.feat.quarterly.normalized.install({"MSFT"})  # doctest: +SKIP
>>> finagg.sec.feat.quarterly.normalized.from_refined("MSFT").head(5)  # doctest: +SKIP
                    NORM(LOG_CHANGE(Assets))  NORM(LOG_CHANGE(AssetsCurrent)) ...
fy   fp filed                                                                 ...
2010 Q2 2010-04-21                  0.000000                         0.000000 ...
     Q3 2010-07-21                  0.000000                         0.000000 ...
2011 Q1 2011-01-19                  0.978816                         0.074032 ...
     Q2 2011-04-21                  0.000000                         0.000000 ...
     Q3 2011-07-20                 -0.353553                        -0.353553 ...

Lastly, it's useful to mention that any download/installation step in the
common download-then-retrieve workflow for financial data with **finagg**'s
Python interface can probably be replicated using **finagg**'s CLI. For
example, the following:

>>> finagg.sec.feat.quarterly.install({"MSFT"})  # doctest: +SKIP

is equivalent to:

.. code:: console

    finagg sec install --raw submissions --raw tags --refined quarterly -t MSFT

.. _`FRED API docs have good explanations and examples`: https://fred.stlouisfe ...
