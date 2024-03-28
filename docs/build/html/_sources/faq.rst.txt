Frequently Asked Questions
==========================

Where should I start?
---------------------

Aggregate some data, create some analysis notebooks, or create some RL
environments using the implemented data features and SQL tables. This
project was originally created to make RL environments for financial
applications but has since focused its purpose to just aggregating financial
data and features. That being said, all the implemented features are
defined in such a way to make it very easy to develop financial AI/ML,
so we encourage you to do just that!

Why aren't features being installed for a specific ticker or economic data series?
----------------------------------------------------------------------------------

Implemented APIs may be relatively new and simply may not provide data for a
particular ticker or economic data series. For example, earnings per share may
not be accessible for all companies through the SEC EDGAR API. In some cases,
APIs may raise an HTTP error, causing installations to skip the ticker or
series. Additionally, not all tickers and economic data series contain
sufficient data for feature normalization. If a ticker or series only has one
data point, that data point could be dropped when computing a feature (such as
percent change), causing no data to be installed.

What Python versions are supported?
-----------------------------------

Python 3.10 and up are supported. We don't plan on supporting lower versions
because 3.10 introduces some nice quality of life updates that are used
throughout the package.

What operating systems are supported?
-------------------------------------

The package is developed and tested on both Linux and Windows, but we recommend
using Linux or WSL in practice. The package performs a good amount of I/O and
interprocess operations that could result in a noticeable performance
degradation on Windows.
