Release Notes
=============

1.0.0
-----

Bug Fixes
^^^^^^^^^

- Major bugfix for filtering original SEC filings affecting many SEC API and feature implementations.

Compatibility Notes
^^^^^^^^^^^^^^^^^^^

- Bump Pandas major version dependency from 1.0 to 2.0.
- Rename ``finagg.sec.api.company_concept.join_get`` to ``finagg.sec.api.company_concept.get_multiple_original`` for clarity.
- Rename ``finagg.sec.api.get_financial_ratios`` to ``finagg.sec.api.compute_financial_ratios`` for clarity.
- Rename ``finagg.sec.api.get_unique_filings`` to ``finagg.sec.api.filter_original_filings`` for clarity.
- Rename ``finagg.sec.api.join_filings`` to ``finagg.sec.api.group_and_pivot_filings`` for clarity.
- Rename ``finagg.sec.feat.tags.join_from_raw`` to ``finagg.sec.feat.tags.group_and_pivot_from_raw`` for clarity.

New Features
^^^^^^^^^^^^

- Added ``paginate`` option to FRED API methods to enable automatic pagination of results.
