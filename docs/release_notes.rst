Release Notes
=============

1.0.2
-----

Bug Fixes
^^^^^^^^^

N/A

Compatibility Notes
^^^^^^^^^^^^^^^^^^^

N/A

New Features
^^^^^^^^^^^^

- Added ``FINAGG_DISABLE_HTTP_CACHE`` environment variable for disabling
  the HTTP requests cache.

1.0.1
-----

Bug Fixes
^^^^^^^^^

- Fix wait time calculation for rate limiting.

Compatibility Notes
^^^^^^^^^^^^^^^^^^^

N/A

New Features
^^^^^^^^^^^^

N/A

1.0.0
-----

Bug Fixes
^^^^^^^^^

- Fix filtering original SEC filings.

Compatibility Notes
^^^^^^^^^^^^^^^^^^^

- Bump Pandas major version dependency from 1.0 to 2.0.
- Rename ``finagg.sec.api.company_concept.join_get`` to ``finagg.sec.api.company_concept.get_multiple_original``.
- Rename ``finagg.sec.api.get_financial_ratios`` to ``finagg.sec.api.compute_financial_ratios``.
- Rename ``finagg.sec.api.get_unique_filings`` to ``finagg.sec.api.filter_original_filings``.
- Rename ``finagg.sec.api.join_filings`` to ``finagg.sec.api.group_and_pivot_filings``.
- Rename ``finagg.sec.feat.tags.join_from_raw`` to ``finagg.sec.feat.tags.group_and_pivot_from_raw``.

New Features
^^^^^^^^^^^^

- Added ``paginate`` flag to FRED API methods for automatic pagination of results.
