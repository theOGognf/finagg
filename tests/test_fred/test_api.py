import finagg


def test_category_children() -> None:
    finagg.fred.api.category.children.get(0)


def test_category_series() -> None:
    finagg.fred.api.category.series.get(10)


def test_releases_dates() -> None:
    finagg.fred.api.releases.dates.get()


def test_series() -> None:
    finagg.fred.api.series.get("GDP")


def test_series_search() -> None:
    finagg.fred.api.series.search.get("unemployment")


def test_sources() -> None:
    finagg.fred.api.sources.get()
