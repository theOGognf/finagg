def scrape(series_id: str | list[str], /) -> dict[str, int]:
    if isinstance(series_id, str):
        series_id = [series_id]
