from unittest.mock import patch

import pytest
import requests

import finagg.ratelimit

LIMIT = 3
PERIOD = 10


class StrictLimiter(finagg.ratelimit.RateLimit):
    def eval(self, response: requests.Response) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        wait = 100 if response.status_code != 200 else 0.0
        return {"limit": 1.0, "wait": wait}


@pytest.fixture
def expected_wait() -> list[int]:
    """Mock `time.perf_counter` and yield the expected test results.

    The first side effect goes to an internal dummy call.
    The response deques for each limiter before and after responses
    are filtered (assuming a limit of 3 and period of 10):

    Response appended       Responses filtered
        [0]               -> [0]
        [0, 1]            -> [0, 1]
        [0, 1, 10]        -> [1, 10]
        [1, 10, 11]       -> [10, 11]
        [10, 11, 12]      -> [10, 11, 12]  we must wait 8 seconds
        [10, 11, 12, 20]  -> [11, 12, 20]  we must wait 1 second
        [11, 12, 20, 21]  -> [12, 20, 21]  we must wait 1 second
        [12, 20, 21, 22]  -> [20, 21, 22]  we must wait 8 seconds

    """
    with patch("time.perf_counter") as perf_counter:
        perf_counter.side_effect = [0, 0, 1, 10, 11, 12, 20, 21, 22]
        yield [0, 0, 0, 0, 8, 1, 1, 8]


def test_request_limit_update(expected_wait: list[int]) -> None:
    limit = finagg.ratelimit.RequestLimit(LIMIT, PERIOD)
    response = requests.Response()
    for wait in expected_wait:
        assert limit._update(response) == wait


@patch("time.perf_counter", side_effect=[0, 0, 1, 2])
def test_request_limit_update_with_wait(_) -> None:
    limit = StrictLimiter(LIMIT, PERIOD)
    ok_response = requests.Response()
    ok_response.status_code = 200
    bad_response = requests.Response()
    bad_response.status_code = 404
    assert limit._update(ok_response) == 0.0
    assert limit._update(bad_response) == 100.0
    assert limit._update(ok_response) == 99.0


def test_error_limit_update(expected_wait: list[int]) -> None:
    limit = finagg.ratelimit.ErrorLimit(LIMIT, PERIOD)
    response = requests.Response()
    response.status_code = 404
    for wait in expected_wait:
        assert limit._update(response) == wait


def test_size_limit_update(expected_wait: list[int]) -> None:
    limit = finagg.ratelimit.SizeLimit(LIMIT, PERIOD)
    response = requests.Response()
    response._content = b"0"
    for wait in expected_wait:
        assert limit._update(response) == wait
