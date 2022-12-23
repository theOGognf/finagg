"""Customizable rate-limiting for requests-style getters."""

import time
from abc import ABC, abstractmethod
from collections import deque
from datetime import timedelta
from typing import Any, Callable, Sequence

import requests
import requests_cache

Response = requests.Response | requests_cache.CachedResponse
Getter = Callable[
    [
        Any,
    ],
    Response,
]


class RateLimit(ABC):
    """Interface for defining a rate limit with an external API.

    Args:
        limit: Max limit within `period` (e.g., max number of
            requests, errors, size in memory, etc.).
        period: Time interval for evaluating `limit`.
        buffer: Reduce `limit` by this fraction. Adds a bit of
            leeway to ensure `limit` is not reached. Useful for
            enforcing response size limits.

    """

    #: Max limit within `period`.
    limit: float

    #: Time interval for evaluating `limit`
    period: float

    #: Deque of responses containing limits from `eval` and
    #: the timestep they were observed.
    responses: deque[tuple[float, float]]

    #: Running total contributing to `limit` within `period`.
    total_limit: float

    #: Running total time to wait before next valid request.
    total_wait: float

    def __init__(
        self, limit: float, period: float | timedelta, *, buffer: float = 0.0
    ) -> None:
        self.limit = limit * (1 - buffer)
        self.period = (
            period.total_seconds() if isinstance(period, timedelta) else period
        )
        self.total_limit = 0.0
        self.total_wait = 0.0
        self.responses = deque()

    @abstractmethod
    def eval(self, response: Response) -> float | dict[str, float]:
        """Evaluate a response and determine how much it contributes
        to the max limit imposed by this instance.

        Args:
            response: Request response (possibly cached).

        Returns:
            A number indicating the request/response's contribution
            to the rate limit

            OR

            a dictionary containing:
                "limit": A number indicating the request/response's
                    contribution to the rate limit.
                "wait": Time to wait before a new request can be
                    made.

        """

    @property
    def ts(self) -> float:
        """Get the most recent response timestamp."""
        return self.responses[-1][1] if self.responses else time.perf_counter()

    def update(self, response: Response) -> float:
        """Update the rate limit's running `total` and `responses` collection.

        Args:
            response: Request response (possibly cached).

        Returns:
            Estimated time to wait to avoid being throttled.

        """
        # Get limit value and wait time from response.
        v = self.eval(response)
        if not isinstance(v, dict):
            v = {"limit": v}
        if "wait" not in v:
            v["wait"] = 0.0
        limit = v["limit"]
        wait = v["wait"]

        ts = time.perf_counter()
        dt = max(ts - self.ts, 0.0)
        self.total_limit += limit
        self.total_wait = max(self.total_wait - dt, 0.0)
        self.total_wait = max(self.total_wait, wait)
        new = (limit, ts)
        self.responses.append(new)

        # Remove timed-out responses and remove their contributions
        # to the total limit.
        while self.responses and (
            (self.responses[-1][1] - self.responses[0][1]) >= self.period
        ):
            old = self.responses.popleft()
            self.total_limit -= old[0]

        # Update total wait time according to the max limit.
        tmp_limit = self.total_limit
        tmp_wait = 0.0
        if self.total_limit >= self.limit:
            for response in self.responses:
                limit, ts = response
                tmp_limit -= limit
                tmp_wait += self.period - (self.ts - ts)
                if tmp_limit < self.limit:
                    break

        self.total_wait = max(self.total_wait, tmp_wait)
        return self.total_wait


class RequestLimit(RateLimit):
    """Limit the number of requests made."""

    def eval(self, response: Response) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        return float(1)


class ErrorLimit(RateLimit):
    """Limit the number of errors occurred."""

    def eval(self, response: Response) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        return float(response.status_code != 200)


class SizeLimit(RateLimit):
    """Limit the size of responses."""

    def eval(self, response: Response) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        return float(len(response.content))


class RateLimitGuard:
    """Wraps requests-like getters to introduce blocking functionality
    when requests are getting close to violating call limits.

    """

    #: requests-like getter that returns a response.
    f: Getter

    #: Limits to apply to requests/responses.
    limits: tuple[RateLimit, ...]

    #: Whether to only warn about guarding once.
    warn: bool

    def __init__(
        self,
        f: Getter,
        limits: tuple[RateLimit, ...],
        /,
        *,
        warn: bool = False,
    ) -> None:
        self.f = f
        self.limits = limits
        self.warn = warn

    def __call__(self, *args: Any, **kwargs: Any) -> Response:
        """Call the underlying getter and sleep the wait required to
        satisfy the guard's limits.

        Args:
            *args: Args passed to the underlying getter.
            **kwargs: Kwargs passed to the underlying getter.

        Returns:
            The received response.

        """
        r = self.f(*args, **kwargs)
        wait = 0.0
        for limit in self.limits:
            tmp_wait = limit.update(r)
            wait = max(wait, tmp_wait)
        if wait > 0:
            if self.warn:
                print(f"Throttling requests to {r.url} for {wait:.2f} (s)", flush=True)
            time.sleep(wait)
        return r


def guard(limits: Sequence[RateLimit], *, warn: bool = False):
    """Apply `limits` to a requests-style getter."""

    def decorator(f: Getter) -> RateLimitGuard:
        return RateLimitGuard(f, limits, warn=warn)

    return decorator
