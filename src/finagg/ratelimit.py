"""Customizable rate-limiting for requests-style getters.

The definitions within this submodule are used throughout ``finagg`` for
respecting 3rd party API rate limits to avoid server-side throttling.

"""

import time
from abc import ABC, abstractmethod
from collections import deque
from datetime import timedelta
from functools import update_wrapper
from typing import Callable, Generic, ParamSpec, Sequence

import requests

_P = ParamSpec("_P")


class RateLimit(ABC):
    """Interface for defining a rate limit for an external API getter.

    Args:
        limit: Max limit within ``period`` (e.g., max number of
            requests, errors, size in memory, etc.).
        period: Time interval for evaluating ``limit``.
        buffer: Reduce ``limit`` by this fraction. Adds a bit of
            leeway to ensure ``limit`` is not reached. Useful for
            enforcing response size limits.

    """

    #: Max limit within ``period``.
    limit: float

    #: Time interval for evaluating ``limit``
    period: float

    #: Deque of responses containing limits from ``eval`` and
    #: the timestep they were observed.
    responses: deque[tuple[float, float]]

    #: Running total contributing to ``limit`` within ``period``.
    total_limit: float

    #: Running total time to wait before next valid request.
    total_wait: float

    def __init__(
        self, limit: float, period: float | timedelta, /, *, buffer: float = 0.0
    ) -> None:
        self.limit = limit * (1 - buffer)
        self.period = (
            period.total_seconds() if isinstance(period, timedelta) else period
        )
        self.total_limit = 0.0
        self.total_wait = 0.0
        self.responses = deque()

    @abstractmethod
    def eval(self, response: requests.Response, /) -> float | dict[str, float]:
        """Evaluate a response and determine how much it contributes
        to the max limit imposed by this instance.

        Args:
            response: Request response (possibly cached).

        Returns:
            A number indicating the request/response's contribution
            to the rate limit OR a dictionary containing:

                - "limit": a number indicating the request/response's
                    contribution to the rate limit
                - "wait": time to wait before a new request can be made

        """

    @property
    def ts(self) -> float:
        """Get the most recent response timestamp as a result of calling
        the underlying getter.

        """
        return self.responses[-1][1] if self.responses else time.perf_counter()

    def update(self, response: requests.Response, /) -> float:
        """Update the rate limit's running ``total`` and ``responses``
        collection.

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
            for r in self.responses:
                limit, ts = r
                tmp_limit -= limit
                tmp_wait += self.period - (self.ts - ts)
                if tmp_limit < self.limit:
                    break

        self.total_wait = max(self.total_wait, tmp_wait)
        return self.total_wait


class RequestLimit(RateLimit):
    """Limit the number of requests made by the underlying getter."""

    def eval(self, response: requests.Response, /) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        return float(1)


class ErrorLimit(RateLimit):
    """Limit the number of errors occurred when using the underlying getter."""

    def eval(self, response: requests.Response, /) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        return float(response.status_code != 200)


class SizeLimit(RateLimit):
    """Limit the size of responses when using the underlying getter."""

    def eval(self, response: requests.Response, /) -> float | dict[str, float]:
        if hasattr(response, "from_cache") and response.from_cache:
            return 0.0
        return float(len(response.content))


class RateLimitGuard(Generic[_P]):
    """Wraps requests-like getters to introduce blocking functionality
    when requests are getting close to violating call limits.

    .. seealso::
        :meth:`guard`: For the intended usage of getting a
            :class:`finagg.ratelimit.RateLimitGuard` instance.
        :class:`RequestLimit`: For an example of a request
            rate limiter.

    """

    #: ``requests``-like getter that returns a response.
    f: Callable[_P, requests.Response]

    #: Limits to apply to requests/responses.
    limits: tuple[RateLimit, ...]

    #: Whether to print a warning when requests are being temporarily blocked
    #: to respect imposed rate limits.
    warn: bool

    def __init__(
        self,
        f: Callable[_P, requests.Response],
        limits: tuple[RateLimit, ...],
        /,
        *,
        warn: bool = False,
    ) -> None:
        self.f = f
        self.limits = limits
        self.warn = warn
        update_wrapper(self, f)

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> requests.Response:
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


def guard(
    limits: Sequence[RateLimit], /, *, warn: bool = False
) -> Callable[[Callable[_P, requests.Response],], RateLimitGuard[_P]]:
    """Apply ``limits`` to a requests-style getter.

    Args:
        limits: Rate limits to apply to the requests-style getter.
        warn: Whether to print a message when rate-limiting is occurring.

    Returns:
        A decorator that wraps the original requests-style getter in a
        :class:`RateLimitGuard` to avoid exceeding ``limits``.

    Examples:
        Limit 5 requests to Google per second.

        >>> import requests
        >>> @finagg.ratelimit.guard([finagg.ratelimit.RequestLimit(5, timedelta(seconds=1))])
        ... def get() -> requests.Response:
        ...     return requests.get("https://google.com")

    """

    def decorator(f: Callable[_P, requests.Response], /) -> RateLimitGuard[_P]:
        return RateLimitGuard(f, tuple(limits), warn=warn)

    return decorator
