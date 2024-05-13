"""Customizable rate-limiting for requests-style getters.

The definitions within this submodule are used throughout :mod:`finagg` for
respecting 3rd party API rate limits to avoid server-side throttling.

"""

import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from functools import update_wrapper
from typing import Callable, Generic, ParamSpec, Sequence

import requests

_P = ParamSpec("_P")


@dataclass
class _RateLimitData:
    """Convenience container for rate limit data."""

    #: Timestamp associated with the data.
    ts: float

    #: Response's contribution to a rate limit threshold.
    quantity: float


@dataclass
class _RunningTotals:
    """Track running totals associated with exceeding rate limits."""

    #: Total limit contribution of all responses within a window towards
    #: a rate limit threshold.
    quantity: float = 0.0

    #: Max wait time needed until another request can be made without
    #: exceeding a rate limit.
    wait: float = 0.0


class RateLimit(ABC):
    """Interface for defining a rate limit for an external API getter.

    You can create a custom rate-limiter by inheriting from this class
    and implementing a custom :meth:`eval` method.

    Args:
        limit: Max limit within ``period`` (e.g., max number of
            requests, errors, size in memory, etc.).
        period: Time interval for evaluating ``limit``.
        buffer: Reduce ``limit`` by this fraction. Adds a bit of
            leeway to ensure ``limit`` is not reached. Useful for
            enforcing response size limits.

    .. seealso::
        :meth:`guard`: For the intended usage of getting a
            :class:`RateLimitGuard` instance.
        :class:`RequestLimit`: For an example of a request
            rate limiter.

    """

    #: Deque of responses containing limits from ``eval`` and
    #: the timesteps they were observed.
    _rate_limit_datas: deque[_RateLimitData]

    #: Running total contributions to ``limit`` within ``period``
    #: and the total wait time.
    _running_totals: _RunningTotals

    #: Max quantity allowed within ``period``. The quantity type being limited
    #: is dependent on what's returned by :meth:`eval`.
    limit: float

    #: Time interval for evaluating ``limit`` (in seconds).
    period: float

    def __init__(
        self, limit: float, period: float | timedelta, /, *, buffer: float = 0.0
    ) -> None:
        self.limit = limit * (1 - buffer)
        self.period = (
            period.total_seconds() if isinstance(period, timedelta) else period
        )
        self._rate_limit_datas = deque()
        self._running_totals = _RunningTotals()

    @property
    def _ts(self) -> float:
        """Get the most recent response timestamp as a result of calling
        the underlying getter.

        """
        return (
            self._rate_limit_datas[-1].ts
            if self._rate_limit_datas
            else time.perf_counter()
        )

    def _update(self, response: requests.Response, /) -> float:
        """Update the rate limit's running ``total`` and ``responses``
        collection.

        This method calls :meth:`eval` and uses its return value to
        update the client-side throttling wait time (if there is any) to
        avoid exceeding server-side API limits.

        Args:
            response: Request response (possibly cached).

        Returns:
            Estimated time to wait to avoid being throttled.

        """
        # Get the timestamp and timedelta associated with this response.
        ts = time.perf_counter()
        dt = max(ts - self._ts, 0.0)

        # Get the limit contribution value and wait time from the response.
        v = self.eval(response)
        if not isinstance(v, dict):
            v = {"limit": v}
        if "wait" not in v:
            v["wait"] = 0.0
        quantity = v["limit"]
        wait = v["wait"]

        # Create the new rate limit data.
        new = _RateLimitData(ts, quantity)
        self._rate_limit_datas.append(new)

        # Update running totals for estimating wait times.
        self._running_totals.quantity += quantity
        self._running_totals.wait = max(self._running_totals.wait - dt, 0.0)
        self._running_totals.wait = max(self._running_totals.wait, wait)

        # Remove timed-out responses and remove their contributions
        # to the total limit.
        while self._rate_limit_datas and (
            (self._rate_limit_datas[-1].ts - self._rate_limit_datas[0].ts)
            >= self.period
        ):
            old = self._rate_limit_datas.popleft()
            self._running_totals.quantity -= old.quantity

        # Update total wait time according to the max limit.
        tmp_limit = self._running_totals.quantity
        tmp_wait = 0.0
        if self._running_totals.quantity >= self.limit:
            for r in self._rate_limit_datas:
                tmp_wait = self.period - (self._ts - r.ts)
                tmp_limit -= r.quantity
                if tmp_limit < self.limit:
                    break

        self._running_totals.wait = max(self._running_totals.wait, tmp_wait)
        return self._running_totals.wait

    @abstractmethod
    def eval(self, response: requests.Response, /) -> float | dict[str, float]:
        """Evaluate a response and determine how much it contributes
        to the max limit imposed by this instance.

        This is the main method that should be overwritten by subclasses
        to create custom rate-limiters. This method is called with each
        requests's response to determine how much that request/response
        contributes to the rate-limiting.

        Args:
            response: Request response (possibly cached).

        Returns:
            A number indicating the request/response's contribution
            to the rate limit OR a dictionary containing:

                - "limit": a number indicating the request/response's
                  contribution to the rate limit
                - "wait": time to wait before a new request can be made

        """


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

    Args:
        f: Requests-style getter that's wrapped and rate-limited.
        limits: Limits to apply to the requests-style getter.
        warn: Whether to print a message to stdout whenever client-side
            throttling is occurring to respect ``limits``.

    .. seealso::
        :meth:`guard`: For the intended usage of getting a
            :class:`RateLimitGuard` instance.
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
            tmp_wait = limit._update(r)
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
        warn: Whether to print a message when client-side throttling is
            occurring.

    Returns:
        A decorator that wraps the original requests-style getter in a
        :class:`RateLimitGuard` to avoid exceeding ``limits``.

    Examples:
        Limit 5 requests to Google per second.

        >>> import requests
        >>> from datetime import timedelta
        >>> from finagg.ratelimit import RequestLimit, guard
        >>> @guard([RequestLimit(5, timedelta(seconds=1))])
        ... def get() -> requests.Response:
        ...     return requests.get("https://google.com")

    """

    def decorator(f: Callable[_P, requests.Response], /) -> RateLimitGuard[_P]:
        return RateLimitGuard(f, tuple(limits), warn=warn)

    return decorator
