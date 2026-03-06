"""Token bucket rate limiter for controlling request rates.

Provides per-source rate limiting with configurable tokens-per-second.
Thread-safe implementation using threading locks.
"""

import time
import threading
import logging

logger = logging.getLogger(__name__)


class TokenBucket:
    """Token bucket rate limiter.

    Allows bursting up to `capacity` requests, then enforces
    a sustained rate of `tokens_per_second`.
    """

    def __init__(self, tokens_per_second, capacity=1):
        self.tokens_per_second = tokens_per_second
        self.capacity = capacity
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.tokens_per_second
        self._tokens = min(self.capacity, self._tokens + new_tokens)
        self._last_refill = now

    def acquire(self, blocking=True):
        """Acquire a token, blocking until one is available."""
        with self._lock:
            self._refill()

            if self._tokens >= 1:
                self._tokens -= 1
                return True

            if not blocking:
                return False

            wait_time = (1 - self._tokens) / self.tokens_per_second
            logger.debug(f'Rate limiter waiting {wait_time:.2f}s for token')

        time.sleep(wait_time)

        with self._lock:
            self._refill()
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False


class RateLimiterRegistry:
    """Registry of rate limiters for different sources."""

    def __init__(self):
        self._limiters = {}
        self._lock = threading.Lock()

    def get_limiter(self, source, rate=None):
        """Get or create a rate limiter for a given source."""
        with self._lock:
            if source not in self._limiters:
                if rate is None:
                    raise ValueError(
                        f'Rate must be specified when creating limiter for "{source}"'
                    )
                tokens_per_second = 1.0 / rate
                self._limiters[source] = TokenBucket(tokens_per_second)
                logger.debug(
                    f'Created rate limiter for "{source}": '
                    f'{tokens_per_second:.2f} tokens/s'
                )
            return self._limiters[source]

    def acquire(self, source):
        """Acquire a token for a given source."""
        limiter = self._limiters.get(source)
        if limiter is None:
            raise ValueError(f'No rate limiter registered for "{source}"')
        return limiter.acquire()


# Global registry instance
registry = RateLimiterRegistry()
