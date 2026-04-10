"""Shared HTTP utility: async request with exponential back-off.

All external API calls in this application go through `request_with_backoff` rather
than calling `httpx.AsyncClient.get/post` directly.  This ensures transient errors
(rate limiting, brief outages) are retried automatically without crashing enrichment.

Back-off strategy:
  - Attempt 0 — immediate
  - Attempt 1..N — delay = min(base * 2^attempt + jitter, max_delay)
  - Jitter (random 0–1 s) prevents thundering-herd on simultaneous retries
  - `Retry-After` header value (when numeric) overrides the computed delay
  - After max_retries exhausted on a retriable status, returns the last response
    (caller decides whether to raise_for_status)
  - Connection/timeout errors are always retried; raises on last attempt
"""

import asyncio
import logging
import random
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

import httpx

logger = logging.getLogger(__name__)

T = TypeVar("T")

# HTTP status codes that warrant a retry (rate-limit and server errors).
_DEFAULT_RETRY_STATUSES = {429, 500, 502, 503, 504}
_BASE_DELAY = 1.0     # seconds — starting delay for exponential back-off
_MAX_DELAY = 60.0     # seconds — ceiling to prevent excessively long waits
_MAX_RETRIES = 5      # total extra attempts after the first failure


async def request_with_backoff(
    call: Callable[[], Coroutine[Any, Any, httpx.Response]],
    retry_statuses: set[int] = _DEFAULT_RETRY_STATUSES,
    max_retries: int = _MAX_RETRIES,
) -> httpx.Response:
    """Execute an async httpx call, retrying on transient HTTP errors and network failures.

    Args:
        call:           Zero-argument async callable that performs the HTTP request
                        and returns an `httpx.Response`.  Use a lambda to capture params:
                        `lambda: client.get(url, params=params)`.
        retry_statuses: Set of HTTP status codes to retry.  Defaults to rate-limit
                        and server-error codes.
        max_retries:    Maximum number of additional attempts after the first failure.
                        Total attempts = max_retries + 1.

    Returns:
        The last `httpx.Response` received.  If the final attempt still returns a
        retriable status code, that response is returned — callers should call
        `response.raise_for_status()` as needed.

    Raises:
        httpx.TimeoutException | httpx.ConnectError: Re-raised on the final attempt.
    """
    for attempt in range(max_retries + 1):
        try:
            response = await call()
            if response.status_code not in retry_statuses:
                return response

            # Honour the Retry-After header if the server sent a numeric value.
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                delay = float(retry_after)
            else:
                delay = min(_BASE_DELAY * (2**attempt) + random.uniform(0, 1), _MAX_DELAY)

            if attempt < max_retries:
                logger.warning(
                    "HTTP %s received, retrying in %.1fs (attempt %d/%d)",
                    response.status_code,
                    delay,
                    attempt + 1,
                    max_retries,
                )
                await asyncio.sleep(delay)
            else:
                # All retries exhausted — return the last response for the caller to handle.
                return response

        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            if attempt == max_retries:
                raise
            delay = min(_BASE_DELAY * (2**attempt) + random.uniform(0, 1), _MAX_DELAY)
            logger.warning(
                "Request error %s, retrying in %.1fs (attempt %d/%d)",
                exc,
                delay,
                attempt + 1,
                max_retries,
            )
            await asyncio.sleep(delay)

    # Unreachable — satisfies the type checker.
    raise RuntimeError("Exhausted retries")
