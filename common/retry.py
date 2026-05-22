"""Retry helper with exponential backoff."""
from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

T = TypeVar("T")
log = logging.getLogger(__name__)


def with_retries(fn: Callable[[], T], attempts: int = 3, delay: float = 60.0, backoff: float = 1.0) -> T:
    """Call fn() up to `attempts` times. Raises the last exception on failure."""
    last_exc: BaseException | None = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last_exc = e
            log.warning(f"Attempt {i}/{attempts} failed: {e}")
            if i < attempts:
                sleep_for = delay * (backoff ** (i - 1))
                log.info(f"Retrying in {sleep_for:.1f}s...")
                time.sleep(sleep_for)
    assert last_exc is not None
    raise last_exc
