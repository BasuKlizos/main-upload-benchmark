from asyncio import Lock
from datetime import datetime
from typing import Any, Callable

from backend.utils import get_current_time_utc


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(self, max_failures: int = 3, reset_timeout: int = 300):
        self.max_failures: int = max_failures
        self.reset_timeout: int = reset_timeout
        self.failures: int = 0
        self.last_failure_time: datetime | None = None
        self.open: bool = False
        self._lock = Lock()

    async def __call__(self, func: Callable, *args: tuple, **kwargs: dict) -> Any:
        async with self._lock:
            now = get_current_time_utc()
            if self.open and self.last_failure_time and (now - self.last_failure_time).total_seconds() < self.reset_timeout:
                raise CircuitBreakerOpenError("Circuit is open")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self.failures = 0
                self.open = False
            return result

        except Exception as e:
            async with self._lock:
                self.failures += 1
                self.last_failure_time = get_current_time_utc()
                if self.failures >= self.max_failures:
                    self.open = True
            raise

    def __str__(self) -> str:
        return f"<CircuitBreaker open={self.open} failures={self.failures}>"

    def __bool__(self) -> bool:
        return not self.open
