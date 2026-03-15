from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Final

from app.core.logging import get_logger
from app.core.metrics import record_provider_circuit_breaker_transition


CIRCUIT_STATE_CLOSED: Final[str] = "closed"
CIRCUIT_STATE_OPEN: Final[str] = "open"
CIRCUIT_STATE_HALF_OPEN: Final[str] = "half_open"


class CircuitBreakerOpenError(RuntimeError):
    def __init__(self, provider: str, retry_after_seconds: float) -> None:
        super().__init__(f"Circuit breaker is open for provider '{provider}'.")
        self.provider = provider
        self.retry_after_seconds = max(0.0, retry_after_seconds)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    initial_backoff_seconds: float
    total_timeout_seconds: float


class AsyncCircuitBreaker:
    def __init__(
        self,
        *,
        provider: str,
        fail_max: int,
        reset_timeout_seconds: int,
        half_open_sample_size: int,
        success_threshold: float,
    ) -> None:
        self.provider = provider
        self.fail_max = fail_max
        self.reset_timeout_seconds = reset_timeout_seconds
        self.half_open_sample_size = half_open_sample_size
        self.success_threshold = success_threshold
        self.state = CIRCUIT_STATE_CLOSED
        self.opened_at: float | None = None
        self.consecutive_failures = 0
        self.half_open_attempts = 0
        self.half_open_successes = 0
        self._lock = asyncio.Lock()
        self.logger = get_logger(f"app.resilience.{provider}")

    async def before_call(self) -> None:
        async with self._lock:
            if self.state == CIRCUIT_STATE_OPEN:
                elapsed = 0.0 if self.opened_at is None else time.time() - self.opened_at
                if elapsed >= self.reset_timeout_seconds:
                    self._transition_to(CIRCUIT_STATE_HALF_OPEN)
                    return
                raise CircuitBreakerOpenError(
                    provider=self.provider,
                    retry_after_seconds=self.reset_timeout_seconds - elapsed,
                )

    async def record_success(self) -> None:
        async with self._lock:
            if self.state == CIRCUIT_STATE_HALF_OPEN:
                self.half_open_attempts += 1
                self.half_open_successes += 1
                self._maybe_finish_half_open_window()
                return

            self.consecutive_failures = 0
            if self.state != CIRCUIT_STATE_CLOSED:
                self._transition_to(CIRCUIT_STATE_CLOSED)

    async def record_failure(self, *, reason: str | None = None) -> None:
        async with self._lock:
            if self.state == CIRCUIT_STATE_HALF_OPEN:
                self.half_open_attempts += 1
                self._maybe_finish_half_open_window(force_open=True, reason=reason)
                return

            self.consecutive_failures += 1
            if self.consecutive_failures >= self.fail_max:
                self._transition_to(CIRCUIT_STATE_OPEN, reason=reason)

    def _maybe_finish_half_open_window(
        self,
        *,
        force_open: bool = False,
        reason: str | None = None,
    ) -> None:
        failures = self.half_open_attempts - self.half_open_successes
        allowed_failures = max(0, int(self.half_open_sample_size * (1 - self.success_threshold)))

        if force_open or failures > allowed_failures:
            self._transition_to(CIRCUIT_STATE_OPEN, reason=reason)
            return

        if self.half_open_attempts < self.half_open_sample_size:
            return

        success_ratio = 0.0
        if self.half_open_attempts:
            success_ratio = self.half_open_successes / self.half_open_attempts

        if success_ratio >= self.success_threshold:
            self._transition_to(CIRCUIT_STATE_CLOSED)
        else:
            self._transition_to(CIRCUIT_STATE_OPEN, reason=reason)

    def _transition_to(self, new_state: str, *, reason: str | None = None) -> None:
        previous_state = self.state
        if previous_state == new_state:
            return

        self.state = new_state
        if new_state == CIRCUIT_STATE_OPEN:
            self.opened_at = time.time()
        else:
            self.opened_at = None

        if new_state == CIRCUIT_STATE_CLOSED:
            self.consecutive_failures = 0

        if new_state != CIRCUIT_STATE_HALF_OPEN:
            self.half_open_attempts = 0
            self.half_open_successes = 0

        self.logger.warning(
            "provider_circuit_breaker_state_changed",
            extra={
                "provider": self.provider,
                "previous_state": previous_state,
                "new_state": new_state,
                "reason": reason or "state_transition",
            },
        )
        record_provider_circuit_breaker_transition(self.provider, previous_state, new_state)
