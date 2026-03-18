"""Unit tests for AsyncCircuitBreaker state machine in app/core/resilience.py."""

from __future__ import annotations

import asyncio
import time

import pytest

from app.core.resilience import (
    CIRCUIT_STATE_CLOSED,
    CIRCUIT_STATE_HALF_OPEN,
    CIRCUIT_STATE_OPEN,
    AsyncCircuitBreaker,
    CircuitBreakerOpenError,
)


def _breaker(
    *,
    fail_max: int = 3,
    reset_timeout_seconds: int = 30,
    half_open_sample_size: int = 4,
    success_threshold: float = 0.75,
) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(
        provider="test_provider",
        fail_max=fail_max,
        reset_timeout_seconds=reset_timeout_seconds,
        half_open_sample_size=half_open_sample_size,
        success_threshold=success_threshold,
    )


# ---------------------------------------------------------------------------
# Closed state — normal operation
# ---------------------------------------------------------------------------


def test_initial_state_is_closed() -> None:
    cb = _breaker()
    assert cb.state == CIRCUIT_STATE_CLOSED


def test_before_call_passes_when_closed() -> None:
    async def scenario() -> None:
        cb = _breaker()
        await cb.before_call()  # must not raise

    asyncio.run(scenario())


def test_record_success_in_closed_resets_failure_counter() -> None:
    async def scenario() -> None:
        cb = _breaker(fail_max=3)
        await cb.record_failure()
        await cb.record_failure()
        assert cb.consecutive_failures == 2
        await cb.record_success()
        assert cb.consecutive_failures == 0
        assert cb.state == CIRCUIT_STATE_CLOSED

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Closed → Open transition
# ---------------------------------------------------------------------------


def test_breaker_opens_after_fail_max_consecutive_failures() -> None:
    async def scenario() -> None:
        cb = _breaker(fail_max=3)
        for _ in range(3):
            await cb.record_failure()
        assert cb.state == CIRCUIT_STATE_OPEN

    asyncio.run(scenario())


def test_breaker_does_not_open_before_fail_max() -> None:
    async def scenario() -> None:
        cb = _breaker(fail_max=3)
        await cb.record_failure()
        await cb.record_failure()
        assert cb.state == CIRCUIT_STATE_CLOSED

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Open state — rejects calls
# ---------------------------------------------------------------------------


def test_before_call_raises_when_open() -> None:
    async def scenario() -> None:
        cb = _breaker(fail_max=1, reset_timeout_seconds=60)
        await cb.record_failure()
        assert cb.state == CIRCUIT_STATE_OPEN
        with pytest.raises(CircuitBreakerOpenError) as exc_info:
            await cb.before_call()
        assert exc_info.value.provider == "test_provider"
        assert exc_info.value.retry_after_seconds > 0

    asyncio.run(scenario())


def test_circuit_breaker_open_error_retry_after_is_non_negative() -> None:
    err = CircuitBreakerOpenError(provider="p", retry_after_seconds=-5.0)
    assert err.retry_after_seconds == 0.0


# ---------------------------------------------------------------------------
# Open → Half-open transition (reset timeout elapsed)
# ---------------------------------------------------------------------------


def test_before_call_transitions_to_half_open_after_timeout() -> None:
    async def scenario() -> None:
        cb = _breaker(fail_max=1, reset_timeout_seconds=1)
        await cb.record_failure()
        assert cb.state == CIRCUIT_STATE_OPEN

        # Force opened_at to be old enough
        cb.opened_at = time.time() - 2  # 2 s ago > 1 s timeout

        await cb.before_call()  # must not raise; transitions to half-open
        assert cb.state == CIRCUIT_STATE_HALF_OPEN

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Half-open state — success path → Closed
# ---------------------------------------------------------------------------


def test_half_open_closes_after_all_successes_in_window() -> None:
    async def scenario() -> None:
        cb = _breaker(
            fail_max=1,
            reset_timeout_seconds=1,
            half_open_sample_size=4,
            success_threshold=0.75,
        )
        await cb.record_failure()
        cb.opened_at = time.time() - 2
        await cb.before_call()  # → half-open
        assert cb.state == CIRCUIT_STATE_HALF_OPEN

        # Any failure in half-open triggers force_open=True → immediate re-open.
        # To close, all sample_size probes must succeed.
        for _ in range(4):
            await cb.record_success()

        assert cb.state == CIRCUIT_STATE_CLOSED

    asyncio.run(scenario())


def test_half_open_record_success_increments_counters() -> None:
    async def scenario() -> None:
        cb = _breaker(
            fail_max=1,
            reset_timeout_seconds=1,
            half_open_sample_size=10,
            success_threshold=0.5,
        )
        await cb.record_failure()
        cb.opened_at = time.time() - 2
        await cb.before_call()  # → half-open

        await cb.record_success()
        assert cb.half_open_successes == 1
        assert cb.half_open_attempts == 1
        assert cb.state == CIRCUIT_STATE_HALF_OPEN  # window not complete yet

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Half-open state — failure path → Open
# ---------------------------------------------------------------------------


def test_half_open_reopens_on_failure() -> None:
    async def scenario() -> None:
        cb = _breaker(
            fail_max=1,
            reset_timeout_seconds=1,
            half_open_sample_size=4,
            success_threshold=1.0,  # needs 100% success
        )
        await cb.record_failure()
        cb.opened_at = time.time() - 2
        await cb.before_call()  # → half-open

        await cb.record_failure(reason="probe_failed")  # immediately re-opens
        assert cb.state == CIRCUIT_STATE_OPEN

    asyncio.run(scenario())


def test_half_open_reopens_when_success_ratio_below_threshold() -> None:
    async def scenario() -> None:
        cb = _breaker(
            fail_max=1,
            reset_timeout_seconds=1,
            half_open_sample_size=4,
            success_threshold=0.75,
        )
        await cb.record_failure()
        cb.opened_at = time.time() - 2
        await cb.before_call()  # → half-open

        # 1 success + 3 failures = 25% < 75% threshold
        await cb.record_success()
        await cb.record_failure()
        await cb.record_failure()
        await cb.record_failure()  # window complete with poor ratio

        assert cb.state == CIRCUIT_STATE_OPEN

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# No-op self-transition guard
# ---------------------------------------------------------------------------


def test_no_op_transition_does_not_log_or_change_state() -> None:
    """Transitioning to the same state must be a no-op (line 117)."""
    async def scenario() -> None:
        cb = _breaker()
        # Manually trigger a self-transition by calling record_success
        # when already closed (consecutive_failures == 0, state remains closed).
        await cb.record_success()
        assert cb.state == CIRCUIT_STATE_CLOSED
        assert cb.consecutive_failures == 0

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# opened_at cleared when leaving Open
# ---------------------------------------------------------------------------


def test_opened_at_is_cleared_when_transitioning_out_of_open() -> None:
    async def scenario() -> None:
        cb = _breaker(
            fail_max=1,
            reset_timeout_seconds=1,
            half_open_sample_size=1,
            success_threshold=1.0,
        )
        await cb.record_failure()
        assert cb.opened_at is not None

        cb.opened_at = time.time() - 2
        await cb.before_call()  # → half-open
        # opened_at should be cleared on transition away from OPEN
        assert cb.opened_at is None

        await cb.record_success()  # window = 1, 100% → closed
        assert cb.state == CIRCUIT_STATE_CLOSED

    asyncio.run(scenario())
