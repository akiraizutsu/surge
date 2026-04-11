"""Shared pytest fixtures for Surge v2 tests."""

import pytest


@pytest.fixture(autouse=True)
def reset_scheduler_state():
    """Clear scheduler_service._last_fired between tests.

    scheduler_service uses a module-level dict to dedup "already fired today"
    slots. Tests that touch _should_fire / _mark_fired need a clean slate
    to stay deterministic. This fixture runs before every test; if
    scheduler_service has not been imported yet it is a no-op.
    """
    yield
    import sys
    sched = sys.modules.get("scheduler_service")
    if sched is not None and hasattr(sched, "_last_fired"):
        sched._last_fired.clear()
