"""Tests for scheduler_service._should_fire / _mark_fired / get_schedule_info.

The conftest `reset_scheduler_state` fixture clears the module-level
`_last_fired` dict before every test, so each test sees a clean slate.
"""

from datetime import datetime

import scheduler_service as sched


# US slot (slot_id "us_0600"): Tue-Sat (weekdays {1,2,3,4,5}), 06:00 JST
# JP slot (slot_id "jp_1530"): Mon-Fri (weekdays {0,1,2,3,4}), 15:30 JST


def _find_slot(slot_id):
    for s in sched._SLOTS:
        if s["slot_id"] == slot_id:
            return s
    raise RuntimeError(f"slot {slot_id} not found")


def _jst(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=sched.JST)


# ── _should_fire: weekday gate ────────────────────────────────────────────

def test_should_fire_us_slot_rejects_monday():
    slot = _find_slot("us_0600")
    # 2026-04-13 is a Monday (weekday=0) → not in Tue-Sat
    now = _jst(2026, 4, 13, 6, 0)
    assert now.weekday() == 0
    assert sched._should_fire(slot, now) is False


def test_should_fire_us_slot_accepts_tuesday_at_exact_time():
    slot = _find_slot("us_0600")
    # 2026-04-14 is a Tuesday (weekday=1)
    now = _jst(2026, 4, 14, 6, 0)
    assert now.weekday() == 1
    assert sched._should_fire(slot, now) is True


def test_should_fire_jp_slot_rejects_saturday():
    slot = _find_slot("jp_1530")
    # 2026-04-11 is a Saturday (weekday=5) → not in Mon-Fri
    now = _jst(2026, 4, 11, 15, 30)
    assert now.weekday() == 5
    assert sched._should_fire(slot, now) is False


def test_should_fire_jp_slot_accepts_friday():
    slot = _find_slot("jp_1530")
    # 2026-04-10 is a Friday (weekday=4)
    now = _jst(2026, 4, 10, 15, 30)
    assert now.weekday() == 4
    assert sched._should_fire(slot, now) is True


# ── _should_fire: time window ─────────────────────────────────────────────

def test_should_fire_within_5_minute_window():
    slot = _find_slot("us_0600")
    now = _jst(2026, 4, 14, 6, 3)  # 3 min after scheduled
    assert sched._should_fire(slot, now) is True


def test_should_fire_rejects_past_window():
    slot = _find_slot("us_0600")
    now = _jst(2026, 4, 14, 6, 6)  # 6 min after — past 5-min window
    assert sched._should_fire(slot, now) is False


def test_should_fire_rejects_before_scheduled_time():
    slot = _find_slot("us_0600")
    now = _jst(2026, 4, 14, 5, 59)
    assert sched._should_fire(slot, now) is False


# ── _should_fire: dedup ──────────────────────────────────────────────────

def test_should_not_double_fire_same_day():
    slot = _find_slot("us_0600")
    now = _jst(2026, 4, 14, 6, 0)
    # First check: allowed
    assert sched._should_fire(slot, now) is True
    # Record the fire
    sched._mark_fired(slot, now)
    # Same slot, same day → refused even at the exact scheduled time
    now2 = _jst(2026, 4, 14, 6, 2)
    assert sched._should_fire(slot, now2) is False


def test_next_day_fires_again_after_mark():
    slot = _find_slot("us_0600")
    # Fire on Tuesday
    sched._mark_fired(slot, _jst(2026, 4, 14, 6, 0))
    # Wednesday is also in Tue-Sat and is a fresh day
    tomorrow = _jst(2026, 4, 15, 6, 0)
    assert sched._should_fire(slot, tomorrow) is True


# ── _mark_fired ───────────────────────────────────────────────────────────

def test_mark_fired_records_today_date():
    slot = _find_slot("us_0600")
    now = _jst(2026, 4, 14, 6, 0)
    sched._mark_fired(slot, now)
    assert sched._last_fired.get("us_0600") == "2026-04-14"


def test_mark_fired_is_per_slot():
    us_slot = _find_slot("us_0600")
    jp_slot = _find_slot("jp_1530")
    sched._mark_fired(us_slot, _jst(2026, 4, 14, 6, 0))
    assert "us_0600" in sched._last_fired
    assert "jp_1530" not in sched._last_fired
    sched._mark_fired(jp_slot, _jst(2026, 4, 14, 15, 30))
    assert "jp_1530" in sched._last_fired


# ── get_schedule_info ────────────────────────────────────────────────────

def test_get_schedule_info_shape():
    info = sched.get_schedule_info()
    assert "now_jst" in info
    assert "weekday" in info
    assert "slots" in info
    assert isinstance(info["slots"], list)
    slot_ids = {s["slot_id"] for s in info["slots"]}
    assert slot_ids == {"us_0600", "jp_1530"}


def test_get_schedule_info_slot_fields():
    info = sched.get_schedule_info()
    for slot_info in info["slots"]:
        for key in ("slot_id", "label", "time_jst", "weekdays", "index", "last_fired_date"):
            assert key in slot_info


# ── start_scheduler idempotency (no thread spin-up expected in test) ─────

def test_start_scheduler_is_idempotent_when_already_started(monkeypatch):
    # Force the started flag off first — but if another test already
    # flipped it, that's fine. We just check the second call is a no-op.
    calls = {"count": 0}

    def fake_trigger(index, label):
        calls["count"] += 1

    # Ensure flag is False; save/restore to keep other tests clean.
    original_started = sched._started
    original_thread = sched._thread
    try:
        sched._started = False
        sched._thread = None

        # First call — replace Thread to avoid actually starting one.
        class _FakeThread:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

            def start(self):
                pass

        monkeypatch.setattr(sched.threading, "Thread", _FakeThread)
        sched.start_scheduler(fake_trigger)
        assert sched._started is True
        first_thread = sched._thread

        # Second call should be a no-op
        sched.start_scheduler(fake_trigger)
        assert sched._thread is first_thread  # unchanged
    finally:
        sched._started = original_started
        sched._thread = original_thread
