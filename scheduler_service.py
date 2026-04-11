"""Background scheduler for automatic screening runs.

Runs in a daemon thread, checks every minute, and triggers the screening
callback at the configured JST local times.

Schedule:
- 06:00 JST, Tue-Sat → US markets (sp500 + nasdaq100)
  (Tue-Sat because Fri US close reflects in Sat JST 06:00; no run on Mon JST
  because US markets are closed on US Sunday.)
- 15:30 JST, Mon-Fri → Japan markets (nikkei225 + growth250)
  (Only on JP trading days.)

The scheduler is idempotent: it records the last-fired (date, slot) in memory
and refuses to fire the same slot twice on the same JST date. If the app is
restarted during a firing window, it will still fire once because the in-memory
record is reset — this is acceptable for our use case since the screening
endpoint itself guards against concurrent runs.
"""

import threading
import time
from datetime import datetime, timezone, timedelta

# JST = UTC+9, no DST ever.
JST = timezone(timedelta(hours=9))


# ── Schedule definition ──────────────────────────────────────────────────

# Each slot: (hour, minute, allowed_weekdays, index_key)
# weekday: Monday=0, Sunday=6
_SLOTS = [
    # 06:00 JST — US markets, Tuesday through Saturday (JST)
    #  - Tue JST ← Mon US close
    #  - Wed JST ← Tue US close
    #  - Thu JST ← Wed US close
    #  - Fri JST ← Thu US close
    #  - Sat JST ← Fri US close
    #  - Skipped: Sun JST (Sat US no market) and Mon JST (Sun US no market)
    {
        "slot_id": "us_0600",
        "hour": 6,
        "minute": 0,
        "weekdays": {1, 2, 3, 4, 5},  # Tue-Sat
        "index": "us_all",
        "label": "US自動スクリーニング",
    },
    # 15:30 JST — Japan markets, Monday through Friday (JST)
    {
        "slot_id": "jp_1530",
        "hour": 15,
        "minute": 30,
        "weekdays": {0, 1, 2, 3, 4},  # Mon-Fri
        "index": "japan_all",
        "label": "JP自動スクリーニング",
    },
]

# How many minutes after the scheduled time we still allow the slot to fire.
# Protects against minute-level drift and app startup grace.
_FIRE_WINDOW_MIN = 5


# ── State ────────────────────────────────────────────────────────────────

# {slot_id: "YYYY-MM-DD"} — the JST date on which we last fired this slot.
_last_fired = {}
_state_lock = threading.Lock()
_started = False
_thread = None


def _now_jst():
    return datetime.now(JST)


def _should_fire(slot, now):
    """Return True if this slot should fire right now."""
    if now.weekday() not in slot["weekdays"]:
        return False
    scheduled = now.replace(hour=slot["hour"], minute=slot["minute"], second=0, microsecond=0)
    # Only fire within [scheduled, scheduled + window]
    if now < scheduled:
        return False
    delta_min = (now - scheduled).total_seconds() / 60
    if delta_min > _FIRE_WINDOW_MIN:
        return False
    # Don't double-fire on the same date
    today_str = now.strftime("%Y-%m-%d")
    with _state_lock:
        if _last_fired.get(slot["slot_id"]) == today_str:
            return False
    return True


def _mark_fired(slot, now):
    today_str = now.strftime("%Y-%m-%d")
    with _state_lock:
        _last_fired[slot["slot_id"]] = today_str


def _scheduler_loop(trigger_fn):
    """Main scheduler loop. Runs forever in a daemon thread.

    trigger_fn(index_key, label) is called when a slot fires.
    """
    while True:
        try:
            now = _now_jst()
            for slot in _SLOTS:
                if _should_fire(slot, now):
                    _mark_fired(slot, now)
                    try:
                        print(f"[scheduler] Firing {slot['slot_id']} at {now.isoformat()} → {slot['index']}")
                        trigger_fn(slot["index"], slot["label"])
                    except Exception as e:
                        print(f"[scheduler] Failed to fire {slot['slot_id']}: {e}")
        except Exception as e:
            print(f"[scheduler] loop error: {e}")
        # Check once a minute. Sleeping less than one minute wastes cycles;
        # sleeping more risks missing the 5-minute window on a drifting host.
        time.sleep(30)


def start_scheduler(trigger_fn):
    """Start the background scheduler thread (idempotent).

    trigger_fn(index_key, label) is called at each fire time.
    """
    global _started, _thread
    if _started:
        return
    _started = True
    _thread = threading.Thread(
        target=_scheduler_loop,
        args=(trigger_fn,),
        daemon=True,
        name="surge-scheduler",
    )
    _thread.start()
    print("[scheduler] Started: US 06:00 JST (Tue-Sat), JP 15:30 JST (Mon-Fri)")


def get_schedule_info():
    """Return a human-readable snapshot of the schedule state for debugging."""
    now = _now_jst()
    out = {
        "now_jst": now.isoformat(),
        "weekday": now.strftime("%a"),
        "slots": [],
    }
    with _state_lock:
        for slot in _SLOTS:
            out["slots"].append({
                "slot_id": slot["slot_id"],
                "label": slot["label"],
                "time_jst": f"{slot['hour']:02d}:{slot['minute']:02d}",
                "weekdays": sorted(slot["weekdays"]),
                "index": slot["index"],
                "last_fired_date": _last_fired.get(slot["slot_id"]),
            })
    return out
