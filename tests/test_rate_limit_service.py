"""Tests for rate_limit_service.

This service depends on `database` and `auth_service` for storage.
Real database access is avoided by monkeypatching the imported
module attributes on rate_limit_service itself.
"""

import pytest

import rate_limit_service


# ── Test fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def fake_backend(monkeypatch):
    """Install fake database + auth_service onto rate_limit_service.

    Returns a dict you can mutate to simulate different states between
    `check_rate_limit` / `record_usage` calls.
    """
    state = {
        "user":         {"id": 1, "username": "friend", "role": "user"},
        "usage":        {"request_count": 0, "tokens_in": 0, "tokens_out": 0, "cost_usd": 0.0},
        "global_cost":  0.0,
        "increments":   [],
    }

    def fake_get_user(user_id):
        return state["user"]

    def fake_get_usage(user_id, date):
        return state["usage"]

    def fake_get_global_cost_today(date):
        return state["global_cost"]

    def fake_increment_usage(user_id, date, tokens_in, tokens_out, cost):
        state["increments"].append({
            "user_id": user_id, "date": date,
            "tokens_in": tokens_in, "tokens_out": tokens_out, "cost": cost,
        })
        state["usage"]["request_count"] += 1
        state["usage"]["tokens_in"] += tokens_in
        state["usage"]["tokens_out"] += tokens_out
        state["usage"]["cost_usd"] += cost
        state["global_cost"] += cost

    monkeypatch.setattr(rate_limit_service.auth_service, "get_user", fake_get_user)
    monkeypatch.setattr(rate_limit_service.database, "get_usage", fake_get_usage)
    monkeypatch.setattr(rate_limit_service.database, "get_global_cost_today", fake_get_global_cost_today)
    monkeypatch.setattr(rate_limit_service.database, "increment_usage", fake_increment_usage)

    return state


# ── calculate_cost (pure) ─────────────────────────────────────────────────

def test_calculate_cost_flash_known_values():
    # flash pricing: in=0.10/1M, out=0.40/1M
    cost = rate_limit_service.calculate_cost("gemini-2.5-flash", 1_000_000, 1_000_000)
    assert cost == pytest.approx(0.10 + 0.40)


def test_calculate_cost_flash_lite_half_of_flash():
    cost = rate_limit_service.calculate_cost("gemini-2.5-flash-lite", 1_000_000, 1_000_000)
    assert cost == pytest.approx(0.05 + 0.20)


def test_calculate_cost_pro_much_more_expensive():
    flash_cost = rate_limit_service.calculate_cost("gemini-2.5-flash", 1_000_000, 1_000_000)
    pro_cost = rate_limit_service.calculate_cost("gemini-2.5-pro", 1_000_000, 1_000_000)
    assert pro_cost > flash_cost * 10


def test_calculate_cost_unknown_model_falls_back_to_flash():
    cost_unknown = rate_limit_service.calculate_cost("made-up-model", 1_000_000, 1_000_000)
    cost_flash = rate_limit_service.calculate_cost("gemini-2.5-flash", 1_000_000, 1_000_000)
    assert cost_unknown == pytest.approx(cost_flash)


def test_calculate_cost_zero_tokens():
    assert rate_limit_service.calculate_cost("gemini-2.5-flash", 0, 0) == 0.0


# ── check_rate_limit ─────────────────────────────────────────────────────

def test_check_rate_limit_user_under_limit(fake_backend):
    fake_backend["usage"]["request_count"] = 5
    allowed, msg = rate_limit_service.check_rate_limit(1)
    assert allowed is True
    assert msg is None


def test_check_rate_limit_user_at_limit(fake_backend, monkeypatch):
    # Force limit to a small number for easy testing
    monkeypatch.setitem(rate_limit_service.DAILY_LIMITS, "user", 10)
    fake_backend["usage"]["request_count"] = 10
    allowed, msg = rate_limit_service.check_rate_limit(1)
    assert allowed is False
    assert "10" in msg  # limit appears in message


def test_check_rate_limit_owner_has_higher_limit(fake_backend, monkeypatch):
    monkeypatch.setitem(rate_limit_service.DAILY_LIMITS, "owner", 200)
    fake_backend["user"]["role"] = "owner"
    fake_backend["usage"]["request_count"] = 50  # well under owner limit
    allowed, msg = rate_limit_service.check_rate_limit(1)
    assert allowed is True
    assert msg is None


def test_check_rate_limit_blocks_user_when_global_cost_exceeded(fake_backend, monkeypatch):
    monkeypatch.setattr(rate_limit_service, "GLOBAL_COST_LIMIT_USD", 5.0)
    fake_backend["global_cost"] = 6.0  # exceeded
    fake_backend["user"]["role"] = "user"
    allowed, msg = rate_limit_service.check_rate_limit(1)
    assert allowed is False
    assert "コスト上限" in msg or "システム全体" in msg


def test_check_rate_limit_exempts_owner_from_global_cost_brake(fake_backend, monkeypatch):
    monkeypatch.setattr(rate_limit_service, "GLOBAL_COST_LIMIT_USD", 5.0)
    fake_backend["global_cost"] = 20.0
    fake_backend["user"]["role"] = "owner"
    allowed, msg = rate_limit_service.check_rate_limit(1)
    assert allowed is True
    assert msg is None


def test_check_rate_limit_user_not_found(monkeypatch):
    monkeypatch.setattr(rate_limit_service.auth_service, "get_user", lambda _id: None)
    allowed, msg = rate_limit_service.check_rate_limit(999)
    assert allowed is False
    assert "not found" in msg


# ── record_usage ─────────────────────────────────────────────────────────

def test_record_usage_calls_increment_with_computed_cost(fake_backend):
    rate_limit_service.record_usage(
        user_id=1, model="gemini-2.5-flash",
        tokens_in=1000, tokens_out=500,
    )
    assert len(fake_backend["increments"]) == 1
    rec = fake_backend["increments"][0]
    assert rec["user_id"] == 1
    assert rec["tokens_in"] == 1000
    assert rec["tokens_out"] == 500
    expected = rate_limit_service.calculate_cost("gemini-2.5-flash", 1000, 500)
    assert rec["cost"] == pytest.approx(expected)


# ── get_usage_today ───────────────────────────────────────────────────────

def test_get_usage_today_computes_remaining(fake_backend, monkeypatch):
    monkeypatch.setitem(rate_limit_service.DAILY_LIMITS, "user", 30)
    fake_backend["usage"] = {
        "request_count": 7, "tokens_in": 100, "tokens_out": 50, "cost_usd": 0.01,
    }
    summary = rate_limit_service.get_usage_today(1)
    assert summary["request_count"] == 7
    assert summary["limit"] == 30
    assert summary["remaining"] == 23
    assert summary["cost_usd"] == 0.01
    assert "reset_at" in summary


def test_get_usage_today_user_not_found(monkeypatch):
    monkeypatch.setattr(rate_limit_service.auth_service, "get_user", lambda _id: None)
    assert rate_limit_service.get_usage_today(999) is None


def test_get_usage_today_remaining_never_negative(fake_backend, monkeypatch):
    monkeypatch.setitem(rate_limit_service.DAILY_LIMITS, "user", 30)
    fake_backend["usage"]["request_count"] = 100  # way over
    summary = rate_limit_service.get_usage_today(1)
    assert summary["remaining"] == 0


# ── get_global_cost_summary ──────────────────────────────────────────────

def test_get_global_cost_summary_shape(fake_backend, monkeypatch):
    monkeypatch.setattr(rate_limit_service, "GLOBAL_COST_LIMIT_USD", 5.0)
    fake_backend["global_cost"] = 1.23
    summary = rate_limit_service.get_global_cost_summary()
    assert summary["total_cost_usd"] == 1.23
    assert summary["limit_usd"] == 5.0
    assert summary["remaining_usd"] == pytest.approx(3.77)
    assert summary["exceeded"] is False


def test_get_global_cost_summary_exceeded_flag(fake_backend, monkeypatch):
    monkeypatch.setattr(rate_limit_service, "GLOBAL_COST_LIMIT_USD", 5.0)
    fake_backend["global_cost"] = 10.0
    summary = rate_limit_service.get_global_cost_summary()
    assert summary["exceeded"] is True
    assert summary["remaining_usd"] == 0.0


def test_check_global_cost_brake(fake_backend, monkeypatch):
    monkeypatch.setattr(rate_limit_service, "GLOBAL_COST_LIMIT_USD", 5.0)
    fake_backend["global_cost"] = 4.0
    assert rate_limit_service.check_global_cost_brake() is False
    fake_backend["global_cost"] = 5.0
    assert rate_limit_service.check_global_cost_brake() is True
