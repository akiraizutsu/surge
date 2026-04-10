"""Per-user rate limiting and cost tracking for LLM usage."""

import os
from datetime import datetime, timezone, timedelta

import database
import auth_service


# ── Configuration ────────────────────────────────────────────────────────

DAILY_LIMITS = {
    "user":  int(os.environ.get("FRIEND_DAILY_REQUEST_LIMIT", "30")),
    "owner": int(os.environ.get("OWNER_DAILY_REQUEST_LIMIT", "200")),
}

GLOBAL_COST_LIMIT_USD = float(os.environ.get("TOTAL_DAILY_COST_LIMIT_USD", "5.00"))

# Gemini pricing (USD per token). Source: Google AI pricing page 2025.
MODEL_PRICING = {
    "gemini-2.5-flash":       {"in": 0.10 / 1_000_000, "out": 0.40 / 1_000_000},
    "gemini-2.5-flash-lite":  {"in": 0.05 / 1_000_000, "out": 0.20 / 1_000_000},
    "gemini-2.5-pro":         {"in": 1.25 / 1_000_000, "out": 10.0 / 1_000_000},
}

# JST for daily reset alignment
_JST = timezone(timedelta(hours=9))


def _today_jst():
    return datetime.now(_JST).strftime("%Y-%m-%d")


def calculate_cost(model, tokens_in, tokens_out):
    """Compute USD cost for a request."""
    pricing = MODEL_PRICING.get(model) or MODEL_PRICING["gemini-2.5-flash"]
    return tokens_in * pricing["in"] + tokens_out * pricing["out"]


def check_rate_limit(user_id):
    """Check if a user is allowed to make another request today.

    Returns (allowed: bool, message: str | None).
    """
    user = auth_service.get_user(user_id)
    if not user:
        return False, "user not found"

    role = user.get("role", "user")
    limit = DAILY_LIMITS.get(role, DAILY_LIMITS["user"])
    today = _today_jst()

    usage = database.get_usage(user_id, today)
    if usage["request_count"] >= limit:
        return False, f"本日の利用上限（{limit}回）に達しました。明日0時(JST)にリセットされます。"

    # Global cost brake — block non-owner users when exceeded
    global_cost = database.get_global_cost_today(today)
    if global_cost >= GLOBAL_COST_LIMIT_USD and role != "owner":
        return False, "システム全体のコスト上限に達しました。オーナーに連絡してください。"

    return True, None


def record_usage(user_id, model, tokens_in, tokens_out):
    """Record a completed LLM request and its cost."""
    cost = calculate_cost(model, tokens_in, tokens_out)
    today = _today_jst()
    database.increment_usage(user_id, today, tokens_in, tokens_out, cost)


def get_usage_today(user_id):
    """Get today's usage summary for a user, including limits and remaining."""
    user = auth_service.get_user(user_id)
    if not user:
        return None
    role = user.get("role", "user")
    limit = DAILY_LIMITS.get(role, DAILY_LIMITS["user"])
    today = _today_jst()
    usage = database.get_usage(user_id, today)
    remaining = max(0, limit - usage["request_count"])
    return {
        "date": today,
        "request_count": usage["request_count"],
        "tokens_in": usage["tokens_in"],
        "tokens_out": usage["tokens_out"],
        "cost_usd": round(usage["cost_usd"], 4),
        "limit": limit,
        "remaining": remaining,
        "reset_at": "明日 00:00 JST",
    }


def get_global_cost_summary():
    """Return global spending summary for today (admin view)."""
    today = _today_jst()
    total = database.get_global_cost_today(today)
    return {
        "date": today,
        "total_cost_usd": round(total, 4),
        "limit_usd": GLOBAL_COST_LIMIT_USD,
        "remaining_usd": round(max(0, GLOBAL_COST_LIMIT_USD - total), 4),
        "exceeded": total >= GLOBAL_COST_LIMIT_USD,
    }


def check_global_cost_brake():
    """Return True if global cost limit exceeded (non-owner should be blocked)."""
    today = _today_jst()
    total = database.get_global_cost_today(today)
    return total >= GLOBAL_COST_LIMIT_USD
