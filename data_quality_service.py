"""Data Quality Service — Sprint 9 (仕様書 Section 9)

Tracks data source health, logs fetch success/failure rates,
detects missing/anomalous values, and provides UI-ready status summaries.

All state is persisted in the `data_source_status` SQLite table.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime

import database

_lock = threading.Lock()

# ── Source name constants ─────────────────────────────────────────────────────

SOURCE_YFINANCE    = "yfinance"
SOURCE_WIKIPEDIA   = "wikipedia"
SOURCE_EDINET      = "edinet"
SOURCE_JQUANTS     = "jquants"
SOURCE_BENCHMARK   = "benchmark_etf"


# ── Public API ────────────────────────────────────────────────────────────────

def record_success(source: str, metadata: dict | None = None):
    """Record a successful fetch for a data source."""
    _upsert(source, success=True, error=None, metadata=metadata)


def record_failure(source: str, error: str, metadata: dict | None = None):
    """Record a failed fetch for a data source."""
    _upsert(source, success=False, error=error, metadata=metadata)


def get_status_summary() -> list[dict]:
    """Return UI-ready status for all known data sources."""
    conn = database._connect()
    rows = conn.execute("SELECT * FROM data_source_status ORDER BY source_name").fetchall()
    conn.close()

    result = []
    for r in rows:
        meta = {}
        try:
            meta = json.loads(r["metadata_json"]) if r["metadata_json"] else {}
        except Exception:
            pass

        age_h = _age_hours(r["last_success_at"])
        health = r["health_status"] or "unknown"

        result.append({
            "source_name":    r["source_name"],
            "health_status":  health,
            "health_label":   _health_label(health),
            "last_success_at": r["last_success_at"],
            "last_failure_at": r["last_failure_at"],
            "last_error":      r["last_error"],
            "age_hours":       age_h,
            "stale":           age_h is not None and age_h > 24,
            "metadata":        meta,
        })
    return result


def validate_ranking(ranking: list[dict]) -> dict:
    """Scan a ranking list for missing/anomalous values.

    Returns:
        {
          "missing_fields":  [{ticker, fields: [str]}],
          "anomalous":       [{ticker, field, value, reason}],
          "missing_count":   int,
          "anomalous_count": int,
          "quality_pct":     float,   # 0-100 (higher = cleaner)
        }
    """
    REQUIRED_FIELDS = [
        ("price",          lambda v: v and v > 0),
        ("momentum_score", lambda v: v is not None),
        ("ret_1m",         lambda v: v is not None),
        ("rsi",            lambda v: v is not None),
    ]
    ANOMALY_CHECKS = [
        ("price",          lambda v: v and v > 0,         "株価ゼロ/負"),
        ("rsi",            lambda v: 0 <= (v or 50) <= 100, "RSI範囲外"),
        ("vol_ratio",      lambda v: (v or 1) < 100,       "出来高比異常 (>100x)"),
        ("momentum_score", lambda v: 0 <= (v or 50) <= 100, "スコア範囲外"),
    ]

    missing_fields = []
    anomalous      = []

    for r in ranking:
        t = r.get("technicals", {}) or {}
        merged = {**r, **t}

        missing = []
        for field, check in REQUIRED_FIELDS:
            val = merged.get(field)
            if not check(val):
                missing.append(field)
        if missing:
            missing_fields.append({"ticker": r.get("ticker"), "fields": missing})

        for field, check, reason in ANOMALY_CHECKS:
            val = merged.get(field)
            if val is not None and not check(val):
                anomalous.append({"ticker": r.get("ticker"), "field": field, "value": val, "reason": reason})

    total = len(ranking)
    quality_pct = round(
        (1 - (len(missing_fields) + len(anomalous)) / max(total * 2, 1)) * 100, 1
    ) if total > 0 else 0
    quality_pct = max(0.0, min(100.0, quality_pct))

    return {
        "missing_fields":  missing_fields[:20],
        "anomalous":       anomalous[:20],
        "missing_count":   len(missing_fields),
        "anomalous_count": len(anomalous),
        "quality_pct":     quality_pct,
    }


def get_ticker_coverage(ranking: list[dict], total_tickers: int) -> dict:
    """Compute fetch coverage statistics."""
    screened = len(ranking)
    coverage = round(screened / total_tickers * 100, 1) if total_tickers > 0 else 0
    no_fundamental = sum(
        1 for r in ranking
        if not (r.get("fundamentals", {}) or {}).get("pe_forward")
           and not (r.get("fundamentals", {}) or {}).get("target_price")
    )
    return {
        "total_tickers":    total_tickers,
        "screened":         screened,
        "coverage_pct":     coverage,
        "no_fundamental":   no_fundamental,
        "no_fundamental_pct": round(no_fundamental / max(screened, 1) * 100, 1),
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

def _upsert(source: str, success: bool, error: str | None, metadata: dict | None):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    health = _compute_health(success, error)
    meta_json = json.dumps(metadata or {}, ensure_ascii=False)
    conn = database._connect()
    with conn:
        existing = conn.execute(
            "SELECT id FROM data_source_status WHERE source_name = ?", (source,)
        ).fetchone()
        if existing:
            if success:
                conn.execute(
                    """UPDATE data_source_status
                       SET last_success_at=?, health_status=?, metadata_json=?
                       WHERE source_name=?""",
                    (now, health, meta_json, source),
                )
            else:
                conn.execute(
                    """UPDATE data_source_status
                       SET last_failure_at=?, last_error=?, health_status=?, metadata_json=?
                       WHERE source_name=?""",
                    (now, error or "", health, meta_json, source),
                )
        else:
            conn.execute(
                """INSERT INTO data_source_status
                   (source_name, last_success_at, last_failure_at, last_error, health_status, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    source,
                    now if success else None,
                    None if success else now,
                    None if success else error or "",
                    health,
                    meta_json,
                ),
            )
    conn.close()


def _compute_health(success: bool, error: str | None) -> str:
    if success:
        return "ok"
    if error and any(k in (error or "").lower() for k in ["timeout", "connection", "network"]):
        return "degraded"
    return "error"


def _age_hours(ts_str: str | None) -> float | None:
    if not ts_str:
        return None
    try:
        ts = datetime.strptime(ts_str[:19], "%Y-%m-%d %H:%M:%S")
        return round((datetime.now() - ts).total_seconds() / 3600, 1)
    except Exception:
        return None


def _health_label(health: str) -> str:
    return {"ok": "正常", "degraded": "低下", "error": "エラー", "unknown": "未取得"}.get(health, health)
