"""Tests for Sprint 8 features: Timeline, Smart Watchlist, Morning Brief."""

import json
import os
import sqlite3
import tempfile
import pytest

# ── Helpers ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(monkeypatch):
    """Create a temporary SQLite DB and patch database module to use it."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    import database
    monkeypatch.setattr(database, "DB_PATH", path)
    database.init_db()
    yield path
    os.unlink(path)


def _insert_session(db_path, index_name="nikkei225", generated_at="2026-04-10 09:00", regime_json=None, brief_json=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """INSERT INTO screening_sessions (index_name, top_n, total_screened, generated_at, regime_json, brief_json)
           VALUES (?, 20, 200, ?, ?, ?)""",
        (index_name, generated_at, regime_json, brief_json),
    )
    sid = conn.execute("SELECT last_insert_rowid() as id").fetchone()["id"]
    conn.commit()
    conn.close()
    return sid


def _insert_result(db_path, session_id, ticker, momentum_score, rsi, ret_1m, rank):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO screening_results
           (session_id, ticker, name, sector, price, momentum_score, rsi, ret_1m, rank)
           VALUES (?, ?, ?, 'Tech', 100, ?, ?, ?, ?)""",
        (session_id, ticker, ticker, momentum_score, rsi, ret_1m, rank),
    )
    conn.commit()
    conn.close()


def _insert_watchlist(db_path, ticker, user_id=1, alert_rules_json=None):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT OR IGNORE INTO watchlist (ticker, user_id, alert_rules_json) VALUES (?, ?, ?)",
        (ticker, user_id, alert_rules_json),
    )
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# Feature 2: Momentum Timeline
# ══════════════════════════════════════════════════════════════════════════════

class TestTickerTimeline:
    def test_timeline_returns_chronological_order(self, tmp_db):
        """get_ticker_timeline should return oldest-first."""
        import database
        s1 = _insert_session(tmp_db, generated_at="2026-04-01 09:00")
        s2 = _insert_session(tmp_db, generated_at="2026-04-02 09:00")
        s3 = _insert_session(tmp_db, generated_at="2026-04-03 09:00")
        _insert_result(tmp_db, s1, "AAPL", 80.0, 55.0, 5.0, 3)
        _insert_result(tmp_db, s2, "AAPL", 85.0, 60.0, 7.0, 2)
        _insert_result(tmp_db, s3, "AAPL", 90.0, 65.0, 10.0, 1)

        data = database.get_ticker_timeline("AAPL", limit=30)
        assert len(data) == 3
        # Chronological: oldest first
        assert data[0]["generated_at"] == "2026-04-01 09:00"
        assert data[1]["generated_at"] == "2026-04-02 09:00"
        assert data[2]["generated_at"] == "2026-04-03 09:00"
        # Scores ascending
        assert data[0]["momentum_score"] == 80.0
        assert data[2]["momentum_score"] == 90.0

    def test_timeline_respects_limit(self, tmp_db):
        """get_ticker_timeline should respect the limit parameter."""
        import database
        for i in range(10):
            sid = _insert_session(tmp_db, generated_at=f"2026-04-{i+1:02d} 09:00")
            _insert_result(tmp_db, sid, "MSFT", 70 + i, 50 + i, 3.0, 5)

        data = database.get_ticker_timeline("MSFT", limit=3)
        assert len(data) == 3
        # Should be the 3 most recent, in chronological order
        assert data[0]["generated_at"] == "2026-04-08 09:00"
        assert data[2]["generated_at"] == "2026-04-10 09:00"

    def test_timeline_empty_for_unknown_ticker(self, tmp_db):
        """get_ticker_timeline returns empty list for nonexistent ticker."""
        import database
        data = database.get_ticker_timeline("UNKNOWN")
        assert data == []

    def test_timeline_case_insensitive(self, tmp_db):
        """Ticker lookup should be case-insensitive (uppercased)."""
        import database
        sid = _insert_session(tmp_db)
        _insert_result(tmp_db, sid, "GOOG", 88.0, 62.0, 8.0, 1)

        data = database.get_ticker_timeline("goog")
        assert len(data) == 1
        assert data[0]["momentum_score"] == 88.0

    def test_timeline_includes_all_fields(self, tmp_db):
        """Each timeline entry should have score, rsi, ret_1m, rank, generated_at."""
        import database
        sid = _insert_session(tmp_db, index_name="sp500", generated_at="2026-04-05 15:30")
        _insert_result(tmp_db, sid, "NVDA", 92.5, 71.3, 12.5, 1)

        data = database.get_ticker_timeline("NVDA")
        assert len(data) == 1
        entry = data[0]
        assert entry["momentum_score"] == 92.5
        assert entry["rsi"] == 71.3
        assert entry["ret_1m"] == 12.5
        assert entry["rank"] == 1
        assert entry["generated_at"] == "2026-04-05 15:30"
        assert entry["index_name"] == "sp500"


# ══════════════════════════════════════════════════════════════════════════════
# Feature 3: Smart Watchlist (Alert Rules)
# ══════════════════════════════════════════════════════════════════════════════

class TestAlertRules:
    def test_get_alert_rules_empty(self, tmp_db):
        """No rules returns empty list."""
        import database
        _insert_watchlist(tmp_db, "AAPL", user_id=1)
        rules = database.get_alert_rules("AAPL", user_id=1)
        assert rules == []

    def test_update_and_get_alert_rules(self, tmp_db):
        """Rules round-trip through update → get."""
        import database
        _insert_watchlist(tmp_db, "TSLA", user_id=1)
        rules = [
            {"field": "rsi", "op": "<", "value": 30, "label": "RSI < 30"},
            {"field": "momentum_score", "op": ">", "value": 90, "label": "スコア > 90"},
        ]
        ok = database.update_alert_rules("TSLA", user_id=1, rules=rules)
        assert ok is True

        fetched = database.get_alert_rules("TSLA", user_id=1)
        assert len(fetched) == 2
        assert fetched[0]["field"] == "rsi"
        assert fetched[1]["value"] == 90

    def test_update_rules_nonexistent_ticker(self, tmp_db):
        """Updating rules for non-watchlisted ticker returns False."""
        import database
        ok = database.update_alert_rules("FAKE", user_id=1, rules=[{"field": "rsi", "op": "<", "value": 30}])
        assert ok is False

    def test_get_alert_rules_wrong_user(self, tmp_db):
        """Rules are per-user — different user_id returns empty."""
        import database
        _insert_watchlist(tmp_db, "GOOG", user_id=1, alert_rules_json='[{"field":"rsi","op":"<","value":30}]')
        rules = database.get_alert_rules("GOOG", user_id=999)
        assert rules == []

    def test_get_all_alert_rules(self, tmp_db):
        """get_all_alert_rules returns all entries with rules across users."""
        import database
        _insert_watchlist(tmp_db, "AAPL", user_id=1, alert_rules_json='[{"field":"rsi","op":"<","value":30}]')
        _insert_watchlist(tmp_db, "MSFT", user_id=1, alert_rules_json='[]')  # empty rules
        _insert_watchlist(tmp_db, "GOOG", user_id=2, alert_rules_json='[{"field":"momentum_score","op":">","value":90}]')
        _insert_watchlist(tmp_db, "TSLA", user_id=1)  # no rules at all

        all_rules = database.get_all_alert_rules()
        # Only AAPL and GOOG have non-empty rules
        tickers = {r["ticker"] for r in all_rules}
        assert tickers == {"AAPL", "GOOG"}

    def test_case_insensitive_alert_rules(self, tmp_db):
        """Alert rule CRUD uppercases tickers."""
        import database
        _insert_watchlist(tmp_db, "AAPL", user_id=1)
        ok = database.update_alert_rules("aapl", user_id=1, rules=[{"field": "rsi", "op": "<", "value": 25}])
        assert ok is True
        rules = database.get_alert_rules("aapl", user_id=1)
        assert len(rules) == 1


class TestCheckCustomAlerts:
    def test_alerts_trigger_correctly(self, tmp_db):
        """check_custom_alerts fires when condition is met."""
        import database
        from screener import check_custom_alerts

        _insert_watchlist(tmp_db, "AAPL", user_id=1,
                          alert_rules_json='[{"field":"rsi","op":"<","value":30,"label":"RSI < 30"}]')

        ranking = [
            {"ticker": "AAPL", "name": "Apple", "rank": 1, "momentum_score": 85,
             "technicals": {"rsi": 25, "ret_1m": 5}, "fundamentals": {}},
        ]
        events = check_custom_alerts(ranking, "sp500")
        assert len(events) == 1
        assert events[0]["event_type"] == "custom_alert"
        payload = json.loads(events[0]["payload_json"])
        assert payload["ticker"] == "AAPL"
        assert payload["rule"] == "RSI < 30"
        assert payload["actual"] == 25.0

    def test_alerts_do_not_trigger_when_condition_not_met(self, tmp_db):
        """check_custom_alerts does not fire when condition is false."""
        import database
        from screener import check_custom_alerts

        _insert_watchlist(tmp_db, "AAPL", user_id=1,
                          alert_rules_json='[{"field":"rsi","op":"<","value":30,"label":"RSI < 30"}]')

        ranking = [
            {"ticker": "AAPL", "name": "Apple", "rank": 1, "momentum_score": 85,
             "technicals": {"rsi": 55}, "fundamentals": {}},
        ]
        events = check_custom_alerts(ranking, "sp500")
        assert len(events) == 0

    def test_alerts_skip_missing_ticker(self, tmp_db):
        """Ticker in rules but not in ranking → no event."""
        import database
        from screener import check_custom_alerts

        _insert_watchlist(tmp_db, "TSLA", user_id=1,
                          alert_rules_json='[{"field":"rsi","op":">","value":70,"label":"RSI > 70"}]')

        ranking = [
            {"ticker": "AAPL", "name": "Apple", "rank": 1, "momentum_score": 80,
             "technicals": {"rsi": 75}, "fundamentals": {}},
        ]
        events = check_custom_alerts(ranking, "sp500")
        assert len(events) == 0

    def test_alerts_multiple_rules_multiple_triggers(self, tmp_db):
        """Multiple rules on same ticker can each fire independently."""
        import database
        from screener import check_custom_alerts

        rules = [
            {"field": "rsi", "op": "<", "value": 30, "label": "RSI低"},
            {"field": "momentum_score", "op": ">", "value": 80, "label": "スコア高"},
        ]
        _insert_watchlist(tmp_db, "GOOG", user_id=1, alert_rules_json=json.dumps(rules))

        ranking = [
            {"ticker": "GOOG", "name": "Google", "rank": 2, "momentum_score": 92,
             "technicals": {"rsi": 22}, "fundamentals": {}},
        ]
        events = check_custom_alerts(ranking, "nasdaq100")
        assert len(events) == 2
        labels = {json.loads(e["payload_json"])["rule"] for e in events}
        assert "RSI低" in labels
        assert "スコア高" in labels

    def test_alerts_all_operators(self, tmp_db):
        """All 6 operators (<, >, <=, >=, ==, !=) work correctly."""
        import database
        from screener import check_custom_alerts

        rules = [
            {"field": "rsi", "op": "<", "value": 50, "label": "lt"},
            {"field": "rsi", "op": ">", "value": 20, "label": "gt"},
            {"field": "rsi", "op": "<=", "value": 40, "label": "le"},
            {"field": "rsi", "op": ">=", "value": 40, "label": "ge"},
            {"field": "rsi", "op": "==", "value": 40, "label": "eq"},
            {"field": "rsi", "op": "!=", "value": 99, "label": "ne"},
        ]
        _insert_watchlist(tmp_db, "TEST", user_id=1, alert_rules_json=json.dumps(rules))

        ranking = [
            {"ticker": "TEST", "name": "Test", "rank": 1, "momentum_score": 50,
             "technicals": {"rsi": 40}, "fundamentals": {}},
        ]
        events = check_custom_alerts(ranking, "sp500")
        triggered = {json.loads(e["payload_json"])["rule"] for e in events}
        # rsi=40: <50 ✓, >20 ✓, <=40 ✓, >=40 ✓, ==40 ✓, !=99 ✓
        assert triggered == {"lt", "gt", "le", "ge", "eq", "ne"}

    def test_alerts_resolve_flat_fields(self, tmp_db):
        """Fields at top level (momentum_score, quality_score) are resolved."""
        import database
        from screener import check_custom_alerts

        rules = [{"field": "momentum_score", "op": ">=", "value": 90, "label": "高スコア"}]
        _insert_watchlist(tmp_db, "NVDA", user_id=1, alert_rules_json=json.dumps(rules))

        ranking = [
            {"ticker": "NVDA", "name": "NVIDIA", "rank": 1, "momentum_score": 95,
             "technicals": {"rsi": 60}, "fundamentals": {}},
        ]
        events = check_custom_alerts(ranking, "sp500")
        assert len(events) == 1

    def test_alerts_no_rules_returns_empty(self, tmp_db):
        """No alert rules at all → empty events."""
        import database
        from screener import check_custom_alerts

        ranking = [{"ticker": "AAPL", "name": "Apple", "rank": 1, "momentum_score": 85,
                     "technicals": {"rsi": 50}, "fundamentals": {}}]
        events = check_custom_alerts(ranking, "sp500")
        assert events == []


# ══════════════════════════════════════════════════════════════════════════════
# Feature 1: Morning Brief
# ══════════════════════════════════════════════════════════════════════════════

class TestMorningBrief:
    def test_generate_morning_brief_basic(self):
        """generate_morning_brief produces expected structure."""
        from screener import generate_morning_brief

        daily_report = {
            "highlights": ["騰落比率 +65% — 広い上昇が継続中", "新規ランクイン: NVDA"],
            "initial_candidates": [
                {"ticker": "NVDA", "name": "NVIDIA", "score": 95, "reason": "BB圧縮ブレイク"},
            ],
            "streak_candidates": [
                {"ticker": "AAPL", "name": "Apple", "score": 88, "reason": "高スコア+高RS継続"},
            ],
            "caution_candidates": [
                {"ticker": "TSLA", "name": "Tesla", "score": 75, "reason": "RSI78.5"},
            ],
            "watchlist_alerts": ["MSFT: スコア急上昇 — +12pt → 90"],
        }
        regime = {"label": "強気"}
        brief = generate_morning_brief(daily_report, {}, regime, "S&P 500", "2026-04-10 06:00")

        assert brief["index_label"] == "S&P 500"
        assert brief["generated_at"] == "2026-04-10 06:00"
        assert brief["regime_label"] == "強気"
        assert len(brief["summary_lines"]) == 2
        assert len(brief["candidates"]) == 2
        assert brief["candidates"][0]["type"] == "initial"
        assert brief["candidates"][1]["type"] == "streak"
        assert len(brief["cautions"]) == 1
        assert brief["cautions"][0]["ticker"] == "TSLA"
        assert len(brief["watchlist_alerts"]) == 1

    def test_generate_morning_brief_empty_report(self):
        """Brief from empty daily_report still has regime fallback."""
        from screener import generate_morning_brief

        brief = generate_morning_brief({}, {}, {"label": "中立"}, "日経225", "2026-04-10 15:30")
        assert brief["regime_label"] == "中立"
        assert len(brief["summary_lines"]) == 1
        assert "中立" in brief["summary_lines"][0]
        assert brief["candidates"] == []
        assert brief["cautions"] == []
        assert brief["watchlist_alerts"] == []

    def test_generate_morning_brief_no_regime(self):
        """Brief with None regime defaults to '不明'."""
        from screener import generate_morning_brief

        brief = generate_morning_brief({}, {}, None, "S&P 500", "2026-04-10 06:00")
        assert brief["regime_label"] == "不明"

    def test_generate_morning_brief_limits(self):
        """Brief caps summary_lines=5, candidates=6, cautions=3, alerts=5."""
        from screener import generate_morning_brief

        daily_report = {
            "highlights": [f"ハイライト{i}" for i in range(10)],
            "initial_candidates": [{"ticker": f"T{i}", "name": f"N{i}", "score": 80, "reason": "r"} for i in range(5)],
            "streak_candidates": [{"ticker": f"S{i}", "name": f"S{i}", "score": 85, "reason": "r"} for i in range(5)],
            "caution_candidates": [{"ticker": f"C{i}", "name": f"C{i}", "score": 70, "reason": "r"} for i in range(5)],
            "watchlist_alerts": [f"alert{i}" for i in range(10)],
        }
        brief = generate_morning_brief(daily_report, {}, {"label": "強気"}, "Test", "2026-04-10")
        assert len(brief["summary_lines"]) <= 5
        assert len(brief["candidates"]) <= 6
        assert len(brief["cautions"]) <= 3
        assert len(brief["watchlist_alerts"]) <= 5

    def test_save_and_get_brief(self, tmp_db):
        """Brief round-trips through save → get_latest_briefs."""
        import database

        sid = _insert_session(tmp_db, index_name="sp500", generated_at="2026-04-10 06:00")
        brief_data = {
            "index_label": "S&P 500",
            "regime_label": "強気",
            "summary_lines": ["test highlight"],
            "candidates": [{"ticker": "NVDA", "type": "initial", "score": 95, "reason": "test"}],
            "cautions": [],
            "watchlist_alerts": [],
        }
        database.save_brief(sid, brief_data)

        briefs = database.get_latest_briefs(["sp500"])
        assert len(briefs) == 1
        b = briefs[0]
        assert b["index_label"] == "S&P 500"
        assert b["regime_label"] == "強気"
        assert b["generated_at"] == "2026-04-10 06:00"
        assert len(b["candidates"]) == 1

    def test_get_latest_briefs_per_index(self, tmp_db):
        """get_latest_briefs returns only the latest per index."""
        import database

        s1 = _insert_session(tmp_db, index_name="nikkei225", generated_at="2026-04-08 15:30")
        database.save_brief(s1, {"index_label": "日経225 old", "regime_label": "弱気"})

        s2 = _insert_session(tmp_db, index_name="nikkei225", generated_at="2026-04-09 15:30")
        database.save_brief(s2, {"index_label": "日経225 new", "regime_label": "強気"})

        s3 = _insert_session(tmp_db, index_name="growth250", generated_at="2026-04-09 15:30")
        database.save_brief(s3, {"index_label": "グロース250", "regime_label": "中立"})

        briefs = database.get_latest_briefs(["nikkei225", "growth250"], limit=2)
        assert len(briefs) == 2
        labels = {b["index_label"] for b in briefs}
        assert "日経225 new" in labels  # latest, not old
        assert "グロース250" in labels

    def test_get_latest_briefs_missing_index(self, tmp_db):
        """Index with no brief_json is skipped."""
        import database

        sid = _insert_session(tmp_db, index_name="sp500", generated_at="2026-04-10 06:00")
        # No brief saved for this session

        briefs = database.get_latest_briefs(["sp500", "nasdaq100"])
        assert len(briefs) == 0

    def test_brief_json_column_migration(self, tmp_db):
        """brief_json column exists after init_db (migration)."""
        conn = sqlite3.connect(tmp_db)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(screening_sessions)")}
        conn.close()
        assert "brief_json" in columns


# ══════════════════════════════════════════════════════════════════════════════
# Schema / Migration Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaMigrations:
    def test_alert_rules_json_column_exists(self, tmp_db):
        """alert_rules_json column exists on watchlist after init_db."""
        conn = sqlite3.connect(tmp_db)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(watchlist)")}
        conn.close()
        assert "alert_rules_json" in columns

    def test_screening_results_ticker_index_exists(self, tmp_db):
        """idx_screening_results_ticker index exists after init_db."""
        conn = sqlite3.connect(tmp_db)
        indexes = conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        conn.close()
        index_names = {row[0] for row in indexes}
        assert "idx_screening_results_ticker" in index_names

    def test_init_db_idempotent(self, tmp_db):
        """Calling init_db twice doesn't crash (idempotent migrations)."""
        import database
        database.init_db()  # second call
        database.init_db()  # third call
        # No exception = pass
