"""Live integration tests for the hypothesis investigation agent.

These tests call the real Gemini API — they require GEMINI_API_KEY in .env.
Run with: python3 tests/test_agent_live.py

Each test verifies:
1. Agent produces a plan
2. Agent executes tool calls (steps)
3. Agent concludes with a verdict
4. Conclusion is saved to research notes
5. No internal tool names leak into text output
"""

import json
import os
import sys
import time

# Load .env
_dotenv = os.path.join(os.path.dirname(__file__), "..", ".env")
if os.path.exists(_dotenv):
    with open(_dotenv) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ[k.strip()] = v.strip()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import database
database.init_db()

from llm_service import AnalystAI
import notes_service

# ── Helpers ──────────────────────────────────────────────────────────────────

INTERNAL_TOOL_NAMES = [
    "get_ranking", "get_stock_detail", "get_stock_live", "filter_stocks",
    "get_market_regime", "compare_stocks", "find_similar_stocks",
    "get_cf_pattern_stocks", "get_sector_rotation", "search_web_sentiment",
    "conclude_investigation", "get_collective_notes", "get_friends_activity_summary",
]


def get_test_user_id():
    """Get or create a test user for agent runs."""
    conn = database._connect()
    row = conn.execute("SELECT id FROM users LIMIT 1").fetchone()
    conn.close()
    if row:
        return row["id"]
    raise RuntimeError("No users in DB — seed with SURGE_USERS or run app.py first")


def run_agent_test(hypothesis, market, label):
    """Run one agent test case and return structured results."""
    print(f"\n{'='*60}")
    print(f"TEST: {label}")
    print(f"  Hypothesis: {hypothesis}")
    print(f"  Market: {market}")
    print(f"{'='*60}")

    user_id = get_test_user_id()
    ai = AnalystAI(user_id=user_id)

    events = []
    plan_text = ""
    steps = []
    conclusion = None
    errors = []
    text_chunks = []
    usage = None

    try:
        for chunk in ai.run_agent(hypothesis, market=market):
            events.append(chunk)
            t = chunk.get("type")

            if t == "plan":
                plan_text = chunk.get("content", "")
                print(f"  📝 Plan: {plan_text[:100]}...")
            elif t == "step":
                step_num = chunk.get("step", "?")
                tool = chunk.get("tool", "?")
                steps.append({"step": step_num, "tool": tool})
                print(f"  🔄 Step {step_num}: {tool}")
            elif t == "step_result":
                step_num = chunk.get("step", "?")
                print(f"  ✅ Step {step_num} done")
            elif t == "text":
                text_chunks.append(chunk.get("content", ""))
            elif t == "conclusion":
                conclusion = chunk
                print(f"  📋 Conclusion: {chunk.get('verdict_label', chunk.get('verdict'))}")
                print(f"     Title: {chunk.get('title', '')}")
                print(f"     Summary: {(chunk.get('summary', ''))[:120]}...")
                ev_count = len(chunk.get("evidence", []))
                print(f"     Evidence: {ev_count} items")
                if chunk.get("note_id"):
                    print(f"     Note ID: {chunk['note_id']}")
            elif t == "usage":
                usage = chunk
                print(f"  💰 Tokens: {chunk.get('tokens_in', 0)} in / {chunk.get('tokens_out', 0)} out / ${chunk.get('cost_usd', 0):.4f}")
            elif t == "error":
                errors.append(chunk.get("error", ""))
                print(f"  ⚠️ Error: {chunk['error']}")
            elif t == "done":
                print(f"  ✅ Done")

    except Exception as e:
        errors.append(str(e))
        print(f"  ❌ Exception: {e}")

    return {
        "label": label,
        "events": events,
        "plan_text": plan_text,
        "steps": steps,
        "conclusion": conclusion,
        "errors": errors,
        "text_chunks": text_chunks,
        "usage": usage,
    }


def validate_result(result):
    """Validate a test result and return (pass_count, fail_count, details)."""
    checks = []
    label = result["label"]

    # 1. No errors
    if result["errors"]:
        checks.append(("FAIL", f"Errors occurred: {result['errors']}"))
    else:
        checks.append(("PASS", "No errors"))

    # 2. Plan was generated
    if result["plan_text"]:
        checks.append(("PASS", f"Plan generated ({len(result['plan_text'])} chars)"))
    else:
        checks.append(("WARN", "No plan text (may have been in first step)"))

    # 3. At least 2 tool calls
    step_count = len(result["steps"])
    if step_count >= 2:
        checks.append(("PASS", f"{step_count} steps executed"))
    elif step_count >= 1:
        checks.append(("WARN", f"Only {step_count} step (expected 2+)"))
    else:
        checks.append(("FAIL", "No steps executed"))

    # 4. Conclusion reached
    if result["conclusion"]:
        verdict = result["conclusion"].get("verdict")
        valid_verdicts = {"supported", "partially_supported", "not_supported", "inconclusive"}
        if verdict in valid_verdicts:
            checks.append(("PASS", f"Valid verdict: {verdict}"))
        else:
            checks.append(("FAIL", f"Invalid verdict: {verdict}"))

        # Check evidence
        evidence = result["conclusion"].get("evidence", [])
        if len(evidence) >= 1:
            checks.append(("PASS", f"{len(evidence)} evidence items"))
        else:
            checks.append(("WARN", "No evidence items"))

        # Check note saved
        note_id = result["conclusion"].get("note_id")
        if note_id:
            checks.append(("PASS", f"Note saved (ID: {note_id})"))
        else:
            checks.append(("FAIL", "Note not saved"))

        # Check related tickers
        tickers = result["conclusion"].get("related_tickers", [])
        if tickers:
            checks.append(("PASS", f"Related tickers: {', '.join(tickers[:5])}"))
        else:
            checks.append(("WARN", "No related tickers"))
    else:
        checks.append(("FAIL", "No conclusion reached"))

    # 5. No internal tool names leaked in text output
    all_text = result["plan_text"] + " ".join(result["text_chunks"])
    if result["conclusion"]:
        all_text += " " + (result["conclusion"].get("summary", ""))
    leaked = [t for t in INTERNAL_TOOL_NAMES if t in all_text]
    if leaked:
        checks.append(("FAIL", f"Internal tool names leaked: {leaked}"))
    else:
        checks.append(("PASS", "No internal tool names in output"))

    # 6. Usage recorded
    if result["usage"]:
        checks.append(("PASS", f"Usage tracked: ${result['usage'].get('cost_usd', 0):.4f}"))
    else:
        checks.append(("WARN", "No usage data"))

    return checks


# ── Test Cases ───────────────────────────────────────────────────────────────

TEST_CASES = [
    {
        "hypothesis": "日経225の中で建設セクターに資金が集まっている",
        "market": "jp",
        "label": "JP: セクターローテーション仮説",
    },
    {
        "hypothesis": "S&P 500の上位銘柄はRSIが過熱圏にある",
        "market": "us",
        "label": "US: 過熱判定仮説",
    },
    {
        "hypothesis": "グロース市場のモメンタムスコア上位銘柄は出来高が増えている",
        "market": "jp",
        "label": "JP: 出来高確認仮説",
    },
]


def main():
    print("\n" + "=" * 60)
    print("  SURGE v2 — Agent Live Integration Tests")
    print("=" * 60)

    results = []
    for i, tc in enumerate(TEST_CASES):
        result = run_agent_test(tc["hypothesis"], tc["market"], tc["label"])
        results.append(result)
        # Rate limit: wait between tests to avoid 429
        if i < len(TEST_CASES) - 1:
            print("\n  ⏳ Waiting 35s for rate limit cooldown...")
            time.sleep(35)

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n\n" + "=" * 60)
    print("  TEST RESULTS SUMMARY")
    print("=" * 60)

    total_pass = 0
    total_fail = 0
    total_warn = 0

    for result in results:
        checks = validate_result(result)
        print(f"\n  📋 {result['label']}")
        for status, detail in checks:
            icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️"}[status]
            print(f"    {icon} {detail}")
            if status == "PASS":
                total_pass += 1
            elif status == "FAIL":
                total_fail += 1
            else:
                total_warn += 1

    print(f"\n  {'='*40}")
    print(f"  ✅ PASS: {total_pass}  ❌ FAIL: {total_fail}  ⚠️ WARN: {total_warn}")
    print(f"  {'='*40}")

    # Clean up test notes
    user_id = get_test_user_id()
    for result in results:
        if result["conclusion"] and result["conclusion"].get("note_id"):
            try:
                notes_service.delete_note(result["conclusion"]["note_id"], user_id)
                print(f"  🗑️ Cleaned up note #{result['conclusion']['note_id']}")
            except Exception:
                pass

    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
