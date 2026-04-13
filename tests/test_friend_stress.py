"""Friend user stress test — Gemini free tier rate limit endurance.

Tests what a Friend user can realistically do within the free tier limits.
Run with: python3 tests/test_friend_stress.py

Tests progressively harder scenarios:
1. Simple question (single tool)
2. Multi-tool question (2-3 tools in one chat)
3. Rapid consecutive questions (back-to-back)
4. Agent mode hypothesis verification
5. Sustained conversation (5 questions in sequence)
"""

import json
import os
import sys
import time

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


# ── Config ───────────────────────────────────────────────────────────────────

FRIEND_USER_ID = 2  # friend1

# How long to wait between tests to stay within rate limits
INTER_TEST_WAIT = 35  # seconds


# ── Helpers ──────────────────────────────────────────────────────────────────

def run_chat(message, label, history=None, agent_mode=False):
    """Run a single chat request and return structured result."""
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"  Q: {message[:80]}{'...' if len(message) > 80 else ''}")
    print(f"  Mode: {'agent' if agent_mode else 'chat'}")
    print(f"{'─'*50}")

    ai = AnalystAI(user_id=FRIEND_USER_ID)
    events = []
    text = ""
    tool_calls = []
    errors = []
    usage = None
    t0 = time.time()

    try:
        if agent_mode:
            market = "jp"
            market_label = "日本株（日経225・グロース250）"
            full_msg = f"以下の投資仮説を検証してください。\n\n対象市場: {market_label}\n\n仮説: {message}"
            gen = ai.chat_stream(full_msg, history=[], agent_mode=True)
        else:
            gen = ai.chat_stream(message, history=history or [])

        for chunk in gen:
            events.append(chunk)
            t = chunk.get("type")
            if t == "text":
                text += chunk.get("content", "")
            elif t == "tool_call":
                tool_calls.append(chunk.get("name"))
                print(f"    🔧 {chunk.get('name')}")
            elif t == "error":
                errors.append(chunk.get("error", ""))
                print(f"    ⚠️ {chunk['error']}")
            elif t == "usage":
                usage = chunk
    except Exception as e:
        errors.append(str(e))
        print(f"    ❌ Exception: {e}")

    elapsed = round(time.time() - t0, 1)
    status = "FAIL" if errors else "OK"
    text_len = len(text)
    cost = usage.get("cost_usd", 0) if usage else 0

    print(f"    {'✅' if not errors else '❌'} {elapsed}s | {text_len} chars | {len(tool_calls)} tools | ${cost:.4f}")
    if text_len > 0:
        print(f"    📝 {text[:100]}{'...' if text_len > 100 else ''}")

    return {
        "label": label,
        "status": status,
        "elapsed": elapsed,
        "text_len": text_len,
        "tool_calls": tool_calls,
        "tool_count": len(tool_calls),
        "errors": errors,
        "cost": cost,
        "tokens_in": usage.get("tokens_in", 0) if usage else 0,
        "tokens_out": usage.get("tokens_out", 0) if usage else 0,
        "model": usage.get("model", "?") if usage else "?",
    }


# ── Test Cases ───────────────────────────────────────────────────────────────

def test_1_simple_question():
    """Single tool: get_ranking"""
    return run_chat("日経225のモメンタム上位3銘柄を教えて", "Test 1: 単純質問 (1ツール)")


def test_2_multi_tool():
    """Should use 2-3 tools: ranking + detail or comparison"""
    return run_chat(
        "S&P500の1位の銘柄と日経225の1位の銘柄を比較して",
        "Test 2: 複数ツール質問 (2-3ツール)"
    )


def test_3_rapid_fire():
    """Two questions with only 5s gap — will the second hit 429?"""
    r1 = run_chat("NASDAQ100のレジームは？", "Test 3a: 連続質問1/2")
    print("    ⏳ 5秒待機...")
    time.sleep(5)
    r2 = run_chat("グロース250のレジームは？", "Test 3b: 連続質問2/2")
    return [r1, r2]


def test_4_agent_mode():
    """Agent mode hypothesis — uses chat_stream with agent prompt"""
    return run_chat(
        "日経225の上位銘柄はRSIが過熱圏にある",
        "Test 4: 仮説検証モード",
        agent_mode=True,
    )


def test_5_sustained():
    """5-turn conversation simulating real usage"""
    results = []
    history = []
    questions = [
        "日経225の今の地合いを教えて",
        "上位5銘柄のRSIは？",
        "その中で出来高が一番増えてるのは？",
        "セクターローテーションの状況は？",
        "総合的に今は買い場？",
    ]
    for i, q in enumerate(questions):
        r = run_chat(q, f"Test 5-{i+1}: 連続会話 ({i+1}/5)", history=history)
        results.append(r)
        if r["text_len"] > 0:
            history.append({"role": "user", "content": q})
            history.append({"role": "model", "content": "(response)"})
        if r["errors"]:
            print(f"    ⚠️ 会話中断 — 429エラーのため残りスキップ")
            break
        if i < len(questions) - 1:
            wait = 15
            print(f"    ⏳ {wait}秒待機...")
            time.sleep(wait)
    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  SURGE v2 — Friend User Stress Test")
    print(f"  User: friend1 (id={FRIEND_USER_ID})")
    print(f"  Gemini Free Tier: 5 req/min per model")
    print("=" * 60)

    all_results = []

    # Test 1
    r = test_1_simple_question()
    all_results.append(r)
    print(f"\n  ⏳ {INTER_TEST_WAIT}秒クールダウン...")
    time.sleep(INTER_TEST_WAIT)

    # Test 2
    r = test_2_multi_tool()
    all_results.append(r)
    print(f"\n  ⏳ {INTER_TEST_WAIT}秒クールダウン...")
    time.sleep(INTER_TEST_WAIT)

    # Test 3 (rapid fire)
    results_3 = test_3_rapid_fire()
    all_results.extend(results_3 if isinstance(results_3, list) else [results_3])
    print(f"\n  ⏳ {INTER_TEST_WAIT}秒クールダウン...")
    time.sleep(INTER_TEST_WAIT)

    # Test 4 (agent)
    r = test_4_agent_mode()
    all_results.append(r)
    print(f"\n  ⏳ {INTER_TEST_WAIT}秒クールダウン...")
    time.sleep(INTER_TEST_WAIT)

    # Test 5 (sustained)
    results_5 = test_5_sustained()
    all_results.extend(results_5)

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n\n" + "=" * 60)
    print("  STRESS TEST RESULTS")
    print("=" * 60)

    total_ok = sum(1 for r in all_results if r["status"] == "OK")
    total_fail = sum(1 for r in all_results if r["status"] == "FAIL")
    total_cost = sum(r["cost"] for r in all_results)
    total_tokens_in = sum(r["tokens_in"] for r in all_results)
    total_tokens_out = sum(r["tokens_out"] for r in all_results)

    print(f"\n  {'Label':<35} {'Status':>6} {'Time':>6} {'Tools':>5} {'Cost':>8} {'Model':<20}")
    print(f"  {'─'*35} {'─'*6} {'─'*6} {'─'*5} {'─'*8} {'─'*20}")
    for r in all_results:
        icon = "✅" if r["status"] == "OK" else "❌"
        print(f"  {r['label']:<35} {icon:>6} {r['elapsed']:>5.1f}s {r['tool_count']:>5} ${r['cost']:>7.4f} {r['model']:<20}")
        if r["errors"]:
            for e in r["errors"]:
                print(f"    └ ⚠️ {e[:70]}")

    print(f"\n  {'─'*60}")
    print(f"  ✅ OK: {total_ok}  ❌ FAIL: {total_fail}")
    print(f"  💰 Total cost: ${total_cost:.4f}")
    print(f"  📊 Total tokens: {total_tokens_in} in / {total_tokens_out} out")
    print(f"  {'─'*60}")

    # Rate limit assessment
    print(f"\n  📋 Free Tier 耐久評価:")
    if total_fail == 0:
        print(f"  ✅ 全テスト成功 — Free tier で十分運用可能")
    elif total_fail <= 2:
        print(f"  ⚠️ 一部失敗 — 連続使用時に間隔を空ける必要あり")
    else:
        print(f"  ❌ 多数失敗 — Free tier では厳しい。有料プラン推奨")

    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
