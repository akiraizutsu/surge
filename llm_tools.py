"""Tool definitions and implementations for Gemini function calling.

All tools are read-only wrappers around existing database/screener data.
Each tool has a declaration (schema) and an implementation function that
takes (args, user_id) and returns a dict result.

The 11 tools:
  Friend + Owner (8):
    1. get_ranking
    2. get_stock_detail
    3. filter_stocks
    4. get_market_regime
    5. compare_stocks
    6. find_similar_stocks
    7. get_cf_pattern_stocks (Japan only)
    8. get_sector_rotation

  Owner-only (3):
    9. get_collective_notes
    10. get_friends_activity_summary
    11. search_web_sentiment (Google Search grounding)
"""

import json
import math
import operator as op
from datetime import datetime, timedelta

import database
import notes_service


# ── Helpers ──────────────────────────────────────────────────────────────

def _get_index_results(index):
    """Return cached results for an index, or None."""
    all_results = database.get_latest_sessions_by_index()
    return all_results.get(index)


def _find_stock_everywhere(ticker):
    """Search all indices for a ticker, returning (index, stock_dict) or None."""
    all_results = database.get_latest_sessions_by_index()
    for idx, data in all_results.items():
        for r in (data or {}).get("momentum_ranking") or []:
            if r.get("ticker", "").upper() == ticker.upper():
                return idx, r
    return None, None


def _trim_stock(r, level="medium"):
    """Trim a full stock dict to reduce token usage when sending to LLM."""
    if not r:
        return None
    t = r.get("technicals") or {}
    f = r.get("fundamentals") or {}
    base = {
        "ticker": r.get("ticker"),
        "name": r.get("name"),
        "sector": r.get("sector"),
        "price": r.get("price"),
        "momentum_score": r.get("momentum_score"),
    }
    if level in ("medium", "full"):
        base.update({
            "rsi": t.get("rsi"),
            "ret_1m": t.get("ret_1m"),
            "ret_3m": t.get("ret_3m"),
            "vol_ratio": t.get("vol_ratio"),
            "ma50_dev": t.get("ma50_dev"),
            "adx": t.get("adx"),
            "obv_divergence": t.get("obv_divergence"),
            "max_drawdown_3m": t.get("max_drawdown_3m"),
            "is_breakout": t.get("is_breakout"),
            "golden_cross": t.get("golden_cross"),
            "rs_label": t.get("rs_label"),
            "quality_score": r.get("quality_score"),
            "entry_difficulty": r.get("entry_difficulty"),
        })
    if level == "full":
        base.update({
            "ret_1d": t.get("ret_1d"),
            "ret_1w": t.get("ret_1w"),
            "ma200_dev": t.get("ma200_dev"),
            "macd_hist_pct": t.get("macd_hist_pct"),
            "rs_1m": t.get("rs_1m"),
            "rs_3m": t.get("rs_3m"),
            "high_52w": t.get("high_52w"),
            "dist_from_high": t.get("dist_from_high"),
            "obv_slope": t.get("obv_slope"),
            "current_drawdown": t.get("current_drawdown"),
            "support_levels": t.get("support_levels"),
            "resistance_levels": t.get("resistance_levels"),
            "fundamentals": {
                "market_cap_b": f.get("market_cap_b"),
                "pe_trailing": f.get("pe_trailing"),
                "pe_forward": f.get("pe_forward"),
                "pb": f.get("pb"),
                "dividend_yield": f.get("dividend_yield"),
                "revenue_growth": f.get("revenue_growth"),
                "earnings_growth": f.get("earnings_growth"),
                "target_price": f.get("target_price"),
                "recommendation": f.get("recommendation"),
            },
        })
    return base


# ── Tool implementations ─────────────────────────────────────────────────

def _tool_get_ranking(args, user_id):
    index = args.get("index", "sp500")
    top_n = int(args.get("top_n", 10))
    top_n = max(1, min(top_n, 50))
    data = _get_index_results(index)
    if not data:
        return {"error": f"no data for index '{index}'. Run screening first."}
    ranking = data.get("momentum_ranking") or []
    trimmed = [_trim_stock(r, "medium") for r in ranking[:top_n]]
    return {
        "index": index,
        "generated_at": data.get("generated_at"),
        "total": len(ranking),
        "results": trimmed,
    }


def _tool_get_stock_detail(args, user_id):
    ticker = (args.get("ticker") or "").strip().upper()
    if not ticker:
        return {"error": "ticker is required"}
    idx, stock = _find_stock_everywhere(ticker)
    if not stock:
        return {"error": f"ticker '{ticker}' not found in latest screening results"}
    return {"index": idx, "stock": _trim_stock(stock, "full")}


_FILTER_OPS = {
    "<": op.lt, ">": op.gt, "<=": op.le, ">=": op.ge,
    "==": op.eq, "!=": op.ne,
}


def _get_field(stock, field):
    """Resolve a dotted field path, e.g. 'technicals.rsi' or 'momentum_score'."""
    if "." in field:
        parts = field.split(".")
        v = stock
        for p in parts:
            if isinstance(v, dict):
                v = v.get(p)
            else:
                return None
        return v
    # Check top-level first, then fall back to technicals/fundamentals
    if field in stock:
        return stock[field]
    t = stock.get("technicals") or {}
    if field in t:
        return t[field]
    f = stock.get("fundamentals") or {}
    if field in f:
        return f[field]
    return None


def _tool_filter_stocks(args, user_id):
    index = args.get("index", "sp500")
    conditions = args.get("conditions") or []
    limit = int(args.get("limit", 10))
    data = _get_index_results(index)
    if not data:
        return {"error": f"no data for index '{index}'"}
    ranking = data.get("momentum_ranking") or []
    matched = []
    for s in ranking:
        ok = True
        for c in conditions:
            field = c.get("field")
            comp = c.get("op")
            val = c.get("value")
            if not field or comp not in _FILTER_OPS:
                continue
            actual = _get_field(s, field)
            if actual is None:
                ok = False
                break
            try:
                if not _FILTER_OPS[comp](actual, val):
                    ok = False
                    break
            except Exception:
                ok = False
                break
        if ok:
            matched.append(s)
            if len(matched) >= limit:
                break
    return {
        "index": index,
        "total_matched": len(matched),
        "conditions": conditions,
        "results": [_trim_stock(s, "medium") for s in matched],
    }


def _tool_get_market_regime(args, user_id):
    index = args.get("index", "sp500")
    data = _get_index_results(index)
    if not data:
        return {"error": f"no data for index '{index}'"}
    return {
        "index": index,
        "regime": data.get("regime"),
        "latest_breadth": data.get("latest_breadth"),
        "summary": data.get("summary"),
    }


def _tool_compare_stocks(args, user_id):
    tickers = args.get("tickers") or []
    tickers = [t.upper() for t in tickers[:5]]
    if not tickers:
        return {"error": "at least one ticker required"}
    stocks = []
    for t in tickers:
        _, s = _find_stock_everywhere(t)
        if s:
            stocks.append(_trim_stock(s, "full"))
    if not stocks:
        return {"error": "no matching tickers found"}
    return {"stocks": stocks}


def _similarity_score(ref, candidate):
    """Compute a similarity score (0-1, higher = more similar) between two stocks."""
    score = 1.0
    if ref.get("sector") != candidate.get("sector"):
        score *= 0.5
    ref_t = ref.get("technicals") or {}
    cand_t = candidate.get("technicals") or {}
    ref_f = ref.get("fundamentals") or {}
    cand_f = candidate.get("fundamentals") or {}
    # Market cap log ratio
    rm = (ref_f.get("market_cap_b") or 0) + 1e-6
    cm = (cand_f.get("market_cap_b") or 0) + 1e-6
    log_ratio = abs(math.log(cm / rm)) if rm > 0 and cm > 0 else 2.0
    score /= (1 + log_ratio * 0.5)
    # Momentum diff
    rs = ref.get("momentum_score") or 0
    cs = candidate.get("momentum_score") or 0
    score /= (1 + abs(rs - cs) / 50)
    # ADX diff
    ra = ref_t.get("adx") or 0
    ca = cand_t.get("adx") or 0
    score /= (1 + abs(ra - ca) / 50)
    # RSI diff
    rr = ref_t.get("rsi") or 0
    cr = cand_t.get("rsi") or 0
    score /= (1 + abs(rr - cr) / 100)
    return score


def _tool_find_similar_stocks(args, user_id):
    ref_ticker = (args.get("reference_ticker") or "").upper()
    n = int(args.get("n", 5))
    n = max(1, min(n, 20))
    if not ref_ticker:
        return {"error": "reference_ticker required"}
    ref_idx, ref_stock = _find_stock_everywhere(ref_ticker)
    if not ref_stock:
        return {"error": f"reference ticker '{ref_ticker}' not found"}
    data = _get_index_results(ref_idx)
    ranking = data.get("momentum_ranking") or []
    scored = []
    for s in ranking:
        if s.get("ticker", "").upper() == ref_ticker:
            continue
        score = _similarity_score(ref_stock, s)
        scored.append((score, s))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:n]
    return {
        "reference": _trim_stock(ref_stock, "medium"),
        "similar_stocks": [
            {"similarity": round(score, 3), **(_trim_stock(s, "medium") or {})}
            for score, s in top
        ],
    }


def _tool_get_cf_pattern_stocks(args, user_id):
    """Japan-only: search EDINET cached financials for CF patterns."""
    ocf_growth_min = args.get("ocf_growth_min")
    fcf_positive = args.get("fcf_positive")
    capex_trend = args.get("capex_trend")  # 'growing' | 'stable' | 'declining'
    limit = int(args.get("limit", 10))

    # Get top-ranked Japan stocks
    jp_results = {}
    for idx in ("nikkei225", "growth250"):
        data = _get_index_results(idx)
        if data:
            for r in (data.get("momentum_ranking") or []):
                jp_results[r.get("ticker")] = r

    if not jp_results:
        return {"error": "no Japanese stock data available"}

    # Fetch cached EDINET financials
    import sqlite3
    conn = sqlite3.connect(database.DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT sec_code, latest_financials_json FROM edinet_company_cache WHERE latest_financials_json IS NOT NULL LIMIT 500"
    ).fetchall()
    conn.close()

    matched = []
    for row in rows:
        try:
            fin = json.loads(row["latest_financials_json"])
        except Exception:
            continue
        ocf = fin.get("ocf_latest") or 0
        fcf = fin.get("fcf_latest") or 0
        ocf_prev = fin.get("ocf_prev") or 0
        # OCF growth check
        if ocf_growth_min is not None:
            if ocf_prev <= 0:
                continue
            growth = (ocf - ocf_prev) / abs(ocf_prev) * 100
            if growth < ocf_growth_min:
                continue
        if fcf_positive is True and fcf <= 0:
            continue
        if fcf_positive is False and fcf > 0:
            continue
        ticker = f"{row['sec_code']}.T"
        stock = jp_results.get(ticker)
        if stock:
            matched.append({
                "ticker": ticker,
                "name": stock.get("name"),
                "sector": stock.get("sector"),
                "momentum_score": stock.get("momentum_score"),
                "ocf_latest": ocf,
                "fcf_latest": fcf,
                "ocf_prev": ocf_prev,
                "ocf_growth_pct": round((ocf - ocf_prev) / abs(ocf_prev) * 100, 1) if ocf_prev else None,
            })
        if len(matched) >= limit:
            break
    return {
        "total_matched": len(matched),
        "conditions": {
            "ocf_growth_min": ocf_growth_min,
            "fcf_positive": fcf_positive,
            "capex_trend": capex_trend,
        },
        "results": matched,
    }


def _tool_get_sector_rotation(args, user_id):
    index = args.get("index", "sp500")
    data = _get_index_results(index)
    if not data:
        return {"error": f"no data for index '{index}'"}
    return {
        "index": index,
        "sector_rotation": data.get("sector_rotation") or [],
    }


def _tool_get_collective_notes(args, user_id):
    """Owner only — return all users' notes."""
    user = database.get_user_by_id(user_id)
    if not user or user.get("role") != "owner":
        return {"error": "permission denied: owner only"}
    ticker = args.get("ticker")
    limit = int(args.get("limit", 20))
    notes = notes_service.get_collective_notes(ticker=ticker, limit=limit)
    # Trim answer content to keep token count reasonable
    trimmed = [{
        "id": n.get("id"),
        "title": n.get("title"),
        "author": n.get("author_name"),
        "created_at": n.get("created_at"),
        "tickers": n.get("tickers"),
        "tags": n.get("tags"),
        "question": (n.get("question") or "")[:200],
        "answer_excerpt": (n.get("answer") or "")[:500],
    } for n in notes]
    return {"total": len(trimmed), "notes": trimmed}


def _tool_get_friends_activity_summary(args, user_id):
    """Owner only — aggregate friends' research activity."""
    user = database.get_user_by_id(user_id)
    if not user or user.get("role") != "owner":
        return {"error": "permission denied: owner only"}
    days = int(args.get("days", 7))
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    # Get all notes (recent)
    notes = notes_service.get_collective_notes(limit=200)
    filtered = [n for n in notes if (n.get("created_at") or "") >= since]
    # Count by user and ticker
    by_user = {}
    ticker_counts = {}
    for n in filtered:
        author = n.get("author_name") or "unknown"
        by_user[author] = by_user.get(author, 0) + 1
        for t in (n.get("tickers") or []):
            ticker_counts[t] = ticker_counts.get(t, 0) + 1
    top_tickers = sorted(ticker_counts.items(), key=lambda x: -x[1])[:10]
    return {
        "days": days,
        "total_notes": len(filtered),
        "by_user": by_user,
        "top_tickers": [{"ticker": t, "mentions": c} for t, c in top_tickers],
    }


def _tool_search_web_sentiment(args, user_id):
    """Owner only — placeholder that instructs LLM to use web grounding.

    The actual grounding is enabled at the Gemini model level via
    tools=[{'google_search': {}}]. This tool just validates permission
    and returns a directive.
    """
    user = database.get_user_by_id(user_id)
    if not user or user.get("role") != "owner":
        return {"error": "permission denied: owner only"}
    query = args.get("query", "")
    return {
        "note": "Web search grounding is enabled at the model level for owner tier. Use your web_search capability to answer the query.",
        "query": query,
    }


# ── Tool declarations (Gemini function calling format) ──────────────────

def _decl(name, description, properties, required=None):
    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return {
        "name": name,
        "description": description,
        "parameters": schema,
    }


TOOL_DECLARATIONS = {
    "get_ranking": _decl(
        "get_ranking",
        "指定されたインデックスの最新モメンタムスクリーニング結果から上位N銘柄を取得する。",
        {
            "index": {"type": "string", "enum": ["sp500", "nasdaq100", "nikkei225", "growth250"], "description": "対象インデックス"},
            "top_n": {"type": "integer", "description": "取得する銘柄数（デフォルト10、最大50）"},
        },
        required=["index"],
    ),
    "get_stock_detail": _decl(
        "get_stock_detail",
        "特定の銘柄の全テクニカル指標・ファンダメンタルズ・サポレジを取得する。",
        {
            "ticker": {"type": "string", "description": "ティッカーシンボル（例: AAPL, 7203.T）"},
        },
        required=["ticker"],
    ),
    "filter_stocks": _decl(
        "filter_stocks",
        "複数の条件で銘柄を絞り込む。各条件は field/op/value の形式。使用可能フィールド: momentum_score, rsi, ret_1m, ret_3m, vol_ratio, ma50_dev, adx, max_drawdown_3m, quality_score, sector, market_cap_b",
        {
            "index": {"type": "string", "enum": ["sp500", "nasdaq100", "nikkei225", "growth250"]},
            "conditions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "field": {"type": "string"},
                        "op": {"type": "string", "enum": ["<", ">", "<=", ">=", "==", "!="]},
                        "value": {"type": "number"},
                    },
                },
            },
            "limit": {"type": "integer", "description": "最大返却件数（デフォルト10）"},
        },
        required=["index", "conditions"],
    ),
    "get_market_regime": _decl(
        "get_market_regime",
        "指定インデックスの現在の市場レジーム（強気/弱気/中立/分散）、ブレス、サマリーを取得する。",
        {"index": {"type": "string", "enum": ["sp500", "nasdaq100", "nikkei225", "growth250"]}},
        required=["index"],
    ),
    "compare_stocks": _decl(
        "compare_stocks",
        "2〜5銘柄の指標を並べて比較する。",
        {
            "tickers": {"type": "array", "items": {"type": "string"}, "description": "比較する銘柄リスト"},
        },
        required=["tickers"],
    ),
    "find_similar_stocks": _decl(
        "find_similar_stocks",
        "特定銘柄と類似する特徴を持つ銘柄を検索する。セクター・時価総額・モメンタム・ADXの類似度で評価。",
        {
            "reference_ticker": {"type": "string", "description": "基準銘柄"},
            "n": {"type": "integer", "description": "返却数（デフォルト5）"},
        },
        required=["reference_ticker"],
    ),
    "get_cf_pattern_stocks": _decl(
        "get_cf_pattern_stocks",
        "日本株のキャッシュフローパターンで銘柄を検索（EDINET データを使用）。日本株のみ有効。",
        {
            "ocf_growth_min": {"type": "number", "description": "前年比 OCF 成長率の最小値（%）"},
            "fcf_positive": {"type": "boolean", "description": "FCFがプラスか"},
            "capex_trend": {"type": "string", "enum": ["growing", "stable", "declining"]},
            "limit": {"type": "integer"},
        },
    ),
    "get_sector_rotation": _decl(
        "get_sector_rotation",
        "指定インデックスのセクターローテーション（セクター別平均リターン、加速/減速トレンド）を取得する。",
        {"index": {"type": "string", "enum": ["sp500", "nasdaq100", "nikkei225", "growth250"]}},
        required=["index"],
    ),
    # Owner only
    "get_collective_notes": _decl(
        "get_collective_notes",
        "[オーナー専用] 全ユーザーが作成した調査ノートを参照する。集合知として過去の分析を活用可能。",
        {
            "ticker": {"type": "string", "description": "特定銘柄で絞り込む場合"},
            "limit": {"type": "integer", "description": "最大件数（デフォルト20）"},
        },
    ),
    "get_friends_activity_summary": _decl(
        "get_friends_activity_summary",
        "[オーナー専用] 友人ユーザーの最近の調査活動サマリー（銘柄言及頻度、ユーザー別ノート数）。",
        {"days": {"type": "integer", "description": "遡る日数（デフォルト7）"}},
    ),
    "search_web_sentiment": _decl(
        "search_web_sentiment",
        "[オーナー専用] Google Search を使って最新のニュースやセンチメントを取得する。",
        {"query": {"type": "string"}},
        required=["query"],
    ),
}


TOOL_IMPLS = {
    "get_ranking": _tool_get_ranking,
    "get_stock_detail": _tool_get_stock_detail,
    "filter_stocks": _tool_filter_stocks,
    "get_market_regime": _tool_get_market_regime,
    "compare_stocks": _tool_compare_stocks,
    "find_similar_stocks": _tool_find_similar_stocks,
    "get_cf_pattern_stocks": _tool_get_cf_pattern_stocks,
    "get_sector_rotation": _tool_get_sector_rotation,
    "get_collective_notes": _tool_get_collective_notes,
    "get_friends_activity_summary": _tool_get_friends_activity_summary,
    "search_web_sentiment": _tool_search_web_sentiment,
}


def build_tool_declarations(tool_names):
    """Return a list of tool declaration dicts for the given tool names."""
    return [TOOL_DECLARATIONS[n] for n in tool_names if n in TOOL_DECLARATIONS]


def dispatch_tool(name, args, user_id):
    """Execute a tool by name with the given arguments and user context.

    Returns a JSON-serializable dict. Catches exceptions and returns
    them as {'error': ...} so the LLM can recover.
    """
    impl = TOOL_IMPLS.get(name)
    if not impl:
        return {"error": f"unknown tool: {name}"}
    try:
        return impl(args or {}, user_id)
    except Exception as e:
        return {"error": f"tool '{name}' failed: {type(e).__name__}: {e}"}
