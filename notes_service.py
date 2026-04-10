"""Research notes service for Surge v2.

Notes are per-user investigation logs created from LLM chat answers.
Friends only see their own notes. The owner can access all users' notes
for collective knowledge.
"""

import re

import database


# Regex for ticker extraction. Matches:
#   $AAPL   (with dollar sign)
#   AAPL    (2-5 uppercase letters, word boundary)
#   7203.T  (Japanese stocks)
_TICKER_PATTERNS = [
    re.compile(r"\$([A-Z]{1,5})\b"),
    re.compile(r"\b([0-9]{4}\.T)\b"),
    re.compile(r"\b([A-Z]{2,5})\b"),  # generic fallback
]

# Common English words that look like tickers but aren't — filter these out
_TICKER_BLACKLIST = {
    "AI", "API", "AND", "THE", "FOR", "YOU", "ARE", "NOT", "BUT", "ALL",
    "CAN", "NOW", "TOP", "END", "USE", "USD", "JPY", "EUR", "GDP", "CEO",
    "CFO", "CTO", "USA", "JP", "US", "UK", "EU", "IT", "IS", "OK", "NG",
    "ON", "OFF", "UP", "DOWN", "BUY", "SELL", "HOLD", "RSI", "MA", "MACD",
    "EPS", "PER", "PBR", "ROE", "ROI", "ADX", "OBV", "ATR", "BB", "DD",
    "ETF", "SPY", "QQQ", "VIX", "CF", "FCF", "OCF", "ESG", "IPO", "M&A",
    "SNS", "IR", "PR", "PC", "PCI", "HDD", "SSD", "RAM", "CPU", "GPU",
    "NEW", "OLD", "HIGH", "LOW", "MID", "BIG", "IDEAL",
}


def extract_tickers(text):
    """Extract probable stock tickers from text.

    Returns a deduplicated list preserving first-seen order.
    """
    if not text:
        return []

    seen = []
    def _add(t):
        if t and t not in seen and t not in _TICKER_BLACKLIST:
            seen.append(t)

    # $AAPL style first
    for m in _TICKER_PATTERNS[0].finditer(text):
        _add(m.group(1).upper())
    # 7203.T style
    for m in _TICKER_PATTERNS[1].finditer(text):
        _add(m.group(1).upper())
    # Plain 2-5 letter uppercase (more aggressive, filtered by blacklist)
    for m in _TICKER_PATTERNS[2].finditer(text):
        _add(m.group(1).upper())

    return seen[:15]  # cap to avoid false positive explosion


# ── Public API ────────────────────────────────────────────────────────────

def create_note(user_id, title, question, answer,
                tickers=None, tags=None, index_name=None,
                llm_model=None, tool_calls=None):
    """Create a new research note for a user.

    If tickers is None, auto-extracts from question + answer.
    Returns the new note id.
    """
    if not title or not answer:
        return None
    if tickers is None:
        combined = f"{question or ''}\n{answer}"
        tickers = extract_tickers(combined)
    return database.insert_note(
        user_id=user_id,
        title=title,
        question=question,
        answer=answer,
        tickers=tickers or [],
        tags=tags or [],
        index_name=index_name,
        llm_model=llm_model,
        tool_calls=tool_calls or [],
    )


def list_user_notes(user_id, ticker=None, pinned_only=False, limit=50):
    """List notes owned by a user, filtered optionally by ticker or pin."""
    return database.get_notes_by_user(
        user_id=user_id,
        ticker=ticker,
        pinned_only=pinned_only,
        limit=limit,
    )


def get_note(note_id, user_id):
    """Return a single note, enforcing ownership."""
    return database.get_note_by_id(note_id, user_id=user_id)


def update_note(note_id, user_id, **fields):
    """Update allowed fields on a note. Ownership enforced in DB layer."""
    return database.update_note_fields(note_id, user_id, **fields)


def delete_note(note_id, user_id):
    return database.delete_note_by_id(note_id, user_id)


def toggle_pin(note_id, user_id):
    return database.toggle_note_pin(note_id, user_id)


def get_collective_notes(ticker=None, limit=30):
    """Owner-only: return notes from ALL users, attributed with author name."""
    return database.get_all_notes(ticker=ticker, limit=limit)


def auto_generate_title(question, tickers):
    """Auto-generate a short title from question + tickers."""
    if not question:
        base = "調査ノート"
    else:
        base = question.strip().replace("\n", " ")[:40]
        if len(question) > 40:
            base = base.rstrip() + "…"
    if tickers:
        prefix = "_".join(tickers[:2])
        return f"{prefix}_{base}" if base != "調査ノート" else prefix
    return base
