"""Tests for notes_service pure helpers: extract_tickers and auto_generate_title."""

import notes_service


# ── extract_tickers ───────────────────────────────────────────────────────

def test_extract_tickers_none_returns_empty():
    assert notes_service.extract_tickers(None) == []


def test_extract_tickers_empty_string_returns_empty():
    assert notes_service.extract_tickers("") == []


def test_extract_tickers_dollar_prefix():
    result = notes_service.extract_tickers("Bought $AAPL today")
    assert "AAPL" in result


def test_extract_tickers_japanese_format():
    result = notes_service.extract_tickers("Nikkei winner 7203.T looks strong")
    assert "7203.T" in result


def test_extract_tickers_plain_uppercase():
    result = notes_service.extract_tickers("MSFT and GOOGL had great earnings")
    assert "MSFT" in result
    assert "GOOGL" in result


def test_extract_tickers_filters_blacklist():
    text = "The AI for USA investors should USE the API with RSI"
    result = notes_service.extract_tickers(text)
    assert "AI" not in result
    assert "USA" not in result
    assert "USE" not in result
    assert "API" not in result
    assert "RSI" not in result


def test_extract_tickers_dedupes_preserving_first_seen_order():
    result = notes_service.extract_tickers("$AAPL and AAPL and $MSFT again AAPL")
    assert result.index("AAPL") < result.index("MSFT")
    assert result.count("AAPL") == 1


def test_extract_tickers_caps_at_15():
    # Generate 20 distinct ticker-like strings
    tickers = [f"ZZ{chr(65+i)}" for i in range(20)]
    text = " ".join(f"${t}" for t in tickers)
    result = notes_service.extract_tickers(text)
    assert len(result) <= 15


# ── auto_generate_title ───────────────────────────────────────────────────

def test_auto_title_none_question_no_tickers():
    assert notes_service.auto_generate_title(None, []) == "調査ノート"


def test_auto_title_empty_question_returns_fallback():
    # empty string is falsy → same as None
    assert notes_service.auto_generate_title("", []) == "調査ノート"


def test_auto_title_short_question_no_tickers():
    result = notes_service.auto_generate_title("Quick question", [])
    assert result == "Quick question"


def test_auto_title_long_question_gets_truncated_with_ellipsis():
    long_q = "a" * 100
    result = notes_service.auto_generate_title(long_q, [])
    assert result.endswith("…")
    assert len(result) <= 42  # 40 chars + ellipsis


def test_auto_title_with_tickers_prefixes():
    result = notes_service.auto_generate_title("Check earnings", ["AAPL", "MSFT"])
    assert "AAPL" in result
    assert "MSFT" in result
    assert "Check earnings" in result


def test_auto_title_caps_tickers_at_two():
    result = notes_service.auto_generate_title("Check", ["AAPL", "MSFT", "GOOGL"])
    assert "AAPL" in result
    assert "MSFT" in result
    assert "GOOGL" not in result


def test_auto_title_newline_in_question_becomes_space():
    result = notes_service.auto_generate_title("Line1\nLine2", [])
    assert "\n" not in result
    assert "Line1 Line2" in result


def test_auto_title_tickers_only_when_no_meaningful_question():
    result = notes_service.auto_generate_title(None, ["AAPL"])
    assert result == "AAPL"
