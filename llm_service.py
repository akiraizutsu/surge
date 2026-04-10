"""LLM service layer for Surge v2.

Wraps the google-genai SDK to provide:
- Tier-aware chat (Friend vs Owner)
- Tool Use orchestration (function calling)
- Streaming NDJSON events
- Per-user rate limiting integration
- Pro mode (Gemini 2.5 Pro escalation, owner only)
"""

import os
from datetime import datetime

from google import genai
from google.genai import types

import auth_service
import llm_tools
import rate_limit_service


# ── Tier configuration ──────────────────────────────────────────────────

FRIEND_TOOLS = [
    "get_ranking",
    "get_stock_detail",
    "filter_stocks",
    "get_market_regime",
    "compare_stocks",
    "find_similar_stocks",
    "get_cf_pattern_stocks",
    "get_sector_rotation",
]

OWNER_ONLY_TOOLS = [
    "get_collective_notes",
    "get_friends_activity_summary",
    "search_web_sentiment",
]

OWNER_TOOLS = FRIEND_TOOLS + OWNER_ONLY_TOOLS

DEFAULT_MODEL = "gemini-2.5-flash"
ADVANCED_MODEL = "gemini-2.5-pro"


# ── Client singleton ────────────────────────────────────────────────────

_client = None


def get_client():
    global _client
    if _client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not set in environment")
        _client = genai.Client(api_key=api_key)
    return _client


# ── System prompts ──────────────────────────────────────────────────────

FRIEND_SYSTEM_PROMPT = """あなたは Surge v2 というモメンタムスクリーニングダッシュボードのAIアナリストアシスタントです。

ユーザーは株式投資の調査を行なっており、あなたは以下のデータツールを使ってその調査を支援します:
- S&P 500 / NASDAQ 100 / 日経225 / グロース250 の最新スクリーニング結果
- モメンタムスコア、RSI、MACD、ADX、OBV、ドローダウン、サポレジなどのテクニカル指標
- セクターローテーション、市場レジーム、類似銘柄検索
- キャッシュフローパターン検索（日本株のみ）

## 原則
- **ツールを積極的に使う**: ユーザーの質問は必ず最新データを参照して回答する。推測や一般論だけで答えない。
- **簡潔に**: 冗長な説明は避け、データに基づく結論を先に述べる。必要に応じて箇条書きを使う。
- **銘柄コードを明示**: 言及する銘柄は必ず $AAPL や 7203.T のような形式でティッカーを示す。
- **免責**: 「投資判断は自己責任」という注意を過剰に繰り返さない。プロのアナリストらしく端的に述べる。
- **日本語で**: 回答は日本語で。
- **失敗時**: ツールがエラーを返した場合は、代替の方法を試すか、ユーザーに追加情報を求める。

## 利用可能なインデックス
- `sp500` — S&P 500 (米国大型株)
- `nasdaq100` — NASDAQ 100 (米国テック中心)
- `nikkei225` — 日経225 (日本大型株)
- `growth250` — グロース250 (日本グロース株)
"""

OWNER_SYSTEM_PROMPT = FRIEND_SYSTEM_PROMPT + """

## オーナー追加機能
あなたは現在 **オーナー権限** のアナリストとして動作しています。以下の追加ツールが利用可能:
- `get_collective_notes`: 全ユーザーが作成した過去の調査ノートを参照できる（集合知として活用）
- `get_friends_activity_summary`: 友人ユーザーの最近の調査活動を把握できる
- `search_web_sentiment`: Google 検索による最新ニュース・センチメント取得

ユーザー（AKIRA）は友人3人とシステムを共有しているため、他ユーザーの調査を参考にして回答に深みを加えてください。
"""


# ── Analyst AI class ────────────────────────────────────────────────────

class AnalystAI:
    def __init__(self, user_id):
        self.user_id = user_id
        self.user = auth_service.get_user(user_id)
        if not self.user:
            raise ValueError(f"user {user_id} not found")
        self.is_owner = self.user.get("role") == "owner"

    def _tool_names(self):
        return OWNER_TOOLS if self.is_owner else FRIEND_TOOLS

    def _system_prompt(self):
        return OWNER_SYSTEM_PROMPT if self.is_owner else FRIEND_SYSTEM_PROMPT

    def _build_tools_config(self, use_pro):
        """Build the tools argument for Gemini client."""
        tool_decls = llm_tools.build_tool_declarations(self._tool_names())
        # Convert plain dicts to google-genai FunctionDeclaration
        function_decls = [
            types.FunctionDeclaration(**decl) for decl in tool_decls
        ]
        tools = [types.Tool(function_declarations=function_decls)]
        return tools

    def chat_stream(self, message, history=None, use_pro=False):
        """Stream chat events. Generator yielding dicts:

            {'type': 'text', 'content': str}
            {'type': 'tool_call', 'name': str, 'args': dict}
            {'type': 'tool_result', 'name': str, 'result': dict}
            {'type': 'usage', 'tokens_in': int, 'tokens_out': int, 'cost_usd': float}
            {'type': 'done'}
            {'type': 'error', 'error': str}
        """
        history = history or []

        # Rate limit check
        allowed, reason = rate_limit_service.check_rate_limit(self.user_id)
        if not allowed:
            yield {"type": "error", "error": reason}
            return

        model = ADVANCED_MODEL if (use_pro and self.is_owner) else DEFAULT_MODEL

        try:
            client = get_client()
        except Exception as e:
            yield {"type": "error", "error": f"LLM client init failed: {e}"}
            return

        # Build contents from history + new message
        contents = []
        for msg in history[-10:]:  # last 5 exchanges to keep context small
            role = msg.get("role", "user")
            if role not in ("user", "model"):
                continue
            contents.append(
                types.Content(
                    role=role,
                    parts=[types.Part.from_text(text=msg.get("content", ""))],
                )
            )
        contents.append(
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=message)],
            )
        )

        tools = self._build_tools_config(use_pro)
        config = types.GenerateContentConfig(
            system_instruction=self._system_prompt(),
            tools=tools,
            temperature=0.4,
            max_output_tokens=4000 if self.is_owner else 1500,
        )

        # Function-calling loop (up to 6 rounds to avoid infinite loops)
        total_tokens_in = 0
        total_tokens_out = 0
        final_text = ""

        for round_idx in range(6):
            # Retry on transient 503/429 errors
            import time
            response = None
            last_error = None
            for retry in range(3):
                try:
                    response = client.models.generate_content(
                        model=model,
                        contents=contents,
                        config=config,
                    )
                    break
                except Exception as e:
                    last_error = e
                    err_str = str(e)
                    if "503" in err_str or "UNAVAILABLE" in err_str or "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        time.sleep(1.5 * (retry + 1))
                        continue
                    break
            if response is None:
                yield {"type": "error", "error": f"LLM call failed: {last_error}"}
                return

            # Accumulate usage
            if response.usage_metadata:
                total_tokens_in += (response.usage_metadata.prompt_token_count or 0)
                total_tokens_out += (response.usage_metadata.candidates_token_count or 0)

            if not response.candidates:
                yield {"type": "error", "error": "empty response from model"}
                return

            candidate = response.candidates[0]
            content = candidate.content
            parts = content.parts or []

            # Check for tool calls first
            function_calls = []
            text_parts = []
            for part in parts:
                if hasattr(part, "function_call") and part.function_call:
                    function_calls.append(part.function_call)
                elif hasattr(part, "text") and part.text:
                    text_parts.append(part.text)

            # If model returned text, stream it
            if text_parts:
                joined = "".join(text_parts)
                final_text += joined
                yield {"type": "text", "content": joined}

            # If model requested function calls, execute and loop
            if function_calls:
                # Append the model's turn (with function calls)
                contents.append(content)
                function_response_parts = []
                for fc in function_calls:
                    fn_name = fc.name
                    fn_args = dict(fc.args) if fc.args else {}
                    yield {"type": "tool_call", "name": fn_name, "args": fn_args}
                    result = llm_tools.dispatch_tool(fn_name, fn_args, self.user_id)
                    yield {"type": "tool_result", "name": fn_name, "result": result}
                    function_response_parts.append(
                        types.Part.from_function_response(
                            name=fn_name,
                            response={"result": result},
                        )
                    )
                # Append tool results as a user turn
                contents.append(
                    types.Content(role="user", parts=function_response_parts)
                )
                continue  # loop again to get model's response to tool results

            # No function calls — done
            break

        # Record usage
        cost = rate_limit_service.calculate_cost(model, total_tokens_in, total_tokens_out)
        rate_limit_service.record_usage(
            self.user_id, model, total_tokens_in, total_tokens_out
        )
        yield {
            "type": "usage",
            "tokens_in": total_tokens_in,
            "tokens_out": total_tokens_out,
            "cost_usd": round(cost, 6),
            "model": model,
        }
        yield {"type": "done", "final_text": final_text}
