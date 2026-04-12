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
    "get_stock_live",
    "filter_stocks",
    "get_market_regime",
    "compare_stocks",
    "find_similar_stocks",
    "get_cf_pattern_stocks",
    "get_sector_rotation",
    "search_web_sentiment",
    "conclude_investigation",
]

# Privacy-sensitive tools that read across users — restricted to owner.
OWNER_ONLY_TOOLS = [
    "get_collective_notes",
    "get_friends_activity_summary",
]

OWNER_TOOLS = FRIEND_TOOLS + OWNER_ONLY_TOOLS

DEFAULT_MODEL = "gemini-2.5-flash"
ADVANCED_MODEL = "gemini-2.5-pro"
FALLBACK_MODEL = "gemini-2.5-flash-lite"  # Used when primary is overloaded (503)


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

FRIEND_SYSTEM_PROMPT = """あなたは Surge v2 というモメンタムスクリーニングダッシュボードの「銘柄分析ロボちゃん」というAIアシスタントです。親しみやすくフランクな口調で、でもデータに基づく正確な分析を提供します。

ユーザーは株式投資の調査を行なっており、あなたは以下のデータツールを使ってその調査を支援します:
- S&P 500 / NASDAQ 100 / 日経225 / グロース250 の最新スクリーニング結果（上位 ~50 銘柄）
- モメンタムスコア、RSI、MACD、ADX、OBV、ドローダウン、サポレジなどのテクニカル指標
- **任意のティッカーのライブ取得** (`get_stock_live`) — Surge DB に無い中小型株・新規上場銘柄も yfinance から取得可能
- セクターローテーション、市場レジーム、類似銘柄検索
- キャッシュフローパターン検索（日本株のみ、EDINET データ）
- `search_web_sentiment` — 補助的な Web 検索情報(現状は事前学習知識ベース)

## 原則
- **ツールを積極的に使う**: ユーザーの質問は必ず最新データを参照して回答する。推測や一般論だけで答えない。
- **簡潔に**: 冗長な説明は避け、データに基づく結論を先に述べる。必要に応じて箇条書きを使う。
- **銘柄コードを明示**: 言及する銘柄は必ず $AAPL や 7203.T のような形式でティッカーを示す。
- **免責**: 「投資判断は自己責任」という注意を過剰に繰り返さない。プロのアナリストらしく端的に述べる。
- **日本語で**: 回答は日本語で。

## 絶対に守るルール(超重要)
- **内部ツール名を回答に書かない**: `get_stock_live`, `get_stock_detail`, `get_ranking`, `search_web_sentiment` などの関数名・ツール名を、あなたの回答テキストに絶対に含めない。コードブロック・インラインコード・平文すべて禁止。ユーザーには「最新のマーケットデータ」「スクリーニング結果」「Surge のデータベース」のような自然な日本語で話すこと。
- **「yfinance」「API」などの実装用語も書かない**: ユーザーは投資家であってエンジニアではない。「リアルタイムの株価データ」「最新データ」のように言い換える。
- **Surge のモメンタムスコアの性質**: モメンタムスコアは Surge 独自の複合指標(6指標の加重平均)で、Surge DB にある銘柄(各指数の上位~50銘柄)にしか計算されていない。DB 外の銘柄を調べた場合、スコアは返らないが「1ヶ月リターンが +40%、3ヶ月で +137% ととても強いモメンタム」のように定性的に答えれば十分。スコアの不在を詫びる必要はない。

## 銘柄が見つからない時の対処（重要）
Surge の DB には各インデックスのモメンタム上位約50銘柄しか保存されていません。しかし、**`get_stock_detail` は DB に無い銘柄を自動的に yfinance からライブ取得** します(返却値の `source` が `"live_yfinance"` になる)。

**重要な原則**:
1. **`get_stock_detail` が "not found" エラーを返すのは稀** — ほとんどの上場銘柄はライブフェッチで取得できる
2. **「上場していない」と推測で答えない** — ライブフェッチが失敗したら、それは単にティッカーが間違っているだけの可能性が高い
3. **明示的にライブデータが欲しい時は `get_stock_live`** を呼ぶ(DB をスキップして直接 yfinance から取得)
4. **失敗時**: `get_stock_detail` 両方失敗した場合のみ、ユーザーに正確なティッカーを尋ねる

## 日本企業の名前からの検索
ユーザーが「キオクシアホールディングス」「トヨタ自動車」のように日本語の会社名で質問した場合:
1. 著名企業ならティッカーを知っているはず(キオクシア=285A.T、トヨタ=7203.T、ソニー=6758.T)
2. そのまま `get_stock_detail` に会社名か推測ティッカーを渡す(DB → ライブフェッチが自動で走る)
3. ティッカーがわからない時だけ `search_web_sentiment` で調べる

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


AGENT_SYSTEM_PROMPT_ADDON = """

## 投資仮説検証エージェントモード

あなたは現在、投資仮説検証エージェントとして動作しています。

ユーザーから投資仮説を受け取り、以下の手順で自律的に検証してください：

1. **仮説の分解**: 仮説を検証可能な 2〜5 個の観点に分解する
2. **調査計画**: どのツールをどの順番で使うか計画を立てる
3. **逐次検証**: ツールを 1 つずつ呼び出し、結果を解釈する
4. **動的判断**: 途中結果に応じて計画を修正する（想定外のデータが出たら別の角度から調査する）
5. **結論**: 十分な根拠が集まったら conclude_investigation を呼び出す

重要なルール：
- 1 ステップにつき 1 ツールを呼び出す（並列呼び出しはしない）
- 各ステップで「何を調べているか」「何がわかったか」を日本語で簡潔に述べる
- 仮説に反する証拠も公平に報告する
- 最大 12 ステップ以内で結論を出す
- 不確実な場合は "inconclusive" と判定し、追加調査の提案をする
- 最初に調査計画を述べてから、ツール呼び出しを開始すること
- **conclude_investigation の summary, evidence, next_steps に内部ツール名（get_ranking, get_stock_detail 等）を絶対に書かない。「ランキングデータ」「銘柄詳細」のように自然な日本語で表現すること**
"""

MAX_AGENT_STEPS = 12


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
            # Retry on transient 503/429 errors with exponential backoff,
            # then fall back to gemini-2.5-flash-lite if primary is still overloaded.
            import time
            response = None
            last_error = None
            models_to_try = [model]
            if model != FALLBACK_MODEL:
                models_to_try.append(FALLBACK_MODEL)

            for attempt_model in models_to_try:
                for retry in range(4):
                    try:
                        response = client.models.generate_content(
                            model=attempt_model,
                            contents=contents,
                            config=config,
                        )
                        # Silently record the model actually used for cost calc
                        model = attempt_model
                        break
                    except Exception as e:
                        last_error = e
                        err_str = str(e)
                        is_transient = (
                            "503" in err_str
                            or "UNAVAILABLE" in err_str
                            or "overloaded" in err_str.lower()
                        )
                        is_quota = "429" in err_str or "RESOURCE_EXHAUSTED" in err_str
                        if is_transient or is_quota:
                            # Longer backoff for 429 (API says ~30s), shorter for 503
                            wait = (8 * (retry + 1)) if is_quota else (2 ** retry)
                            time.sleep(wait)
                            continue
                        break
                if response is not None:
                    break

            if response is None:
                # User-friendly message without leaking raw API details
                err_str = str(last_error) if last_error else ""
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    friendly = "AIが混雑しています。30秒ほど待ってから再度お試しください。"
                elif "503" in err_str or "UNAVAILABLE" in err_str:
                    friendly = "AIサーバーが一時的に利用できません。少し時間をおいてから再度お試しください。"
                else:
                    friendly = "AIへの接続に失敗しました。しばらくしてから再度お試しください。"
                yield {
                    "type": "error",
                    "error": friendly,
                }
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

    def run_agent(self, hypothesis, market="jp"):
        """Run the hypothesis investigation agent. Generator yielding NDJSON events:

            {'type': 'plan', 'content': str}
            {'type': 'step', 'step': int, 'tool': str, 'summary': str}
            {'type': 'step_result', 'step': int, 'tool': str, 'summary': str}
            {'type': 'conclusion', 'verdict': str, 'summary': str, 'note_id': int}
            {'type': 'usage', ...}
            {'type': 'done'}
            {'type': 'error', 'error': str}
        """
        # Rate limit check
        allowed, reason = rate_limit_service.check_rate_limit(self.user_id)
        if not allowed:
            yield {"type": "error", "error": reason}
            return

        model = DEFAULT_MODEL

        try:
            client = get_client()
        except Exception as e:
            yield {"type": "error", "error": f"LLM client init failed: {e}"}
            return

        # Build system prompt with agent addon
        system_prompt = self._system_prompt() + AGENT_SYSTEM_PROMPT_ADDON

        # Initial user message with market context
        market_label = "日本株（日経225・グロース250）" if market == "jp" else "米国株（S&P 500・NASDAQ 100）"
        user_message = f"以下の投資仮説を検証してください。\n\n対象市場: {market_label}\n\n仮説: {hypothesis}"

        contents = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=user_message)],
            )
        ]

        tools = self._build_tools_config(use_pro=False)
        config = types.GenerateContentConfig(
            system_instruction=system_prompt,
            tools=tools,
            temperature=0.3,
            max_output_tokens=4000,
        )

        total_tokens_in = 0
        total_tokens_out = 0
        step = 0
        plan_sent = False

        import time
        # Free tier: 5 req/min per model. Use cooldown + model rotation to stay
        # within limits. If primary model (flash) hits 429, fall back to flash-lite
        # which has its own separate quota.
        _STEP_COOLDOWN = int(os.environ.get("AGENT_STEP_COOLDOWN", "13"))
        agent_model = model  # may switch to fallback during the loop

        for round_idx in range(MAX_AGENT_STEPS + 2):
            # Rate-limit spacing: wait between Gemini calls to avoid 429
            if round_idx > 0 and _STEP_COOLDOWN > 0:
                time.sleep(_STEP_COOLDOWN)

            response = None
            last_error = None
            # Try current model, then fallback model (each has its own quota)
            models_to_try = [agent_model]
            if agent_model != FALLBACK_MODEL:
                models_to_try.append(FALLBACK_MODEL)

            for attempt_model in models_to_try:
                for retry in range(3):
                    try:
                        response = client.models.generate_content(
                            model=attempt_model, contents=contents, config=config,
                        )
                        agent_model = attempt_model
                        break
                    except Exception as e:
                        last_error = e
                        err_str = str(e)
                        is_quota = "429" in err_str or "RESOURCE_EXHAUSTED" in err_str
                        is_transient = "503" in err_str or "UNAVAILABLE" in err_str
                        if is_quota or is_transient:
                            wait = (15 * (retry + 1)) if is_quota else (2 ** (retry + 1))
                            time.sleep(wait)
                            continue
                        break
                if response is not None:
                    break

            if response is None:
                err_str = str(last_error) if last_error else ""
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    friendly = "AIが混雑しています。1分ほど待ってから再度お試しください。"
                else:
                    friendly = "AIへの接続に失敗しました。しばらくしてから再度お試しください。"
                yield {"type": "error", "error": friendly}
                return

            if response.usage_metadata:
                total_tokens_in += (response.usage_metadata.prompt_token_count or 0)
                total_tokens_out += (response.usage_metadata.candidates_token_count or 0)

            if not response.candidates:
                yield {"type": "error", "error": "empty response"}
                return

            parts = response.candidates[0].content.parts or []
            function_calls = []
            text_parts = []

            for part in parts:
                if hasattr(part, "function_call") and part.function_call:
                    function_calls.append(part.function_call)
                elif hasattr(part, "text") and part.text:
                    text_parts.append(part.text)

            # Emit plan text (first text response before any tool call)
            if text_parts:
                joined = "".join(text_parts)
                if not plan_sent and not function_calls:
                    # Text-only response before conclusion — treat as plan or intermediate thought
                    yield {"type": "plan", "content": joined}
                    plan_sent = True
                elif not plan_sent and function_calls:
                    yield {"type": "plan", "content": joined}
                    plan_sent = True
                else:
                    yield {"type": "text", "content": joined}

            if function_calls:
                contents.append(response.candidates[0].content)
                function_response_parts = []

                for fc in function_calls:
                    fn_name = fc.name
                    fn_args = dict(fc.args) if fc.args else {}
                    step += 1

                    # Check for conclusion
                    if fn_name == "conclude_investigation":
                        yield {"type": "step", "step": step, "tool": fn_name,
                               "summary": "検証結果をまとめています...", "status": "concluding"}
                        result = llm_tools.dispatch_tool(fn_name, fn_args, self.user_id)
                        yield {
                            "type": "conclusion",
                            "verdict": fn_args.get("verdict", "inconclusive"),
                            "verdict_label": result.get("verdict_label", ""),
                            "summary": fn_args.get("summary", ""),
                            "evidence": fn_args.get("evidence", []),
                            "related_tickers": fn_args.get("related_tickers", []),
                            "next_steps": fn_args.get("next_steps", ""),
                            "note_id": result.get("note_id"),
                            "title": fn_args.get("title", ""),
                        }
                        # Record usage and finish
                        cost = rate_limit_service.calculate_cost(agent_model, total_tokens_in, total_tokens_out)
                        rate_limit_service.record_usage(self.user_id, agent_model, total_tokens_in, total_tokens_out)
                        yield {"type": "usage", "tokens_in": total_tokens_in, "tokens_out": total_tokens_out,
                               "cost_usd": round(cost, 6), "model": agent_model}
                        yield {"type": "done"}
                        return

                    # Regular tool call
                    yield {"type": "step", "step": step, "tool": fn_name,
                           "summary": f"ステップ {step}: {fn_name} を実行中...", "status": "investigating"}
                    result = llm_tools.dispatch_tool(fn_name, fn_args, self.user_id)
                    yield {"type": "step_result", "step": step, "tool": fn_name,
                           "summary": f"ステップ {step} 完了", "status": "done"}

                    function_response_parts.append(
                        types.Part.from_function_response(
                            name=fn_name, response={"result": result},
                        )
                    )

                contents.append(types.Content(role="user", parts=function_response_parts))

                if step >= MAX_AGENT_STEPS:
                    yield {"type": "text", "content": f"（ステップ上限 {MAX_AGENT_STEPS} に到達しました。現時点の調査結果をまとめます。）"}
                    break
                continue

            # No function calls — LLM finished without conclude_investigation
            break

        # Record usage
        cost = rate_limit_service.calculate_cost(agent_model, total_tokens_in, total_tokens_out)
        rate_limit_service.record_usage(self.user_id, agent_model, total_tokens_in, total_tokens_out)
        yield {"type": "usage", "tokens_in": total_tokens_in, "tokens_out": total_tokens_out,
               "cost_usd": round(cost, 6), "model": agent_model}
        yield {"type": "done"}
