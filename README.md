<p align="center">
  <img src="static/favicon.svg" width="64" alt="Surge logo" />
</p>

<h1 align="center">Surge v2</h1>

<p align="center">
  <strong>4市場 × 30+指標のモメンタムスクリーニング + AI 銘柄分析ダッシュボード</strong><br/>
  日経225 / グロース250 / S&P 500 / NASDAQ 100
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776ab?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Flask-3.0-000000?logo=flask" />
  <img src="https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white" />
  <img src="https://img.shields.io/badge/Tailwind_CSS-06B6D4?logo=tailwindcss&logoColor=white" />
  <img src="https://img.shields.io/badge/Chart.js-4-FF6384?logo=chartdotjs&logoColor=white" />
  <img src="https://img.shields.io/badge/Gemini-2.5_Flash-4285F4?logo=google" />
  <img src="https://img.shields.io/badge/Deploy-Railway-0B0D0E?logo=railway" />
  <img src="https://img.shields.io/badge/License-MIT-green" />
</p>

---

## What is Surge?

Surge は **「今、勢いのある銘柄」を定量的に見つけて、AI と対話しながら深掘りする** ためのダッシュボードです。

1. ボタン1つで 225〜503 銘柄を一括スクリーニング
2. 騰落率・出来高・MACD・RSI・OBV など **30以上のテクニカル指標** を自動計算
3. 6指標を加重平均した **モメンタムスコア（100点満点）** でランキング
4. 市場レジーム・セクターローテーション・相関マトリクスで **相場全体の温度感** を把握
5. 🤖 **Gemini 2.5 Flash** に対話形式で質問 → Tool Use で DB・EDINET・Web検索を横断
6. 📓 気になった銘柄は **調査ノート** にワンクリック保存、全文検索で後から再利用
7. 👥 **マルチユーザー認証** で個別アカウント運用
8. ウォッチリスト・変化検知・カスタムアラート・ブラウザ通知で **見逃さない**
9. 📈 スクリーニング毎に **モメンタムタイムライン** でスコア推移を可視化
10. 📋 チャットドロワーに **モーニングブリーフ** が自動表示され、朝の市場チェックが30秒で完了

---

## Highlights

### 🤖 AI & 調査ノート（LLM Phase 1）

| 機能 | 概要 |
|------|------|
| **銘柄分析ロボちゃん** | Gemini 2.5 Flash が銘柄DBを直接参照するAIチャット。右下FABボタンから起動 |
| **Tool Use（9種）** | ランキング・銘柄詳細・条件フィルタ・レジーム・比較・類似銘柄・CFパターン・セクター回転・Web検索 |
| **情報源の横断** | Surge DB（一次情報）+ EDINET + Google Search + Gemini の事前学習知識を AI が自動で使い分け |
| **調査ノート** | チャット回答をワンクリック保存。タイトル・タグ・関連銘柄付きで記録、全文検索対応 |
| **マルチユーザー認証** | 個別アカウント、日次リクエスト上限、パスワード変更UI |
| **コスト管理** | 日次リクエスト上限 + 全体コストキャップ（USD）。毎日JST 0:00に自動リセット |

### 📊 スクリーニング & 分析

| 機能 | 概要 |
|------|------|
| **複合モメンタムスコア** | 6指標パーセンタイルランクの加重平均。5つのウェイトプリセットで即座に切替 |
| **市場レジーム自動判定** | 騰落比率・ADL・スコア分布・セクターローテーションから7段階に分類 |
| **セクターローテーション** | 1M×3Mリターンのバブルチャートで資金の流れを4象限に可視化 |
| **セクター相関マトリクス** | 60日リターンに基づくヒートマップ。分散投資の判断材料 |
| **逆張り候補 (Value Gap)** | アナリスト目標株価乖離 × ファンダメンタルズで割安銘柄を検出 |
| **52週ブレイクアウト** | 新高値・BB圧縮銘柄を専用タブで一覧表示 |
| **OBV (On-Balance Volume)** | 出来高累積トレンド + 価格との乖離検出（強気/弱気ダイバージェンス） |
| **ドローダウン分析** | 3ヶ月最大DD・現在DDをモーダルに表示。リスク把握に |
| **銘柄比較モード** | チェックボックスで2〜3銘柄を選択 → 18指標を並べて比較 |
| **変化検知 & 通知** | 新規ランクイン・スコア急変をリアルタイム検出。ブラウザ通知対応 |
| **モメンタムタイムライン** | 銘柄詳細モーダルで過去スクリーニングのスコア+RSI推移を折れ線チャート表示 |
| **スマートウォッチリスト** | ウォッチリスト銘柄にカスタム条件(RSI<30等)を設定、スクリーニング時に自動チェック→⚡通知 |
| **モーニングブリーフ** | チャットドロワー起動時に自動表示。地合い・注目銘柄・過熱注意・WLアラートをカード形式で要約 |
| **オプション動向分析** | yfinance option_chain()からGEX・PCR・IV Rank・Max Pain・IVスキューを算出（米国株） |
| **日次レポート** | レジーム×ブレス×ローテーション×変化検知から「初動/継続/注意」候補を自動生成 |
| **バックテスト** | 上位銘柄等加重ポートフォリオの勝率・シャープ比・vsベンチマークを算出 |
| **日本株専用分析** | CF分析（EDINET）・種まき度・資本配分・タイム裁定・小型優良株 |
| **米国株専用分析** | EPS改定・機関フロー・決算後ドリフト・ショートスクイーズ・オプション動向・週足9EMA |
| **データ品質管理** | ソース健全性・カバレッジ・異常値検出を品質マトリクスタブで確認 |

---

## ページ構成

| URL | 対象市場 | 専用機能 |
|-----|---------|---------|
| `/` | 日経225 / グロース250 | CF分析・種まき度・資本配分・タイム裁定・小型優良株 |
| `/us` | S&P 500 / NASDAQ 100 | ショートスクイーズ・オプション動向(GEX/PCR/IV)・EPS改定・機関フロー・決算後ドリフト・W9EMA |
| `/howto` | — | 使い方ガイド（全機能の解説） |

---

## スコアリング

### モメンタムスコア（100点満点）

| 指標 | デフォルトウェイト |
|------|----------|
| 1ヶ月リターン | 20% |
| 3ヶ月リターン | 20% |
| 出来高比 (5日/20日) | 15% |
| 50日MA乖離率 | 15% |
| MACDヒストグラム | 15% |
| RSI | 15% |

**5つのプリセット**: バランス型 / 出来高重視 / トレンド重視 / 初動特化 / リバーサル

### その他のスコア

| スコア | 概要 |
|--------|------|
| **逆張りスコア** | 目標株価乖離(40%) + 推奨(15%) + PER割安(15%) + EPS成長(15%) + RSI(15%) |
| **スクイーズスコア** | 空売り比率(40%) + DTC(30%) + 前月比変化(15%) + モメンタム(15%) |
| **US Advanced Score** | EPS改定(30%) + 機関フロー(30%) + 決算後ドリフト(25%) + オプション動向(15%) |
| **種まき度スコア** | 設備投資急増 × CF黒字 × 売上成長 × 利益減 × 株価失望 |
| **資本配分スコア** | 営業CF安定性 / 設備投資一貫性 / FCF品質 / 株主還元 / M&A余力 |
| **品質スコア** | ATR% / 上下出来高比 / ギャップ率 / ヒゲ比率 |

### 相対強度 (RS) 判定

| ラベル | 条件 | 意味 |
|--------|------|------|
| 本命 | RS1M > +2% かつ RS3M > 0 | 短期も中期もセクター比で強い |
| 短期 | RS1M > +2% かつ RS3M ≤ 0 | 短期のみ強い |
| 劣後 | RS1M ≤ +2% | セクター比で上位にいない |
| テーマ | BTC連動銘柄等 | セクター比較が不適切 |

---

## セットアップ

```bash
git clone https://github.com/akiraizutsu/Surge.git
cd Surge
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python app.py
```

ブラウザで `http://localhost:5001` を開き、「スクリーニング実行」をクリック。

### 環境変数

#### 基本設定

| 変数名 | 用途 | デフォルト |
|--------|------|-----------|
| `PORT` | サーバーポート | `5001` |
| `SECRET_KEY` | Flaskセッション署名キー | フォールバック値（本番では必ず設定） |
| `DATA_DIR` | データ永続化ディレクトリ（Railway Volume） | — |

#### データソース

| 変数名 | 用途 | 必須 |
|--------|------|------|
| `EDINETDB_API_KEY` | EDINET DB APIキー（日本株CF） | 日本株CF機能 |
| `JQUANTS_API_KEY` | J-Quants APIキー（四半期CF） | 任意 |

#### LLM & マルチユーザー認証

| 変数名 | 用途 | 必須 |
|--------|------|------|
| `GEMINI_API_KEY` | Gemini API キー（AIチャット機能） | AIチャット機能 |
| `SURGE_USERS` | 初期ユーザー JSON（下記参照） | マルチユーザー運用 |
| `FRIEND_DAILY_REQUEST_LIMIT` | 通常アカウントの日次リクエスト上限 | 任意（デフォルト `30`） |
| `OWNER_DAILY_REQUEST_LIMIT` | 管理アカウントの日次リクエスト上限 | 任意（デフォルト `200`） |
| `TOTAL_DAILY_COST_LIMIT_USD` | 全ユーザー合計の日次コスト緊急ブレーキ | 任意（デフォルト `5.00`） |

**`SURGE_USERS` の形式** — 初回起動時に seed される JSON 配列:

```json
[
  {"username":"alice","password":"強いパスワード","display_name":"Alice","role":"owner","avatar_emoji":"🧑‍💻"},
  {"username":"bob","password":"強いパスワード","display_name":"Bob","role":"user","avatar_emoji":"👨"}
]
```

> **Note:** 初回起動後、ユーザーテーブルに追記するには `python3 admin.py seed` を実行してください。既存ユーザーのパスワードは変更されません（INSERT OR IGNORE による保護）。

---

## アーキテクチャ

```
 ┌─────────────────────────────────────────────────────────────────┐
 │                         [Browser]                                │
 │   japan.html / us.html / howto.html / login.html                 │
 │   static/app.js (Chart.js, Tailwind, Markdown rendering)         │
 └──────────────┬──────────────────────────────────┬───────────────┘
                │                                  │
                │ POST /api/screen                 │ POST /api/chat (NDJSON stream)
                │ GET  /api/status                 │ POST /api/notes
                │ GET  /api/results                │ GET  /api/auth/me
                │                                  │
 ┌──────────────▼──────────────────────────────────▼───────────────┐
 │                       [Flask app.py]                             │
 │   @login_required  ·  session auth  ·  per-user rate limit       │
 └────┬──────────────────────────────────────────────┬─────────────┘
      │                                              │
      │ Background Thread                            │ AnalystAI (streaming)
      │ └ screener.run_screening()                   │ └ llm_service.py
      │   ├ Wikipedia → 銘柄リスト                   │   ├ Gemini 2.5 Flash/Pro
      │   ├ yfinance → 価格・出来高・ファンダ        │   ├ Tool Use loop
      │   ├ scoring_service → 複合スコア             │   └ llm_tools.py
      │   ├ tagging_service → タグ判定               │     ├ get_ranking
      │   ├ regime_service → 市場レジーム            │     ├ get_stock_detail
      │   ├ us_advanced_service → 米株高度分析       │     ├ filter_stocks
      │   ├ OBV/DD/相関/変化検知                     │     ├ get_market_regime
      │   └ 日次レポート生成                         │     ├ compare_stocks
      │                                              │     ├ find_similar_stocks
      │                                              │     ├ get_cf_pattern_stocks
      │                                              │     ├ get_sector_rotation
      │                                              │     └ search_web_sentiment
      │                                              │
      └──────────┬───────────────────────────────────┘
                 │
                 ▼
         ┌────────────────┐
         │   database.py  │
         │  SQLite (WAL)  │
         └───────┬────────┘
                 │
   ┌─────────────┴──────────────────────────────────┐
   │                                                │
   ▼                                                ▼
 screening_*                                users / research_notes /
 watchlist / breadth_data /                 user_usage / chat_history
 cf_cache / edinet_* /                      (LLM Phase 1)
 stock_explanations /
 backtest_results / data_source_status
```

### ディレクトリ構成

```
surge/
├── app.py                        # Flask API・バックグラウンドスレッド管理・認証ルート
├── screener.py                   # スクリーニングエンジン（30+指標・OBV・DD・相関）
├── database.py                   # SQLite CRUD・スキーママイグレーション・user seed
│
├── auth_service.py               # パスワードハッシュ・ユーザー認証・セッション管理
├── llm_service.py                # Gemini クライアント・チャットループ・フォールバック
├── llm_tools.py                  # Tool Use 実装（9ツール）
├── notes_service.py              # 調査ノート CRUD・全文検索
│
├── scoring_service.py            # 複合スコア計算・ウェイトプリセット
├── tagging_service.py            # タグ付け・エントリー難易度判定
├── regime_service.py             # 市場レジーム分類（7段階）
├── questions_service.py          # 銘柄別チェック質問生成
├── backtest_service.py           # モメンタムバックテスト
├── seed_score_service.py         # 種まき度スコア（日本株）
├── capital_allocation_service.py # 資本配分スコア
├── us_advanced_service.py        # 米国株高度分析（EPS・機関・決算・オプション）
├── options_service.py            # オプション動向分析（GEX・PCR・IV・MaxPain・スキュー）
├── data_quality_service.py       # データソース健全性管理
│
├── admin.py                      # CLI ユーザー管理（list / add / set-password / seed）
│
├── templates/
│   ├── japan.html                # 日本株ページ
│   ├── us.html                   # 米国株ページ
│   ├── howto.html                # 使い方ガイド（AI・ノート・スクリーニング）
│   └── login.html                # ログイン画面
├── static/
│   ├── app.js                    # フロントエンド（チャート・チャット・ノート・通知）
│   ├── style.css                 # Kokyu Design System
│   └── favicon.svg
├── KOKYU-UI-GUIDE.md             # デザインシステム仕様書
├── requirements.txt
└── README.md
```

---

## データソース

| データ | ソース |
|--------|--------|
| S&P 500 / NASDAQ 100 構成銘柄 | Wikipedia |
| 日経225 / グロース250 構成銘柄 | Wikipedia（日本語版） |
| 価格・出来高・ファンダメンタルズ・空売り | Yahoo Finance (yfinance) |
| オプションチェーン（GEX・PCR・IV） | Yahoo Finance (yfinance `option_chain()`) |
| セクターETF（RS算出） | Yahoo Finance（XLK, XLV, XLF 等 + SPY） |
| 日本株CF・財務データ（年次） | EDINET DB API |
| 日本株CF（四半期） | J-Quants API |

## 技術スタック

| レイヤー | 技術 |
|----------|------|
| バックエンド | Python 3.12 / Flask / pandas / numpy |
| サービス層 | 13 専用サービス（scoring / tagging / regime / us_advanced / options / llm / notes / auth 等） |
| AI | Google Gemini 2.5 Flash / Pro / Flash-lite（google-genai 1.72+）+ Tool Use |
| データ取得 | yfinance / Wikipedia / EDINET DB API / J-Quants API |
| データベース | SQLite（WALモード・自動マイグレーション） |
| 認証 | werkzeug.security（password hash）+ Flask sessions |
| フロントエンド | Tailwind CSS (CDN) / Chart.js 4 / Vanilla JS / NDJSON streaming |
| デプロイ | Railway（Hobby plan + Persistent Volume） |

---

## API リファレンス

<details>
<summary>エンドポイント一覧（クリックで展開）</summary>

### 認証

| メソッド | パス | 用途 |
|---------|------|------|
| GET | `/login` | ログイン画面 |
| POST | `/login` | ログイン処理 |
| POST | `/logout` | ログアウト |
| GET | `/api/auth/me` | 現在のユーザー情報（ティア・リクエスト残数） |
| POST | `/api/auth/change_password` | パスワード変更 |

### AIチャット（LLM Phase 1）

| メソッド | パス | 用途 |
|---------|------|------|
| POST | `/api/chat` | Gemini チャット（NDJSON ストリーム） |
| GET | `/api/chat/history` | 過去のチャット履歴取得 |
| DELETE | `/api/chat/history` | チャット履歴クリア |

### 調査ノート

| メソッド | パス | 用途 |
|---------|------|------|
| GET | `/api/notes` | ノート一覧（自分のみ） |
| POST | `/api/notes` | ノート作成 |
| GET | `/api/notes/<id>` | ノート詳細 |
| PATCH | `/api/notes/<id>` | ノート更新（ピン留め等） |
| DELETE | `/api/notes/<id>` | ノート削除 |
| GET | `/api/notes/search?q=...` | 全文検索 |

### スクリーニング

| メソッド | パス | 用途 |
|---------|------|------|
| POST | `/api/screen` | スクリーニング開始 |
| GET | `/api/status` | 進捗確認（ETA含む） |
| GET | `/api/results` | 全インデックス結果取得 |
| POST | `/api/clear_error` | エラー状態リセット |
| GET | `/api/breadth/<index>` | 騰落線データ |
| GET | `/api/history` | 過去セッション一覧 |

### ウォッチリスト

| メソッド | パス | 用途 |
|---------|------|------|
| GET | `/api/watchlist` | ウォッチリスト取得 |
| POST | `/api/watchlist` | 銘柄追加 |
| DELETE | `/api/watchlist/<ticker>` | 銘柄削除 |
| GET | `/api/watchlist/<ticker>/alerts` | カスタムアラートルール取得 |
| PATCH | `/api/watchlist/<ticker>/alerts` | カスタムアラートルール更新 |
| GET | `/api/watchlist/events` | 変化イベント一覧 |
| POST | `/api/watchlist/events/read` | イベント既読化 |

### 個別銘柄

| メソッド | パス | 用途 |
|---------|------|------|
| GET | `/api/cf_analysis/<ticker>` | CF分析（日本株） |
| GET | `/api/stock/<ticker>/explain` | スコア分解・タグ・質問 |
| GET | `/api/stock/<ticker>/seed_score` | 種まき度詳細 |
| GET | `/api/stock/<ticker>/capital_allocation` | 資本配分詳細 |
| GET | `/api/stock/<ticker>/us_advanced` | 米国株高度分析シグナル（オプション動向含む） |
| GET | `/api/stock/<ticker>/timeline` | モメンタムスコア推移（過去30セッション） |

### バックテスト・品質

| メソッド | パス | 用途 |
|---------|------|------|
| POST | `/api/backtest` | バックテスト実行 |
| GET | `/api/backtest/results` | バックテスト履歴 |
| GET | `/api/briefs/latest` | モーニングブリーフ取得（?page=japan\|us） |
| GET | `/api/data_quality/status` | データソース健全性 |

### `/api/screen` インデックス指定

| 値 | 処理 |
|----|------|
| `japan_all` | 日経225 + グロース250 |
| `us_all` | S&P 500 + NASDAQ 100 |
| `nikkei225` / `growth250` / `sp500` / `nasdaq100` | 単一インデックス |

</details>

---

## ユーザー管理 CLI

```bash
python3 admin.py list                              # 全ユーザー一覧
python3 admin.py add-user --username taro          # 新規ユーザー（対話的にパスワード入力）
python3 admin.py set-password --username akira     # パスワードリセット
python3 admin.py delete-user --username taro       # ユーザー削除（ノートも削除）
python3 admin.py seed                              # SURGE_USERS env から不足分を追加（冪等）
```

Railway 本番で実行する場合は `railway run python3 admin.py ...`。

---

## 免責事項

本ツールは教育・研究目的で作成されています。投資判断への利用は自己責任でお願いします。データは Yahoo Finance・EDINET DB・J-Quants から取得しており、正確性を保証するものではありません。

## ライセンス

[MIT](LICENSE)
