<p align="center">
  <img src="static/favicon.svg" width="64" alt="Surge logo" />
</p>

<h1 align="center">Surge v2</h1>

<p align="center">
  <strong>4市場 × 30+指標のモメンタムスクリーニングダッシュボード</strong><br/>
  日経225 / グロース250 / S&P 500 / NASDAQ 100
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776ab?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/Flask-3.0-000000?logo=flask" />
  <img src="https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white" />
  <img src="https://img.shields.io/badge/Tailwind_CSS-06B6D4?logo=tailwindcss&logoColor=white" />
  <img src="https://img.shields.io/badge/Chart.js-4-FF6384?logo=chartdotjs&logoColor=white" />
  <img src="https://img.shields.io/badge/Deploy-Railway-0B0D0E?logo=railway" />
  <img src="https://img.shields.io/badge/License-MIT-green" />
</p>

---

## What is Surge?

Surge は **「今、勢いのある銘柄」を定量的に見つける** ためのダッシュボードです。

1. ボタン1つで 225〜503 銘柄を一括スクリーニング
2. 騰落率・出来高・MACD・RSI・OBV など **30以上のテクニカル指標** を自動計算
3. 6指標を加重平均した **モメンタムスコア（100点満点）** でランキング
4. 市場レジーム・セクターローテーション・相関マトリクスで **相場全体の温度感** を把握
5. ウォッチリスト・変化検知・ブラウザ通知で **見逃さない**

---

## Highlights

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
| **日次レポート** | レジーム×ブレス×ローテーション×変化検知から「初動/継続/注意」候補を自動生成 |
| **バックテスト** | 上位銘柄等加重ポートフォリオの勝率・シャープ比・vsベンチマークを算出 |
| **日本株専用分析** | CF分析（EDINET）・種まき度・資本配分・タイム裁定・小型優良株 |
| **米国株専用分析** | EPS改定・機関フロー・決算後ドリフト・ショートスクイーズ・週足9EMA |
| **スクリーニングETA** | 残り時間をリアルタイム推定表示 |
| **エラーリカバリー** | 失敗時に再試行ボタンを表示。stale状態も自動回復 |
| **データ品質管理** | ソース健全性・カバレッジ・異常値検出を品質マトリクスタブで確認 |

---

## ページ構成

| URL | 対象市場 | 専用機能 |
|-----|---------|---------|
| `/` | 日経225 / グロース250 | CF分析・種まき度・資本配分・タイム裁定・小型優良株 |
| `/us` | S&P 500 / NASDAQ 100 | ショートスクイーズ・EPS改定・機関フロー・決算後ドリフト・W9EMA |
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
| **US Advanced Score** | EPS改定(30%) + 機関フロー(30%) + 決算後ドリフト(25%) + オプション(15%) |
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

| 変数名 | 用途 | デフォルト |
|--------|------|-----------|
| `PORT` | サーバーポート | `5001` |
| `SURGE_PASSWORD` | ログインパスワード（未設定時は認証スキップ） | — |
| `SECRET_KEY` | Flaskセッション署名キー | フォールバック値 |
| `EDINETDB_API_KEY` | EDINET DB APIキー（日本株CF） | — |
| `JQUANTS_API_KEY` | J-Quants APIキー（四半期CF） | — |
| `DATA_DIR` | データ永続化ディレクトリ（Railway Volume） | — |

---

## アーキテクチャ

```
[Browser] → POST /api/screen → [Flask app.py]
                                      │
                                      ├── Background Thread
                                      │     └── screener.run_screening()
                                      │           ├── Wikipedia → 銘柄リスト
                                      │           ├── yfinance → 価格・出来高・ファンダ
                                      │           ├── scoring_service → 複合スコア
                                      │           ├── tagging_service → タグ・エントリー難易度
                                      │           ├── regime_service → 市場レジーム
                                      │           ├── OBV・ドローダウン・セクター相関
                                      │           ├── us_advanced_service → 米国株高度分析
                                      │           ├── 変化検知 → watchlist_events
                                      │           └── 日次レポート生成
                                      │
                                      └── database.py → SQLite

[Browser] ← GET /api/status（1秒ポーリング + ETA表示）
[Browser] ← GET /api/results ← JSON
```

### ディレクトリ構成

```
surge/
├── app.py                        # Flask API・バックグラウンドスレッド管理
├── screener.py                   # スクリーニングエンジン（30+指標・OBV・DD・相関）
├── database.py                   # SQLite CRUD・スキーママイグレーション
│
├── scoring_service.py            # 複合スコア計算・ウェイトプリセット
├── tagging_service.py            # タグ付け・エントリー難易度判定
├── regime_service.py             # 市場レジーム分類（7段階）
├── questions_service.py          # 銘柄別チェック質問生成
├── backtest_service.py           # モメンタムバックテスト
├── seed_score_service.py         # 種まき度スコア（日本株）
├── capital_allocation_service.py # 資本配分スコア
├── us_advanced_service.py        # 米国株高度分析（EPS・機関・決算・オプション）
├── data_quality_service.py       # データソース健全性管理
│
├── templates/
│   ├── japan.html                # 日本株ページ
│   ├── us.html                   # 米国株ページ
│   └── howto.html                # 使い方ガイド
├── static/
│   ├── app.js                    # フロントエンド（チャート・テーブル・比較・通知）
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
| セクターETF（RS算出） | Yahoo Finance（XLK, XLV, XLF 等 + SPY） |
| 日本株CF・財務データ（年次） | EDINET DB API |
| 日本株CF（四半期） | J-Quants API |

## 技術スタック

| レイヤー | 技術 |
|----------|------|
| バックエンド | Python 3.12 / Flask / pandas / numpy |
| サービス層 | 9つの専用サービス（scoring / tagging / regime / us_advanced 等） |
| データ取得 | yfinance / Wikipedia / EDINET DB API / J-Quants API |
| データベース | SQLite（WALモード・自動マイグレーション） |
| フロントエンド | Tailwind CSS (CDN) / Chart.js 4 / Vanilla JS |
| デプロイ | Railway（Hobby plan + Persistent Volume） |

---

## API リファレンス

<details>
<summary>エンドポイント一覧（クリックで展開）</summary>

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
| GET | `/api/watchlist/events` | 変化イベント一覧 |
| POST | `/api/watchlist/events/read` | イベント既読化 |

### 個別銘柄

| メソッド | パス | 用途 |
|---------|------|------|
| GET | `/api/cf_analysis/<ticker>` | CF分析（日本株） |
| GET | `/api/stock/<ticker>/explain` | スコア分解・タグ・質問 |
| GET | `/api/stock/<ticker>/seed_score` | 種まき度詳細 |
| GET | `/api/stock/<ticker>/capital_allocation` | 資本配分詳細 |
| GET | `/api/stock/<ticker>/us_advanced` | 米国株高度分析シグナル |

### バックテスト・品質

| メソッド | パス | 用途 |
|---------|------|------|
| POST | `/api/backtest` | バックテスト実行 |
| GET | `/api/backtest/results` | バックテスト履歴 |
| GET | `/api/data_quality/status` | データソース健全性 |

### `/api/screen` インデックス指定

| 値 | 処理 |
|----|------|
| `japan_all` | 日経225 + グロース250 |
| `us_all` | S&P 500 + NASDAQ 100 |
| `nikkei225` / `growth250` / `sp500` / `nasdaq100` | 単一インデックス |

</details>

---

## 免責事項

本ツールは教育・研究目的で作成されています。投資判断への利用は自己責任でお願いします。データは Yahoo Finance・EDINET DB・J-Quants から取得しており、正確性を保証するものではありません。

## ライセンス

[MIT](LICENSE)
