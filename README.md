# Surge

日本株（日経225・グロース250）と米国株（S&P 500・NASDAQ 100）のモメンタムスクリーニングダッシュボード。

騰落率・出来高・MA乖離・MACD・RSI を複合スコアリングし、勢いのある銘柄を自動抽出。日本株と米国株でページを分け、それぞれ専用の分析機能を提供します。

![Python](https://img.shields.io/badge/Python-3.12-3776ab?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask)
![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

## ページ構成

| URL | 対象市場 | 専用機能 |
|-----|---------|---------|
| `/` | 日経225 / グロース250 | CF分析・タイム裁定・小型優良株 |
| `/us` | S&P 500 / NASDAQ 100 | ショートスクイーズ分析 |

## 特徴

### 共通機能（日本株・米国株）

- **複合モメンタムスコア** — 6指標のパーセンタイルランクを加重平均（100点満点）
- **逆張り候補 (Value Gap)** — アナリスト目標株価との乖離 × ファンダメンタルズで割安銘柄を検出
- **相対強度 (RS)** — セクターETF / ベンチマーク比較で「本命 / 短期 / 劣後 / テーマ」を自動判定
- **騰落線 (ADL)** — 市場ブレス分析で相場全体の健全性を可視化
- **セクターローテーション** — 1M/3Mリターンのバブルチャートで資金の流れを可視化
- **52週ブレイクアウト** — 新高値・ボリンジャーバンド圧縮銘柄を自動検出
- **決算スケジュール** — 決算まであと何日かをテーブルに表示
- **ウォッチリスト** — 気になる銘柄を☆で保存、フィルタ表示
- **SQLite 永続化** — 再起動・再デプロイ後もスクリーニング結果を自動復元
- **ログイン保護** — セッション認証でダッシュボードをパスワード保護
- **レスポンシブ UI** — モバイル・タブレット・デスクトップ対応
- **HSLベースカラーシステム** — 1色選ぶだけでWCAG AA準拠の配色を自動生成

### 日本株専用機能

- **グロース250対応** — 東証グロース市場250指数の銘柄をWikipediaから取得し、日経225と並列スクリーニング
- **CF分析** — EDINET DB API経由で最大14年の年次キャッシュフロー推移をチャート表示（営業CF / 投資CF / 財務CF / FCF）
- **四半期CF** — J-Quants API連携で四半期単体の営業CFを可視化
- **M&A実弾試算** — ネットキャッシュ + 年間FCF × 年数でM&A余力を自動計算
- **タイム裁定候補** — 設備投資急増（≥+20%）× 一過性赤字 × 営業CFプラスの銘柄を抽出（機関投資家が短期業績で売った種まき銘柄）
- **小型優良株** — 時価総額100〜3000億円帯でモメンタムスコアが高い銘柄を抽出
- **EDINETデータ補完** — セクター・ROE・売上高・純利益をEDINET DB API + SQLiteキャッシュで取得

### 米国株専用機能

- **ショートスクイーズ分析** — 空売り比率・Days to Coverから踏み上げ期待値を算出

## スコアリング

### モメンタムスコア（100点満点）

| 指標 | ウェイト |
|------|----------|
| 1ヶ月リターン | 20% |
| 3ヶ月リターン | 20% |
| 出来高比 (5日/20日) | 15% |
| 50日MA乖離率 | 15% |
| MACDヒストグラム | 15% |
| RSI | 15% |

### 逆張りスコア (Value Gap)

| 指標 | ウェイト |
|------|----------|
| 目標株価乖離率 | 40% |
| アナリスト推奨 | 15% |
| PER (予想) 割安度 | 15% |
| EPS成長率 | 15% |
| RSI売られすぎ | 15% |

### スクイーズスコア（米国株のみ）

| 指標 | ウェイト |
|------|----------|
| 空売り比率 (% of Float) | 40% |
| Days to Cover | 30% |
| 空売り前月比変化 | 15% |
| モメンタムスコア | 15% |

### 相対強度 (RS) 判定

| ラベル | 条件 | 意味 |
|--------|------|------|
| 本命 | RS1M > +2% かつ RS3M > 0 | 短期も中期もセクター比で強い |
| 短期 | RS1M > +2% かつ RS3M ≤ 0 | 短期は強いが中期は弱い |
| 劣後 | RS1M ≤ +2% | セクター比で上位にいない |
| テーマ | BTC連動銘柄等 | セクター比較が不適切 |

- 米国株: セクターETF（XLK, XLV, XLF 等 11本）と比較
- 日経225 / グロース250: 日経平均（^N225）と比較

## セットアップ

```bash
git clone https://github.com/akiraizutsu/surge.git
cd surge
pip install -r requirements.txt
python app.py
```

ブラウザで `http://localhost:5001` を開き、パスワードでログイン後「スクリーニング実行」をクリック。

### 環境変数

| 変数名 | 用途 | デフォルト |
|--------|------|-----------|
| `PORT` | サーバーポート | `5001` |
| `SURGE_PASSWORD` | ログインパスワード（未設定時は認証スキップ） | — |
| `SECRET_KEY` | Flaskセッション署名キー（本番では必ず設定） | フォールバック値 |
| `EDINETDB_API_KEY` | EDINET DB APIキー（日本株CF・財務データ取得） | — |
| `JQUANTS_API_KEY` | J-Quants APIキー（四半期CF取得、無料プラン対応） | — |

`.env` ファイルに記載するか、Railwayの Variables に設定してください。

## API

| メソッド | パス | 用途 |
|---------|------|------|
| POST | `/api/screen` | スクリーニング開始（`japan_all` / `us_all` / 単一インデックス） |
| GET | `/api/status` | 進捗確認 |
| GET | `/api/results` | 全インデックス結果取得（DB復元対応） |
| GET | `/api/breadth/<index>` | 騰落線データ取得 |
| GET | `/api/cf_analysis/<ticker>` | キャッシュフロー分析（日本株） |
| GET | `/api/watchlist` | ウォッチリスト取得 |
| POST | `/api/watchlist` | 銘柄追加 |
| DELETE | `/api/watchlist/<ticker>` | 銘柄削除 |
| GET | `/api/history` | 過去セッション一覧 |

### `/api/screen` インデックス指定

| 値 | 処理内容 |
|----|---------|
| `japan_all` | 日経225 + グロース250（日本株ページ用） |
| `us_all` | S&P 500 + NASDAQ 100（米国株ページ用） |
| `nikkei225` | 日経225のみ |
| `growth250` | グロース250のみ |
| `sp500` | S&P 500のみ |
| `nasdaq100` | NASDAQ 100のみ |

## データソース

| データ | ソース |
|--------|--------|
| S&P 500 / NASDAQ 100 構成銘柄 | Wikipedia |
| 日経225 / グロース250 構成銘柄 | Wikipedia（日本語版） |
| 価格・出来高・ファンダメンタルズ | Yahoo Finance (yfinance) |
| セクターETFデータ | Yahoo Finance（XLK, XLV, XLF 等） |
| 日本株CF・財務データ（年次） | EDINET DB API |
| 日本株CF（四半期） | J-Quants API（無料プラン） |

## 技術スタック

| レイヤー | 技術 |
|----------|------|
| バックエンド | Python 3.12 / Flask / pandas / numpy |
| データ取得 | yfinance / Wikipedia / EDINET DB API / J-Quants API |
| データベース | SQLite（sqlite3 標準ライブラリ） |
| フロントエンド | Tailwind CSS (CDN) / Chart.js 4 / Vanilla JS |
| デプロイ | Railway（Hobby plan + Persistent Volume） |

## 免責事項

本ツールは教育・研究目的で作成されています。投資判断への利用は自己責任でお願いします。データはYahoo Finance・EDINET DB・J-Quantsから取得しており、正確性を保証するものではありません。

## ライセンス

[MIT](LICENSE)
