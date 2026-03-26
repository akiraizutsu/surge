# Surge

S&P 500 / NASDAQ 100 / 日経225 のモメンタムスクリーニングダッシュボード。

騰落率・出来高・MA乖離・MACD・RSI を複合スコアリングし、勢いのある銘柄を自動抽出。逆張り候補の検出・ショートスクイーズ分析・相対強度判定・市場ブレス分析を備えます。

![Python](https://img.shields.io/badge/Python-3.12-3776ab?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.0-000000?logo=flask)
![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

## 特徴

- **複合モメンタムスコア** — 6指標のパーセンタイルランクを加重平均
- **逆張り候補 (Value Gap)** — アナリスト目標株価との乖離 × ファンダメンタルズで割安銘柄を検出
- **相対強度 (RS)** — セクターETF / ベンチマーク比較で「本命 / 短期 / 劣後 / テーマ」を自動判定
- **ショートスクイーズ分析** — 空売り比率・Days to Cover から踏み上げ期待値を算出（米国株のみ）
- **騰落線 (ADL)** — 市場ブレス分析で相場全体の健全性を可視化
- **ウォッチリスト** — 気になる銘柄を☆で保存、フィルタ表示
- **3市場対応** — S&P 500 / NASDAQ 100 / 日経225 を一括スクリーニング
- **SQLite 永続化** — 再起動・再デプロイ後もスクリーニング結果を自動復元
- **レスポンシブ UI** — モバイル・タブレット・デスクトップ対応
- **HSL ベースカラーシステム** — 1色選ぶだけで WCAG AA 準拠の配色を自動生成

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

アナリスト目標株価と現在株価の乖離率をベースに、ファンダメンタルズの質を加味。

| 指標 | ウェイト |
|------|----------|
| 目標株価乖離率 | 40% |
| アナリスト推奨 | 15% |
| PER (予想) 割安度 | 15% |
| EPS 成長率 | 15% |
| RSI 売られすぎ | 15% |

### スクイーズスコア（米国株のみ）

| 指標 | ウェイト |
|------|----------|
| 空売り比率 (% of Float) | 40% |
| Days to Cover | 30% |
| 空売り前月比変化 | 15% |
| モメンタムスコア | 15% |

### 相対強度 (RS) 判定

銘柄リターンからベンチマークリターンを差し引き、セクター連れ高を識別。

| ラベル | 条件 | 意味 |
|--------|------|------|
| 本命 | RS1M > +2% かつ RS3M > 0 | 短期も中期もセクター比で強い |
| 短期 | RS1M > +2% かつ RS3M ≤ 0 | 短期は強いが中期は弱い |
| 劣後 | RS1M ≤ +2% | セクター比で上位にいない |
| テーマ | BTC連動銘柄等 | セクター比較が不適切 |

- 米国株: セクターETF（XLK, XLV, XLF 等 11本）と比較
- 日経225: 日経平均（^N225）と比較

### 騰落線 (Advance-Decline Line)

スクリーニング対象の全銘柄から日次の上昇/下降数を集計し、累積ADL値をチャート表示。指数の上昇が全体的なものか一部銘柄の牽引かを判断。

## セットアップ

```bash
git clone https://github.com/akiraizutsu/surge.git
cd surge
pip install -r requirements.txt
python app.py
```

ブラウザで `http://localhost:5001` を開き、「スクリーニング実行」をクリック。

## API

| メソッド | パス | 用途 |
|---------|------|------|
| POST | `/api/screen` | スクリーニング開始 |
| GET | `/api/status` | 進捗確認 |
| GET | `/api/result` | 最新結果取得 |
| GET | `/api/results` | 全インデックス結果取得（DB復元対応） |
| GET | `/api/breadth/<index>` | 騰落線データ取得 |
| GET | `/api/watchlist` | ウォッチリスト取得 |
| POST | `/api/watchlist` | 銘柄追加 |
| DELETE | `/api/watchlist/<ticker>` | 銘柄削除 |
| GET | `/api/history` | 過去セッション一覧 |
| GET | `/api/history/<id>` | 過去セッション詳細 |

## 技術スタック

| レイヤー | 技術 |
|----------|------|
| バックエンド | Python 3.12 / Flask / pandas / numpy |
| データ取得 | yfinance / Wikipedia (構成銘柄) |
| データベース | SQLite (sqlite3 標準ライブラリ) |
| フロントエンド | Tailwind CSS (CDN) / Chart.js 4 / Vanilla JS |
| デプロイ | Railway (Hobby plan + Persistent Volume) |

## 免責事項

本ツールは教育・研究目的で作成されています。投資判断への利用は自己責任でお願いします。データは Yahoo Finance から取得しており、正確性を保証するものではありません。

## ライセンス

[MIT](LICENSE)
