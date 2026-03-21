# Surge — 開発ガイド

## プロジェクト概要

S&P 500 / NASDAQ 100 / 日経225 のモメンタムスクリーニングダッシュボード。
騰落率・出来高・MA乖離・MACD・RSI を複合スコアリングし、勢いのある銘柄を自動抽出する。
ショートスクイーズ分析・相対強度 (RS) 判定・ウォッチリスト・履歴機能を備える。

## 2台 Mac 並行開発ルール

- 同時作業は行わない（コミット受け渡し方式）
- **作業前**: 必ず `git pull` で最新を取得
- **作業後**: 必ず `git push` でリモートに反映
- 初回展開はディレクトリ丸ごと HDD コピー（`.env`・`venv/`・`surge.db` 含む）

## ディレクトリ構成

```
surge/
├── app.py                 # Flask アプリ（API エンドポイント・バックグラウンドスレッド管理）
├── screener.py            # スクリーニングエンジン（指標計算・RS判定・スクイーズスコア）
├── database.py            # SQLite データレイヤー（セッション・結果・ウォッチリスト CRUD）
├── requirements.txt       # Python 依存パッケージ
├── templates/
│   └── index.html         # ダッシュボード HTML（Tailwind CDN + Chart.js CDN）
├── static/
│   ├── app.js             # フロントエンドロジック（チャート・テーブル・モーダル・カラーシステム）
│   └── style.css          # カスタム CSS（Kokyū Design System 準拠）
├── KOKYU-UI-GUIDE.md      # Kokyū デザインシステム仕様書
├── surge.db               # SQLite DB（.gitignore 対象、各環境ローカル）
├── LICENSE                # MIT License
└── README.md              # プロジェクト説明
```

## 環境セットアップ

```bash
# 1. リポジトリクローン
git clone https://github.com/akiraizutsu/surge.git
cd surge

# 2. 仮想環境（推奨）
python3 -m venv venv
source venv/bin/activate

# 3. 依存パッケージ
pip install -r requirements.txt

# 4. サーバー起動
python app.py
# → http://localhost:5001
```

### 環境変数（任意）

| 変数名 | 用途 | デフォルト |
|--------|------|-----------|
| `PORT` | サーバーポート | `5001` |

現時点で `.env` は不要（外部 API キーなし）。将来追加する場合は `.env.example` をコミットし、`.env` は `.gitignore` で除外済み。

## アーキテクチャ

```
[ブラウザ] → POST /api/screen → [Flask app.py]
                                      │
                                      ├── バックグラウンドスレッド起動
                                      │     └── screener.py
                                      │           ├── Wikipedia → 銘柄リスト取得
                                      │           ├── yfinance → 価格・出来高データ
                                      │           ├── yfinance → セクターETFデータ（RS算出）
                                      │           ├── yfinance → ファンダメンタルズ・空売りデータ
                                      │           ├── 複合スコア計算（パーセンタイルランク）
                                      │           ├── RS判定（本命/短期/劣後/テーマ）
                                      │           └── スクイーズスコア計算
                                      │
                                      └── database.py → SQLite 永続化
                                            ├── screening_sessions
                                            ├── screening_results
                                            └── watchlist

[ブラウザ] ← GET /api/status（ポーリング 1秒） ← 進捗率
[ブラウザ] ← GET /api/result ← JSON レスポンス
```

## 新機能追加の手順

### バックエンド指標追加
1. `screener.py` の `screen_momentum()` に計算ロジック追加
2. `results.append()` の dict にフィールド追加
3. `run_screening()` の `ranking` 構築部に追加
4. `database.py` の `init_db()` スキーマに列追加
5. `database.py` の `save_results()` INSERT 文に列追加

### フロントエンド表示追加
1. `templates/index.html` のテーブルヘッダー `<th>` 追加
2. `static/app.js` の `renderTable()` に `<td>` 追加
3. `static/app.js` の `showDetail()` モーダルに追加
4. ソート対応: `sortable` クラスと `data-key` を `<th>` に追加

### UI変更時の注意
- `KOKYU-UI-GUIDE.md` の禁止パターンを確認
- `text-red-500` → `text-rose-400` など、Kokyū 準拠の色を使用
- ダークモード対応クラス (`dark:`) を必ずセットで記述

## コミットルール

- **絶対にコミットしないもの**: `.env`、認証 JSON、API キー、`surge.db`、`__pycache__/`
- `.gitignore` で除外済みだが、`git add -A` は使わない（`git add <ファイル名>` で個別指定）
- コミットメッセージは変更内容を簡潔に記述

## データソース

| データ | ソース | 取得方法 |
|--------|--------|----------|
| S&P 500 構成銘柄 | Wikipedia | `requests` + `pd.read_html` |
| NASDAQ 100 構成銘柄 | Wikipedia | 同上 |
| 日経225 構成銘柄 | Wikipedia | 同上 |
| 価格・出来高 | Yahoo Finance | `yfinance.download()` |
| ファンダメンタルズ | Yahoo Finance | `yfinance.Ticker().info` |
| 空売りデータ | Yahoo Finance | `yfinance.Ticker().info`（米国株のみ） |
| セクター ETF | Yahoo Finance | XLK, XLV, XLF 等 11本 + SPY |
| 日経平均 | Yahoo Finance | `^N225`（日経225の RS ベンチマーク） |
