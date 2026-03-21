# Kokyū Design System

日本語インターフェースに最適化されたデザインシステム。
Tailwind CSS (CDN) + Chart.js 環境を前提とする。

---

## AIへの指示

**このファイルを読んだら以下の順で動くこと。**

1. 「禁止パターン」を確認し、該当コードを生成しない
2. 「トークン定義」からクラス・変数を選ぶ（生の色値 `#ffffff` 等を直書きしない）
3. 「コンポーネント」の雛形を継承してコードを生成する
4. 生成後に「実装チェック」を自己検証する

**ファイルの更新ルール（AIも人も同じ）**

| 変更内容 | 触る箇所 |
|---------|---------|
| 色・フォント・スペース値の変更 | トークン定義のみ |
| 新コンポーネントの追加 | コンポーネントセクションに追記 |
| 禁止パターンの追加 | 禁止パターンに追記し理由を必ず書く |
| 設計原則の変更 | 人間が議論して決める。AIが勝手に変えない |

---

## 禁止パターン

コードレビュー・生成時の最優先ガードレール。例外を設けない。

| 禁止 | 代替 | 根拠 |
|------|------|------|
| `text-black` | `text-slate-900` | コントラスト過剰 |
| `shadow-lg` / `shadow-2xl` | `shadow-sm` | 立体感過剰（原則: Minimal） |
| `border-gray-100` | `border-slate-200` | 境界が不明瞭 |
| `rounded-none` on cards | `rounded-xl` | カードは必ず角丸 |
| `text-red-500` / `#ef4444` | `text-rose-400` / `#e8a0a0` | 彩度過剰（原則: Soft Palette） |
| `tracking-tight` | `tracking-normal` 以上 | 日本語可読性低下（原則: Breathing） |
| `prefers-color-scheme` で初期化 | `localStorage` 参照、未設定はライト | 意図しないテーマ表示（原則: Light First） |
| 英語のみのUI文言 | 日本語ラベル | ターゲットユーザーへの配慮 |
| `border-t-4` / `border-l-4` カラーバー | `border rounded-xl` 全周ボーダー | フラット原則に反する |
| 生の色値をクラスの代わりに使う | トークン経由で指定 | 原則: Semantic |

---

## 設計原則

**不変層**（普遍的なUI設計の理）

| # | 原則 | 定義 |
|---|------|------|
| 1 | Layered | Background → Surface → Text の3層構成。レイヤーを飛び越えた配色をしない |
| 2 | Contrast | WCAG 2.1 AA 準拠（テキスト対背景 4.5:1 以上） |
| 3 | Semantic | 色は用途トークンで指定する。`bg-teal-500` の直書き禁止 |
| 4 | Minimal | 1 View のアクセントカラーは原則1色 |
| 5 | Grid | スペーシングは 4px 基本単位、8px 倍数推奨 |

**日本語UI層**（言語固有の調整）

| # | 原則 | 定義 |
|---|------|------|
| 6 | Breathing | `letter-spacing: 0.02em` / `line-height: 1.8` を必ず指定する |
| 7 | Soft Palette | 警告色は `rose-400` 系。`red-500` 系は使わない |
| 8 | One Hue | テーマカラーは `--hue` 変数1つで管理し、HSL 10段階パレットを自動生成する |
| 9 | Light First | ライトがデフォルト。ダークはユーザーの明示的な操作でのみ有効にする |

---

## トークン定義

### カラー — プライマリパレット

`--hue` を変更するだけでシステム全体の色が連動する。

```css
:root { --hue: 187; } /* プロジェクトごとに変更する */
```

```js
// tailwind.config
colors: {
  primary: {
    50:  'hsl(var(--hue) 90% 96%)',
    100: 'hsl(var(--hue) 85% 90%)',
    200: 'hsl(var(--hue) 80% 80%)',
    300: 'hsl(var(--hue) 75% 68%)',
    400: 'hsl(var(--hue) 70% 55%)',
    500: 'hsl(var(--hue) 65% 45%)',  // メインアクセント
    600: 'hsl(var(--hue) 65% 38%)',  // ホバー
    700: 'hsl(var(--hue) 60% 30%)',
    800: 'hsl(var(--hue) 55% 23%)',
    900: 'hsl(var(--hue) 50% 17%)',
    950: 'hsl(var(--hue) 45% 10%)',
  }
}
```

### カラー — レイヤー対応表

| トークン名 | ライト | ダーク |
|-----------|--------|--------|
| bg-base | `bg-slate-50` | `bg-gray-950` |
| bg-surface | `bg-white` | `bg-gray-900` |
| bg-surface-inner | `bg-white` | `bg-[#111827]` |
| border-surface | `border-slate-200` | `border-gray-800` |
| bg-header | `bg-primary-600` | `bg-primary-900` |
| text-primary | `text-slate-900` | `text-gray-100` |
| text-secondary | `text-slate-500` | `text-gray-400` |
| text-body | `text-[#3d4b5f]` | `text-[#d1d5db]` |

### カラー — セマンティック

| 用途 | ライト | ダーク |
|------|--------|--------|
| 警告値・ネガティブ | `text-rose-400` | `text-rose-300` |
| 警告バッジ bg / text | `bg-rose-50` / `text-rose-500` | `bg-rose-900/20` / `text-rose-300` |
| チャート警告バー | `#e8a0a0` | `#e8a0a0` |
| ポジティブ値 | `text-emerald-600` | `text-emerald-400` |
| 成功バッジ bg / text | `bg-emerald-100` / `text-emerald-700` | `bg-emerald-900/30` / `text-emerald-400` |

### タイポグラフィ

```js
// tailwind.config
fontFamily: {
  sans: ['"Zen Kaku Gothic New"', 'Ubuntu', 'system-ui', 'sans-serif'],
  mono: ['Ubuntu Mono', 'ui-monospace', 'monospace'],
},
extend: { letterSpacing: { brand: '0.08em' } }
```

```css
body    { letter-spacing: 0.02em; line-height: 1.8; }
h1,h2,h3 { letter-spacing: 0.04em; }
```

Google Fonts 読み込み（`<head>` 内）:

```html
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Ubuntu:wght@300;400;500;700&family=Zen+Kaku+Gothic+New:wght@400;500;700&display=swap" rel="stylesheet">
```

---

## コンポーネント

### ヘッダー

```html
<header class="bg-primary-600 dark:bg-primary-900 text-white shadow-sm">
  <div class="max-w-7xl mx-auto px-8 py-4 flex items-center justify-between">
    <h1 class="text-xl font-bold tracking-brand uppercase" style="color:white">ブランド名</h1>
    <!-- コントロール群 -->
  </div>
</header>
```

### カード

```css
.card {
  background: white; border-radius: 0.75rem; padding: 1.5rem;
  box-shadow: 0 1px 2px rgba(0,0,0,0.04); border: 1px solid #e2e8f0;
}
.dark .card { background: #111827; border-color: rgba(255,255,255,0.06); }
```

### テーブル

```html
<div class="bg-white dark:bg-gray-900 rounded-xl border border-slate-200 dark:border-gray-800 overflow-x-auto">
  <table class="w-full text-sm">
    <thead>
      <tr class="border-b border-slate-200 dark:border-gray-700 bg-slate-50 dark:bg-gray-800/50">
        <th scope="col" class="px-4 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">見出し</th>
      </tr>
    </thead>
    <tbody>
      <tr class="border-b border-slate-100 dark:border-gray-800 hover:bg-slate-50 dark:hover:bg-gray-800/50 transition-colors">
        <td class="px-4 py-3">データ</td>
      </tr>
    </tbody>
  </table>
</div>
```

ソート可能列には `cursor-pointer select-none` を追加する。

### ボタン

```html
<button class="inline-flex items-center justify-center h-10 px-6 text-base font-medium
  bg-primary-500 text-white rounded-lg hover:bg-primary-700
  transition-colors active:scale-[0.98]
  disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer">
  実行する
</button>
```

### バッジ

```html
<!-- 警告 -->
<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full font-medium
  bg-rose-50 dark:bg-rose-900/20 text-rose-500 dark:text-rose-300">警告</span>

<!-- 成功 -->
<span class="inline-block px-1.5 py-0.5 text-[10px] rounded-full font-medium
  bg-emerald-100 dark:bg-emerald-900/30 text-emerald-700 dark:text-emerald-400">完了</span>
```

バッジは必ずテキストラベルを持つ。色だけで状態を伝えない。

### 空状態

```html
<div class="text-center py-16">
  <div class="w-16 h-16 bg-slate-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
    <!-- SVG アイコン -->
  </div>
  <h2 class="text-base font-medium text-slate-900 dark:text-gray-300 mb-1">まだデータがありません</h2>
  <p class="text-sm text-slate-500 dark:text-gray-500">次のアクションを案内するテキスト</p>
</div>
```

---

## 統合

### ダークモード

```js
// 初期化（即時実行）
(function() {
  const stored = localStorage.getItem('dark-mode');
  if (stored === 'true') document.documentElement.classList.add('dark');
})();

// トグル
function toggleDarkMode() {
  const isDark = document.documentElement.classList.toggle('dark');
  localStorage.setItem('dark-mode', isDark);
}
```

`prefers-color-scheme` は使わない（原則 9: Light First）。

### テーマカラー変更

```js
function applyThemeColor(hex) {
  const [h] = hexToHsl(hex); // hex → [h, s, l]
  document.documentElement.style.setProperty('--hue', h);
  localStorage.setItem('theme-color', hex);
}
```

### Chart.js

```js
Chart.defaults.font.family = "'Zen Kaku Gothic New', 'Ubuntu', system-ui, sans-serif";

function getChartColors(count) {
  const hue = parseInt(getComputedStyle(document.documentElement).getPropertyValue('--hue')) || 187;
  return Array.from({ length: count }, (_, i) =>
    `hsl(${(hue + i * 360 / count) % 360}, 65%, 55%)`
  );
}

function getChartTheme() {
  const isDark = document.documentElement.classList.contains('dark');
  return {
    gridColor: isDark ? 'rgba(255,255,255,0.08)' : 'rgba(0,0,0,0.06)',
    textColor: isDark ? '#d1d5db' : '#64748b',
  };
}
```

### トランジション

```css
* { transition-property: background-color, border-color, color; transition-duration: 0.15s; transition-timing-function: ease; }
canvas { transition: none !important; } /* Chart.js 描画崩れ防止 */
```

---

## 実装チェック

生成・実装後に必ず確認する。

**日本語・テキスト**
- [ ] ボタンラベルが日本語動詞（`実行する` `保存する` `削除する`）
- [ ] `Loading...` → `読み込み中...` / `Error:` → `エラー:` / `Last updated:` → `最終更新:`
- [ ] 空状態メッセージが日本語で次のアクションを示している

**アクセシビリティ**
- [ ] `<html lang="ja">` がある
- [ ] `aria-label` が日本語になっている
- [ ] バッジにテキストラベルがある（色だけで状態を伝えない）
- [ ] テーブルに `<th scope="col">` がある

**カラー・テーマ**
- [ ] `text-red-500` / `#ef4444` を使っていない
- [ ] `dark:` クラスがセットで書かれている
- [ ] 生の色値をクラスの代わりに直書きしていない

**フォーム**
- [ ] `<label>` がある（プレースホルダーのみは禁止）
- [ ] フォームラベル親 `<div>` に `leading-normal` がある

---

## ボイラープレート

新規 HTML の出発点。このまま動く。

```html
<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>アプリ名</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Ubuntu:wght@300;400;500;700&family=Zen+Kaku+Gothic+New:wght@400;500;700&display=swap" rel="stylesheet">
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        fontFamily: {
          sans: ['"Zen Kaku Gothic New"', 'Ubuntu', 'system-ui', 'sans-serif'],
          mono: ['Ubuntu Mono', 'ui-monospace', 'monospace'],
        },
        extend: {
          letterSpacing: { brand: '0.08em' },
          colors: {
            primary: {
              50:  'hsl(var(--hue) 90% 96%)',
              100: 'hsl(var(--hue) 85% 90%)',
              200: 'hsl(var(--hue) 80% 80%)',
              300: 'hsl(var(--hue) 75% 68%)',
              400: 'hsl(var(--hue) 70% 55%)',
              500: 'hsl(var(--hue) 65% 45%)',
              600: 'hsl(var(--hue) 65% 38%)',
              700: 'hsl(var(--hue) 60% 30%)',
              800: 'hsl(var(--hue) 55% 23%)',
              900: 'hsl(var(--hue) 50% 17%)',
              950: 'hsl(var(--hue) 45% 10%)',
            }
          }
        }
      }
    }
  </script>
  <style>
    :root { --hue: 187; }
    body { letter-spacing: 0.02em; line-height: 1.8; }
    h1, h2, h3 { letter-spacing: 0.04em; }
    * { transition-property: background-color, border-color, color; transition-duration: 0.15s; transition-timing-function: ease; }
    canvas { transition: none !important; }
  </style>
</head>
<body class="bg-slate-50 dark:bg-gray-950 text-slate-900 dark:text-gray-100 min-h-screen">

  <header class="bg-primary-600 dark:bg-primary-900 text-white shadow-sm">
    <div class="max-w-7xl mx-auto px-8 py-4 flex items-center justify-between">
      <h1 class="text-xl font-bold tracking-brand uppercase" style="color:white">アプリ名</h1>
      <button onclick="toggleDarkMode()" aria-label="ダークモード切替"
        class="p-2 rounded-lg hover:bg-white/10 transition-colors cursor-pointer">🌙</button>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-8 py-8">
    <!-- コンテンツ -->
  </main>

  <script>
    (function() {
      const stored = localStorage.getItem('dark-mode');
      if (stored === 'true') document.documentElement.classList.add('dark');
    })();
    function toggleDarkMode() {
      const isDark = document.documentElement.classList.toggle('dark');
      localStorage.setItem('dark-mode', isDark);
    }
  </script>
</body>
</html>
```

---

*Copyright © Kokyū Design System. All rights reserved.*
