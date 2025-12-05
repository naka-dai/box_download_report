# Box Download Report Batch

Box ダウンロード履歴集計バッチプログラム

## 概要

特別権限フォルダに保存された重要ファイルについて、Box API を使用してダウンロードログを詳細に取得し、SQLite データベースに蓄積します。監査用途・異常検知・月次分析を可能にし、異常発生時は自動メール通知（詳細CSV添付）を行います。

## 主な機能

- Box ダウンロードログ収集（JWT認証）
- User Activity CSV 自動ダウンロード・インポート
- SQLite データベースへの蓄積
- 監査用詳細ログの出力
- **異常値検知（4種類）**
  - ダウンロード数閾値超過
  - ユニークファイル数閾値超過
  - 勤務時間外アクセス検知
  - スパイク検知（短時間大量ダウンロード）
- 異常ユーザー詳細 CSV 出力（複合アラートタイプ対応、DL/PV内訳表示）
- **重大度ベースのメール通知（DL/PV内訳付き）**
  - Critical（緊急）: 閾値の10倍以上超過 → 緊急アラートメール
  - High（警告）: 閾値の5〜10倍超過 → 警告メール
  - Normal（通常）: 閾値の5倍未満 → 通常通知
- 除外ユーザー設定（システムアカウント等を検知対象外に）
- 月次サマリー生成
- **HTMLダッシュボード生成**
  - ダウンロード分析ダッシュボード
  - プレビュー分析ダッシュボード
  - Chart.js によるインタラクティブなグラフ表示
  - 完全オフライン動作
- **クラウドデプロイ**
  - Netlify 自動デプロイ
  - Cloudflare Pages 自動デプロイ

## 必要環境

- Windows 10 以降
- Python 3.10+
- Box Enterprise アカウント（JWT 認証設定済み）

## インストール

### 1. リポジトリのクローン

```bash
git clone <repository_url>
cd box_download_report
```

### 2. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 3. 設定ファイルの準備

#### .env ファイルの作成

`.env.sample` をコピーして `.env` を作成し、環境に合わせて編集します。

```bash
copy .env.sample .env
```

#### Box JWT 認証設定

1. Box Developer Console で JWT アプリケーションを作成
2. `config.json` をダウンロード
3. `.env` の `BOX_CONFIG_PATH` に `config.json` のパスを指定

必要な権限:
- Read all files and folders stored in Box
- Manage enterprise properties
- Read and write all files and folders stored in Box (for admin_logs access)

## 設定項目

### Box API 設定

- `BOX_CONFIG_PATH`: Box JWT 認証設定ファイルのパス
- `BOX_ROOT_FOLDER_ID`: 監視対象フォルダの ID

### 出力設定

- `REPORT_OUTPUT_DIR`: レポート出力先ディレクトリ
- `ACCESS_LOG_OUTPUT_DIR`: アクセスログ出力先ディレクトリ
- `ANOMALY_OUTPUT_DIR`: 異常検知結果出力先ディレクトリ
- `DB_PATH`: SQLite データベースファイルのパス

### 異常検知設定

- `ALERT_ENABLED`: 異常検知の有効/無効
- `ALERT_USER_DOWNLOAD_COUNT_THRESHOLD`: ユーザーあたり1日のダウンロード数閾値（デフォルト: 150）
- `ALERT_USER_UNIQUE_FILES_THRESHOLD`: ユーザーあたり1日のユニークファイル数閾値（デフォルト: 100）
- `BUSINESS_HOURS_START`: 勤務時間開始（JST、例: 08:00）
- `BUSINESS_HOURS_END`: 勤務時間終了（JST、例: 20:00）
- `ALERT_OFFHOUR_DOWNLOAD_THRESHOLD`: ユーザーあたり1日の勤務時間外ダウンロード閾値（デフォルト: 50）
- `ALERT_SPIKE_WINDOW_MINUTES`: スパイク検知時間窓（分、デフォルト: 60）
- `ALERT_SPIKE_DOWNLOAD_THRESHOLD`: ユーザーあたりスパイク検知ダウンロード数閾値（デフォルト: 50）
- `ALERT_EXCLUDE_USERS`: 除外ユーザー（カンマ区切り、システムアカウント等を検知対象外に）

#### 重大度レベル

閾値超過率に応じてメールの緊急度が変わります：

| レベル | 超過率 | メール件名例 |
|--------|--------|-------------|
| Critical | 10倍以上 | 🚨🚨🚨 【緊急】大量ダウンロード検知（閾値の17倍超過）🚨🚨🚨 |
| High | 5〜10倍 | ⚠️ 【警告】異常ダウンロード検知（閾値の7倍超過）|
| Normal | 5倍未満 | Box Download Anomalies Detected |

Critical レベルでは、メール本文に緊急対応アクション（アカウント停止、情報漏洩確認等）が記載されます。

#### アラートメール本文の内容

アラートメールには各ユーザーの詳細情報が日本語で記載されます：

```
検知ユーザー数: 1人

【旭泉　勇太】（yuuta.hiizumi@nikko-net.co.jp）
  ダウンロード: 37件 / プレビュー: 41件 / 合計: 78件
  検知理由: スパイク: 60分間に50件（閾値: 50）
```

- **ダウンロード/プレビュー内訳**: 各ユーザーのDL件数とPV件数を明示
- **検知理由**: どの閾値に抵触したかを日本語で表示

### メール設定

- `SMTP_HOST`: SMTP サーバーホスト（Gmail: smtp.gmail.com）
- `SMTP_PORT`: SMTP ポート（デフォルト: 587）
- `SMTP_USE_TLS`: TLS 使用（True/False）
- `SMTP_USER`: SMTP ユーザー名（Gmail: メールアドレス）
- `SMTP_PASSWORD`: SMTP パスワード（Gmail: アプリパスワード）
- `ALERT_MAIL_FROM`: 送信元メールアドレス
- `ALERT_MAIL_RECIPIENTS_CSV`: 送信先メールアドレスCSVファイルのパス（デフォルト: mail_recipients.csv）
- `ALERT_MAIL_SUBJECT_PREFIX`: メール件名プレフィックス
- `ALERT_ATTACHMENT_MAX_ROWS`: 添付ファイルの最大行数（デフォルト: 5000）

#### 送信先CSVファイル（mail_recipients.csv）

```csv
email,name,enabled
user1@example.com,ユーザー1,1
user2@example.com,ユーザー2,1
disabled@example.com,無効ユーザー,0
```

- `enabled`: 1=有効、0=無効（無効化すると送信対象外）

#### Gmail SMTP設定

Gmailを使用する場合は、アプリパスワードの設定が必要です:

1. Googleアカウントで2段階認証を有効化
2. https://myaccount.google.com/apppasswords にアクセス
3. アプリパスワードを生成（例: BoxReport）
4. 生成された16桁のパスワードを`SMTP_PASSWORD`に設定

## 実行方法

### 手動実行

```bash
python main.py
```

### ダッシュボード生成

#### オールインワンダッシュボード（推奨）

```bash
python generate_allinone_dashboard.py
```

生成されたダッシュボード: `data/dashboard_allinone.html`

1つのHTMLファイルで3つのダッシュボードをタブ切り替えで表示:
- **統合タブ**: ダウンロード＋プレビュー統合分析
- **ダウンロードタブ**: ダウンロードのみ集計
- **プレビュータブ**: プレビューのみ集計

各タブ共通機能:
- 月別推移グラフ（クリックで詳細モーダル表示）
- 日別推移グラフ（直近30日）
- 時間帯別グラフ（24時間分布）
- トップユーザーテーブル（トップ10/全員切り替え可能）
- トップファイルテーブル（ユーザー詳細ツールチップ表示）
- 完全オフライン動作（Chart.js組み込み）

#### 統合ダッシュボード

```bash
python generate_integrated_dashboard.py
```

生成されたダッシュボード: `data/dashboard_integrated.html`

ダウンロードとプレビューを統合した分析ダッシュボード:
- ダウンロード/プレビュー比率表示
- 月別推移（積み上げグラフ）
- 日別推移（2系列比較）
- 時間帯別分布（積み上げグラフ）
- トップユーザー（ダウンロード/プレビュー別表示）
- トップファイル（ダウンロード/プレビュー別表示）

#### ダウンロードのみ集計ダッシュボード

```bash
python generate_dashboard_v2.py
```

生成されたダッシュボード: `data/dashboard.html`

#### プレビューのみ集計ダッシュボード

```bash
python generate_preview_dashboard.py
```

生成されたダッシュボード: `data/dashboard_preview.html`

共通機能:
- 月別推移グラフ（クリックで詳細表示）
- 日別推移グラフ（マウスオーバーでユーザー内訳表示）
- 時間帯別グラフ（マウスオーバーでユーザー内訳表示）
- トップユーザーテーブル（上位10人/全員切り替え可能）
- トップファイルテーブル（ユーザー詳細表示）

#### 期間フィルター付きダッシュボード（図面活用状況）★推奨

```bash
python generate_period_allinone_full.py
```

生成されたダッシュボード: `data/dashboard_period_allinone_full.html`

**Box レポート 図面活用状況** - 運用開始前後の比較分析が可能

主な機能:
- **期間フィルター**: 全期間/運用開始前（～2025-10-13）/運用開始後（2025-10-14～）を切り替え
- **3つのタブ**: 統合レポート/ダウンロードのみ集計/プレビューのみ集計
- **マウスオーバー詳細表示**: 月別/日別/時間帯別グラフでトップ5ユーザーとアクセス数を表示
- **リッチUI**: トグル表示、ユーザー詳細ツールチップ、重複率表示、DL/PVバッジ
- **完全オフライン動作**: Chart.js組み込み（1.6 MB）

**毎日自動更新**: タスクスケジューラで毎日実行することで、常に最新データを表示

### タスクスケジューラでの自動実行

#### EXE ファイルの作成

##### オールインワンバッチ（box_daily_update.py）★推奨

```bash
pyinstaller --onefile --name box_daily_update box_daily_update.py
```

生成された `dist/box_daily_update.exe` を使用します。

このEXEは以下の処理を自動的に順次実行します:
1. BoxからUser Activity CSVを自動ダウンロード・インポート
2. 期間フィルター付きダッシュボード生成
3. 異常検知処理（閾値超過ユーザーの検出）
4. **異常検出時はメール通知（CSV添付、重大度に応じた緊急度）**
5. Netlifyへの自動デプロイ（平日のみ、土日はスキップ）
6. Cloudflare Pagesへの自動デプロイ

#### タスクスケジューラの設定

1. タスクスケジューラを開く
2. 基本タスクの作成
3. 名前: `Box Daily Update - 図面活用状況レポート`
4. トリガー: 毎日深夜（例: 午前2時）
5. 操作: プログラムの開始
6. プログラム/スクリプト: `box_daily_update.exe` のフルパス
7. 開始: プログラムのあるディレクトリ

**実行内容**:
- BoxからUser Activity CSVを自動ダウンロード → SQLiteデータベース保存
- 期間フィルター付きダッシュボード生成・更新
- 異常検知処理 → 閾値超過時は重大度に応じたメール通知
- Netlify / Cloudflare Pagesへ自動デプロイ

これにより、毎日最新のデータでダッシュボードが自動更新され、異常時はメールでアラートが届きます。

### デプロイ設定

#### Netlify

- `NETLIFY_SITE_ID`: NetlifyサイトID
- `SKIP_NETLIFY_DEPLOY`: 1でデプロイをスキップ

#### Cloudflare Pages

- `CLOUDFLARE_PAGES_PROJECT`: Cloudflare Pagesプロジェクト名
- `SKIP_CLOUDFLARE_DEPLOY`: 1でデプロイをスキップ

**ダッシュボードURL**:
- Netlify: https://box-dashboard-report.netlify.app/
- Cloudflare: https://box-dashboard-report.pages.dev/

## 出力ファイル

### 日次レポート

- `box_file_downloads_YYYYMMDD_confirmed.csv`: ファイル別ダウンロード数（前日確定値）
- `box_file_downloads_YYYYMMDD_tentative.csv`: ファイル別ダウンロード数（当日速報値）
- `box_user_file_downloads_YYYYMMDD_confirmed.csv`: ユーザー×ファイル別ダウンロード数（前日確定値）
- `box_user_file_downloads_YYYYMMDD_tentative.csv`: ユーザー×ファイル別ダウンロード数（当日速報値）
- `access_log_YYYYMMDD_confirmed.csv`: 詳細アクセスログ（前日確定値）
- `access_log_YYYYMMDD_tentative.csv`: 詳細アクセスログ（当日速報値）

### 異常検知レポート

- `anomaly_details_YYYYMMDD_confirmed.csv`: 異常候補ユーザーの詳細（前日確定値）
- `anomaly_details_YYYYMMDD_tentative.csv`: 異常候補ユーザーの詳細（当日速報値）
- `anomaly_details_YYYYMMDD_daily.csv`: アラート検知時の詳細（メール添付用）

#### 異常タイプ（anomaly_types列）

CSVの`anomaly_types`列には検知されたアラートタイプが記録されます：

| タイプ | 説明 |
|--------|------|
| `download_count` | ダウンロード数が閾値超過 |
| `unique_files` | ユニークファイル数が閾値超過 |
| `offhour` | 勤務時間外ダウンロードが閾値超過 |
| `spike` | 短時間に大量ダウンロード |

複合アラート時は `download_count+unique_files+spike` のように `+` で連結されます。

#### event_type列（DL/PV識別）

CSVの`event_type`列には各イベントがダウンロードかプレビューかが日本語で記録されます：

| 値 | 説明 |
|--------|------|
| `ダウンロード` | ファイルをダウンロードしたイベント |
| `プレビュー` | ファイルをプレビュー表示したイベント |

これにより、アラート発生時にユーザーがダウンロードを行ったのかプレビューだけなのかを一目で確認できます。

### 月次レポート

- `monthly_user_summary_YYYYMM.csv`: ユーザー別月次サマリー
- `monthly_file_summary_YYYYMM.csv`: ファイル別月次サマリー

### ダッシュボード（HTML）

- `data/dashboard_period_allinone_full.html`: **期間フィルター付きダッシュボード（図面活用状況）★推奨** - 運用開始前後の比較分析、マウスオーバー詳細表示（1.6 MB）
- `data/dashboard_allinone.html`: オールインワンダッシュボード（タブ切り替えで3つのビューを表示）
- `data/dashboard_allinone_full.html`: オールインワンダッシュボード完全版（全リッチUI機能搭載、37MB）
- `data/dashboard_integrated.html`: 統合ダッシュボード（ダウンロード＋プレビュー）
- `data/dashboard.html`: ダウンロードのみ集計ダッシュボード
- `data/dashboard_preview.html`: プレビューのみ集計ダッシュボード

## データベース構造

### downloads テーブル

ダウンロードイベントの詳細ログを格納します。

主要カラム:
- `event_id`: Box イベント ID
- `user_login`: ユーザーログイン ID
- `file_id`: ファイル ID
- `download_at_jst`: ダウンロード日時（JST）
- その他、IP アドレス、クライアント種別など

### anomalies テーブル

検出された異常を記録します。

### monthly_user_summary テーブル

ユーザー別月次サマリーを格納します。

### monthly_file_summary テーブル

ファイル別月次サマリーを格納します。

## ログファイル

実行ログは `box_download_batch.log` に出力されます。

## 重要：EXE更新手順

### ⚠️ EXEリビルド後は必ずdeploymentフォルダにコピーすること

**絶対に忘れてはいけない手順**:

1. `pyinstaller` でEXEをリビルド
2. **必ず** `dist\box_daily_update.exe` を `deployment\box_daily_update\` にコピー
3. **コピー後、タイムスタンプとファイルサイズで正しくコピーされたことを必ず確認**（これが最重要）

```powershell
# コピー方法（どちらでもOK）
# 方法1: cmd /c copy
cmd /c copy "dist\box_daily_update.exe" "deployment\box_daily_update\box_daily_update.exe"

# 方法2: PowerShell Copy-Item
Copy-Item -Path "dist\box_daily_update.exe" -Destination "deployment\box_daily_update\box_daily_update.exe" -Force

# ★★★ 必須：コピー後の確認（これを怠ると事故になる） ★★★
Get-Item "deployment\box_daily_update\box_daily_update.exe" | Select-Object Name, LastWriteTime, Length
# distフォルダのEXEと同じタイムスタンプ・サイズであることを目視確認すること
Get-Item "dist\box_daily_update.exe" | Select-Object Name, LastWriteTime, Length
```

**過去の事故（2025-11-29）**:
- EXEリビルド後、コピーコマンドは実行したが**成功したかどうかの確認を怠った**
- 結果、古いEXEがタスクスケジューラで実行されてアラートメールが誤送信された
- **教訓**: コピーコマンド自体は `cmd /c copy` でも `Copy-Item` でも問題ない。**重要なのはコピー後にタイムスタンプを確認すること**

### タスクスケジューラ実行パス

タスクスケジューラは以下のバッチファイルを実行:
- `C:\dev\python\box_download_report\deployment\box_daily_update\run_with_netlify_deploy.bat`

このバッチは同じディレクトリの `box_daily_update.exe` を呼び出す。

## トラブルシューティング

### Box API 認証エラー

- `config.json` のパスが正しいか確認
- Box Developer Console でアプリケーションが承認されているか確認
- JWT アプリケーションに必要な権限が付与されているか確認

### データベースエラー

- `DB_PATH` で指定したディレクトリに書き込み権限があるか確認
- SQLite データベースファイルが破損していないか確認

### メール送信エラー

- SMTP サーバーの設定が正しいか確認
- ファイアウォールで SMTP ポートがブロックされていないか確認
- 認証情報（ユーザー名・パスワード）が正しいか確認

## ライセンス

このプロジェクトは社内利用を想定しています。

## サポート

問題が発生した場合は、ログファイルを確認の上、管理者に連絡してください。
