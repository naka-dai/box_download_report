# Box Download Report Batch

Box ダウンロード履歴集計バッチプログラム

## 概要

特別権限フォルダに保存された重要ファイルについて、Box API を使用してダウンロードログを詳細に取得し、SQLite データベースに蓄積します。監査用途・異常検知・月次分析を可能にし、異常発生時は自動メール通知（詳細CSV添付）を行います。

## 主な機能

- Box ダウンロードログ収集（JWT認証）
- SQLite データベースへの蓄積
- 監査用詳細ログの出力
- 異常値検知
  - ダウンロード数閾値
  - ユニークファイル数閾値
  - 勤務時間外アクセス検知
  - スパイク検知（短時間大量ダウンロード）
- 異常ユーザー詳細 CSV 出力
- メール通知（CSV 添付）
- 月次サマリー生成

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
- `ALERT_USER_DOWNLOAD_COUNT_THRESHOLD`: ユーザーあたりダウンロード数閾値（デフォルト: 200）
- `ALERT_USER_UNIQUE_FILES_THRESHOLD`: ユーザーあたりユニークファイル数閾値（デフォルト: 100）
- `BUSINESS_HOURS_START`: 勤務時間開始（JST、例: 08:00）
- `BUSINESS_HOURS_END`: 勤務時間終了（JST、例: 20:00）
- `ALERT_OFFHOUR_DOWNLOAD_THRESHOLD`: 勤務時間外ダウンロード閾値（デフォルト: 50）
- `ALERT_SPIKE_WINDOW_MINUTES`: スパイク検知時間窓（分、デフォルト: 60）
- `ALERT_SPIKE_DOWNLOAD_THRESHOLD`: スパイク検知ダウンロード数閾値（デフォルト: 100）

### メール設定

- `SMTP_HOST`: SMTP サーバーホスト
- `SMTP_PORT`: SMTP ポート（デフォルト: 587）
- `SMTP_USE_TLS`: TLS 使用（True/False）
- `SMTP_USER`: SMTP ユーザー名
- `SMTP_PASSWORD`: SMTP パスワード
- `ALERT_MAIL_FROM`: 送信元メールアドレス
- `ALERT_MAIL_TO`: 送信先メールアドレス（カンマ区切りで複数指定可）
- `ALERT_MAIL_SUBJECT_PREFIX`: メール件名プレフィックス
- `ALERT_ATTACHMENT_MAX_ROWS`: 添付ファイルの最大行数（デフォルト: 5000）

## 実行方法

### 手動実行

```bash
python main.py
```

### タスクスケジューラでの自動実行

#### EXE ファイルの作成

```bash
pyinstaller --onefile --name box_download_batch main.py
```

生成された `dist/box_download_batch.exe` を使用します。

#### タスクスケジューラの設定

1. タスクスケジューラを開く
2. 基本タスクの作成
3. トリガー: 毎日深夜（例: 午前2時）
4. 操作: プログラムの開始
5. プログラム/スクリプト: `box_download_batch.exe` のフルパス
6. 開始: プログラムのあるディレクトリ

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

### 月次レポート

- `monthly_user_summary_YYYYMM.csv`: ユーザー別月次サマリー
- `monthly_file_summary_YYYYMM.csv`: ファイル別月次サマリー

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
