# Box ダウンロード履歴集計バッチ 仕様書（SQLite 対応フル統合版）

本仕様書は、  
- Box ダウンロードログ収集  
- 監査用詳細ログ  
- SQLite DB への蓄積  
- 異常値検知（閾値・勤務時間外・スパイク）  
- 異常ユーザー詳細 CSV 出力  
- メール通知（CSV 添付）  
- 月次サマリー  
までを含む完全版です。

Claude Code に渡すことで、この仕様通りのバッチを構築できます。

---

# 1. 目的

特別権限フォルダに保存された重要ファイルについて、

- ダウンロードログを詳細に取得し  
- SQLite DB に蓄積し  
- 監査用途・異常検知・月次分析を可能にし  
- 異常発生時は自動メール通知（詳細CSV添付）する

Python + Box API を用いたバッチを作成する。

---

# 2. 実行環境

- Windows 10 以降  
- Python 3.10+  
- SQLite（標準ライブラリ `sqlite3` を使用）  
- VS Code + Claude Code  
- PyInstaller による EXE 化

---

# 3. Box API 構成

- JWT 認証（Server Authentication）
- 必要権限：
  - Read all files
  - Read enterprise events（admin_logs）
- `config.json` を使用

---

# 4. 設定（.env）

```env
# Box 認証
BOX_CONFIG_PATH=C:\path\to\config.json
BOX_ROOT_FOLDER_ID=1234567890

# 出力フォルダ
REPORT_OUTPUT_DIR=C:\box_reports
ACCESS_LOG_OUTPUT_DIR=C:\box_reports
ANOMALY_OUTPUT_DIR=C:\box_reports

# SQLite DB
DB_PATH=C:\box_reports\box_audit.db

# ===== 異常検知 =====
ALERT_ENABLED=True
ALERT_USER_DOWNLOAD_COUNT_THRESHOLD=200
ALERT_USER_UNIQUE_FILES_THRESHOLD=100

# 勤務時間帯（JST）
BUSINESS_HOURS_START=08:00
BUSINESS_HOURS_END=20:00
ALERT_OFFHOUR_DOWNLOAD_THRESHOLD=50

# スパイク検知
ALERT_SPIKE_WINDOW_MINUTES=60
ALERT_SPIKE_DOWNLOAD_THRESHOLD=100

# ===== メール送信 =====
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USE_TLS=True
SMTP_USER=alert@example.com
SMTP_PASSWORD=xxxxxx
ALERT_MAIL_FROM=alert@example.com
ALERT_MAIL_TO=security@example.com,admin@example.com
ALERT_MAIL_SUBJECT_PREFIX=[BoxDL Alert]

# 添付ファイルの最大行数
ALERT_ATTACHMENT_MAX_ROWS=5000
```

---

# 5. 集計対象期間

1回の実行で：

- 前日（確定値）  
  → 前日 00:00〜24:00（JST）

- 当日（速報値）  
  → 当日 00:00〜実行時刻（JST）

---

# 6. SQLite データベース仕様（★今回追加の主目的）

DB は下記のようにデータを蓄積する。  
初回起動時に自動でテーブル作成する。

---

## 6.1 DB：テーブル一覧

### ◆ downloads（生イベントログ／詳細ログ）

| カラム名 | 型 | 説明 |
|---------|----|------|
| id | INTEGER PRIMARY KEY | 自動連番 |
| event_id | TEXT | BoxイベントID |
| stream_type | TEXT | admin_logs 等 |
| event_type | TEXT | DOWNLOAD など |
| user_login | TEXT | ユーザー |
| user_name | TEXT | ユーザー名 |
| file_id | TEXT | ファイルID |
| file_name | TEXT | ファイル名 |
| download_at_utc | TEXT | ISO8601 |
| download_at_jst | TEXT | ISO8601 |
| ip_address | TEXT | 任意 |
| client_type | TEXT | 任意 |
| user_agent | TEXT | 任意 |
| raw_json | TEXT | 生イベントのJSON（将来対応用） |
| inserted_at | TEXT | DBへの登録日時 |

→ 取得したイベントはすべてこのテーブルに蓄積（重複チェックあり）

---

### ◆ anomalies（異常ログ）

| カラム | 説明 |
|--------|------|
| id | PK |
| batch_date | バッチ対象日 |
| period_type | confirmed / tentative |
| user_login | 異常ユーザー |
| anomaly_type | download_count / unique_files / offhour / spike |
| value | 異常値 |
| created_at | 登録時刻 |

---

### ◆ monthly_user_summary

| カラム | 説明 |
|--------|------|
| month | YYYY-MM |
| user_login | |
| user_name | |
| total_downloads | 月間DL数 |
| active_days | アクセス日数 |
| created_at | |

---

### ◆ monthly_file_summary

| カラム | 説明 |
|--------|------|
| month | YYYY-MM |
| file_id | |
| file_name | |
| total_downloads | |
| unique_users | |
| created_at | |

---

# 7. CSV 出力仕様

※ DB 蓄積とは別に 監査用ファイル出力も併行して実施する

## 7.1 ファイル別

`box_file_downloads_YYYYMMDD_confirmed.csv`  
`box_file_downloads_YYYYMMDD_tentative.csv`

カラム:  
| file_id | file_name | download_count |

---

## 7.2 ユーザー×ファイル別

`box_user_file_downloads_YYYYMMDD_confirmed.csv`  
`box_user_file_downloads_YYYYMMDD_tentative.csv`

カラム:  
| user_login | user_name | file_id | file_name | download_count | last_download_at |

---

## 7.3 監査用アクセスログ（詳細）

`access_log_YYYYMMDD_confirmed.csv`  
`access_log_YYYYMMDD_tentative.csv`

カラムは downloads テーブルと同等。

---

## 7.4 異常候補ユーザー詳細（メール添付）

`anomaly_details_YYYYMMDD_confirmed.csv`  
`anomaly_details_YYYYMMDD_tentative.csv`

カラム:  
| user_login | user_name | file_id | file_name | download_at | folder_path(任意) |

内容：異常候補ユーザーの全ダウンロードイベント行。

---

# 8. 異常値検知（基本）

## 8.1 基本閾値

- 1ユーザー・1期間あたりのダウンロード総数:
  - `ALERT_USER_DOWNLOAD_COUNT_THRESHOLD` 以上
- 1ユーザー・1期間あたりのユニークファイル数:
  - `ALERT_USER_UNIQUE_FILES_THRESHOLD` 以上

上記を満たすユーザーを「異常候補」とする。

---

# 9. 勤務時間外アクセス異常

## 9.1 設定

```env
BUSINESS_HOURS_START=08:00
BUSINESS_HOURS_END=20:00
ALERT_OFFHOUR_DOWNLOAD_THRESHOLD=50
```

## 9.2 ロジック

- JSTの勤務時間帯を外れた時間帯のダウンロード数をユーザー毎に集計。
- 勤務時間外ダウンロード数が  
  `ALERT_OFFHOUR_DOWNLOAD_THRESHOLD` 以上なら「勤務時間外異常候補」とする。

---

# 10. スパイク異常（短時間大量ダウンロード）

## 10.1 設定

```env
ALERT_SPIKE_WINDOW_MINUTES=60
ALERT_SPIKE_DOWNLOAD_THRESHOLD=100
```

## 10.2 ロジック

- ユーザー毎にダウンロードイベントを時系列ソート。
- `ALERT_SPIKE_WINDOW_MINUTES` 分のスライディングウィンドウで件数を数え、
  - あるウィンドウ内の件数が `ALERT_SPIKE_DOWNLOAD_THRESHOLD` を超えた場合、
    「スパイク異常候補」とする。

---

# 11. メール通知（CSV添付）

## 11.1 通知条件

- 前日 or 当日のいずれかで異常候補ユーザーが存在する場合、
  - `ALERT_ENABLED=True` であればメールを送信する。

## 11.2 添付ファイル

- `anomaly_details_YYYYMMDD_confirmed.csv`（前日分異常がある場合）
- `anomaly_details_YYYYMMDD_tentative.csv`（当日分異常がある場合）

行数が `ALERT_ATTACHMENT_MAX_ROWS` を超える場合は、  
先頭から上限件数のみを添付（全件はDBから参照）。

## 11.3 技術仕様

- Python `smtplib` + `email` モジュールで multipart/mixed メールを構築
- 添付は base64 エンコード
- 本文はプレーンテキストでよい

---

# 12. 月次サマリー

## 12.1 ユーザー別月次サマリー

- DB の downloads から月単位で集計し、  
  monthly_user_summary テーブルに INSERT、および CSV 出力も行う。

CSV ファイル名:  
`monthly_user_summary_YYYYMM.csv`

## 12.2 ファイル別月次サマリー

- 同様に monthly_file_summary を作成し、CSV出力。

CSV ファイル名:  
`monthly_file_summary_YYYYMM.csv`

---

# 13. DB 挿入フロー

1. Box Events API から対象期間のイベント取得
2. 対象フォルダ配下のファイルに限定してフィルタ
3. downloads テーブルに対して、`event_id + download_at_utc` で重複チェック
4. 新規イベントのみ INSERT
5. 日次集計・異常検知は DB or メモリ上のデータ双方から可能だが、
   - 今回は「イベントを一度 DB に入れた上で集計してもよい」
6. 異常検知結果は anomalies テーブルにも INSERT

---

# 14. コード構成（推奨）

```text
box_download_report/
  __init__.py
  config.py           # 環境変数・.env 読み込み
  box_client.py       # Box SDK / JWT クライアント
  events.py           # イベント取得ロジック
  db.py               # SQLite 接続・テーブル作成・INSERT/SELECT
  aggregator.py       # 日次集計（ファイル別・ユーザー別）
  anomaly.py          # 異常検知
  reporter.py         # CSV 出力
  mailer.py           # メール送信（添付）
  monthly_summary.py  # 月次集計
  main.py             # エントリーポイント
```

---

# 15. タスクスケジューラ実行

- PyInstaller で `main.py` から単一 EXE を作成
- タスクスケジューラで毎日深夜に EXE を実行
- 実行ユーザーは DB_PATH / 出力フォルダ / config.json へのアクセス権を持つ必要がある

---

# 16. 将来拡張（任意）

- PostgreSQL / MySQL 等への切り替え（DB 抽象層を用意する）
- 共有リンク・権限変更イベントの監視
- SIEM（Elastic, Splunk）連携
- 改ざん検出のためのログハッシュ保存
