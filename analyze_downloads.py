"""
Download Analysis
CSVデータをSQLiteにインポートして分析レポートを生成
"""

import os
import logging
import glob
from datetime import datetime
from db import Database
from csv_importer import CSVImporter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_downloads():
    """Analyze download data from CSV files."""
    try:
        logger.info("="*80)
        logger.info("ダウンロード分析")
        logger.info("="*80)

        # Initialize database
        db_path = r"data\box_audit.db"
        db = Database(db_path)
        db.connect()
        db.initialize_tables()

        logger.info(f"\nDatabase: {db_path}")

        # Find all CSV files
        csv_dir = r"data"
        csv_pattern = os.path.join(csv_dir, "user_activity_run_on_2025-11-17-15-19-34_*.csv")
        csv_files = glob.glob(csv_pattern)

        logger.info(f"\nFound {len(csv_files)} CSV files:")
        for csv_file in csv_files:
            file_size = os.path.getsize(csv_file)
            logger.info(f"  - {os.path.basename(csv_file)} ({file_size:,} bytes)")

        if not csv_files:
            logger.warning("No CSV files found")
            return False

        # Import CSV files
        logger.info("\n" + "="*80)
        logger.info("CSVインポート開始")
        logger.info("="*80)

        importer = CSVImporter(db)
        total_imported = importer.import_multiple_csvs(csv_files)

        logger.info(f"\n✓ インポート完了: {total_imported:,} ダウンロードレコード")

        # Analyze data
        logger.info("\n" + "="*80)
        logger.info("データ分析")
        logger.info("="*80)

        # Define admin users to exclude (by user_id from raw_json)
        admin_user_ids = ['13213941207', '16623033409', '30011740170', '32504279209']

        # Get admin user emails
        cursor = db.connection.cursor()
        cursor.execute("SELECT DISTINCT user_login FROM downloads")
        all_users = {row[0] for row in cursor.fetchall()}

        # Find admin emails by checking raw_json
        admin_emails = set()
        cursor.execute("SELECT DISTINCT user_login, raw_json FROM downloads")
        for email, raw_json in cursor.fetchall():
            if raw_json:
                try:
                    import json
                    data = json.loads(raw_json)
                    user_id = data.get('user_id', '')
                    if user_id in admin_user_ids:
                        admin_emails.add(email)
                except:
                    pass

        logger.info(f"\n除外する管理者ユーザー: {len(admin_emails)}人")
        for email in sorted(admin_emails):
            logger.info(f"  - {email}")

        # Build WHERE clause to exclude admins
        if admin_emails:
            placeholders = ','.join(['?' for _ in admin_emails])
            admin_filter = f"WHERE user_login NOT IN ({placeholders})"
            admin_params = tuple(admin_emails)
        else:
            admin_filter = ""
            admin_params = ()

        # Get download statistics
        # Total downloads (excluding admins)
        cursor.execute(f"SELECT COUNT(*) FROM downloads {admin_filter}", admin_params)
        total_downloads = cursor.fetchone()[0]
        logger.info(f"\n総ダウンロード数（管理者除く）: {total_downloads:,} 件")

        # Unique users (excluding admins)
        cursor.execute(f"SELECT COUNT(DISTINCT user_login) FROM downloads {admin_filter}", admin_params)
        unique_users = cursor.fetchone()[0]
        logger.info(f"ユニークユーザー数（管理者除く）: {unique_users} 人")

        # Unique files (excluding admins)
        cursor.execute(f"SELECT COUNT(DISTINCT file_id) FROM downloads {admin_filter}", admin_params)
        unique_files = cursor.fetchone()[0]
        logger.info(f"ダウンロードされたファイル数（管理者除く）: {unique_files:,} 件")

        # Date range (excluding admins)
        cursor.execute(f"SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads {admin_filter}", admin_params)
        min_date, max_date = cursor.fetchone()
        if min_date and max_date:
            logger.info(f"期間: {min_date} ~ {max_date}")

        # Top 10 users by download count (excluding admins)
        logger.info("\n" + "-"*80)
        logger.info("トップ10ユーザー（ダウンロード数、管理者除く）")
        logger.info("-"*80)

        if admin_emails:
            cursor.execute(f"""
                SELECT
                    user_name,
                    user_login,
                    COUNT(*) as download_count,
                    COUNT(DISTINCT file_id) as unique_files
                FROM downloads
                WHERE user_login NOT IN ({placeholders})
                GROUP BY user_login
                ORDER BY download_count DESC
                LIMIT 10
            """, admin_params)
        else:
            cursor.execute("""
                SELECT
                    user_name,
                    user_login,
                    COUNT(*) as download_count,
                    COUNT(DISTINCT file_id) as unique_files
                FROM downloads
                GROUP BY user_login
                ORDER BY download_count DESC
                LIMIT 10
            """)

        for i, (name, email, count, files) in enumerate(cursor.fetchall(), 1):
            logger.info(f"{i:2d}. {name} ({email})")
            logger.info(f"     ダウンロード数: {count:,} 件, ユニークファイル: {files:,} 件")

        # Top 10 files by download count (excluding admins)
        logger.info("\n" + "-"*80)
        logger.info("トップ10ファイル（ダウンロード数、管理者除く）")
        logger.info("-"*80)

        if admin_emails:
            cursor.execute(f"""
                SELECT
                    file_name,
                    raw_json,
                    COUNT(*) as download_count,
                    COUNT(DISTINCT user_login) as unique_users
                FROM downloads
                WHERE user_login NOT IN ({placeholders})
                GROUP BY file_id
                ORDER BY download_count DESC
                LIMIT 10
            """, admin_params)
        else:
            cursor.execute("""
                SELECT
                    file_name,
                    raw_json,
                    COUNT(*) as download_count,
                    COUNT(DISTINCT user_login) as unique_users
                FROM downloads
                GROUP BY file_id
                ORDER BY download_count DESC
                LIMIT 10
            """)

        for i, (file_name, raw_json, count, users) in enumerate(cursor.fetchall(), 1):
            # Extract parent_folder from raw_json
            folder = ''
            if raw_json:
                try:
                    import json
                    data = json.loads(raw_json)
                    folder = data.get('parent_folder', '')
                except:
                    pass

            logger.info(f"{i:2d}. {file_name}")
            if folder:
                logger.info(f"     フォルダ: {folder}")
            logger.info(f"     ダウンロード数: {count:,} 件, ユニークユーザー: {users} 人")

        # Downloads by date (excluding admins)
        logger.info("\n" + "-"*80)
        logger.info("日別ダウンロード数（最新20日、管理者除く）")
        logger.info("-"*80)

        if admin_emails:
            cursor.execute(f"""
                SELECT
                    DATE(download_at_jst) as date,
                    COUNT(*) as download_count,
                    COUNT(DISTINCT user_login) as unique_users
                FROM downloads
                WHERE user_login NOT IN ({placeholders})
                GROUP BY DATE(download_at_jst)
                ORDER BY date DESC
                LIMIT 20
            """, admin_params)
        else:
            cursor.execute("""
                SELECT
                    DATE(download_at_jst) as date,
                    COUNT(*) as download_count,
                    COUNT(DISTINCT user_login) as unique_users
                FROM downloads
                GROUP BY DATE(download_at_jst)
                ORDER BY date DESC
                LIMIT 20
            """)

        for date, count, users in cursor.fetchall():
            logger.info(f"{date}: {count:,} 件 ({users} ユーザー)")

        # Downloads by hour of day (excluding admins)
        logger.info("\n" + "-"*80)
        logger.info("時間帯別ダウンロード数（管理者除く）")
        logger.info("-"*80)

        if admin_emails:
            cursor.execute(f"""
                SELECT
                    CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
                    COUNT(*) as download_count
                FROM downloads
                WHERE user_login NOT IN ({placeholders})
                GROUP BY hour
                ORDER BY hour
            """, admin_params)
        else:
            cursor.execute("""
                SELECT
                    CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
                    COUNT(*) as download_count
                FROM downloads
                GROUP BY hour
                ORDER BY hour
            """)

        hour_data = cursor.fetchall()
        max_hour_count = max([count for _, count in hour_data]) if hour_data else 1

        for hour, count in hour_data:
            bar_length = int(40 * count / max_hour_count)
            bar = '█' * bar_length
            logger.info(f"{hour:2d}時: {bar} {count:,} 件")

        logger.info("\n" + "="*80)
        logger.info("分析完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = analyze_downloads()
    if success:
        print("\n[OK] 分析成功")
    else:
        print("\n[FAILED] 分析失敗")
