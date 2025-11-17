"""
Import Preview Data
CSVからプレビュー操作のデータをインポート
"""

import os
import logging
import glob
import csv
import json
from datetime import datetime
from db import Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def import_preview_data():
    """Import preview operations from CSV files."""
    try:
        logger.info("="*80)
        logger.info("プレビューデータインポート")
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

        # Import preview data
        total_imported = 0
        total_skipped = 0

        for csv_file in csv_files:
            logger.info(f"\nProcessing: {os.path.basename(csv_file)}")

            # Read CSV
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            logger.info(f"  Read {len(rows):,} rows")

            imported = 0
            skipped = 0

            for row in rows:
                # Get operation type (操作)
                operation = row.get('操作', '').strip()

                # Only import preview events
                if operation != 'プレビュー':
                    skipped += 1
                    continue

                try:
                    # Parse date (日付) - format: "2025-11-10 06:08:38"
                    date_str = row.get('日付', '').strip()
                    if not date_str:
                        logger.warning(f"Skipping row with no date: {row}")
                        skipped += 1
                        continue

                    download_at = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

                    # Get other fields
                    user_id = row.get('ユーザーID', '').strip()
                    user_name = row.get('ユーザー名', '').strip()
                    user_email = row.get('ユーザーのメールアドレス', '').strip()
                    ip_address = row.get('IPアドレス', '').strip()
                    file_name = row.get('対象', '').strip()
                    file_id = row.get('影響を受けるID', '').strip()
                    size_kb_str = row.get('サイズ (KB)', '').strip()
                    parent_folder = row.get('親フォルダ', '').strip()
                    details = row.get('詳細', '').strip()

                    # Parse size
                    try:
                        size_kb = float(size_kb_str) if size_kb_str else 0.0
                        file_size = int(size_kb * 1024)  # Convert to bytes
                    except:
                        file_size = 0

                    # Build event dict for database
                    download_at_utc = download_at.strftime('%Y-%m-%dT%H:%M:%S')
                    download_at_jst = download_at.strftime('%Y-%m-%dT%H:%M:%S')

                    # Create unique event_id from row data
                    event_id = f"preview_{user_id}_{file_id}_{download_at.strftime('%Y%m%d%H%M%S')}"

                    # Build event dictionary
                    event = {
                        'event_id': event_id,
                        'stream_type': 'user_activity_csv',
                        'event_type': 'PREVIEW',
                        'user_login': user_email,
                        'user_name': user_name,
                        'file_id': file_id,
                        'file_name': file_name,
                        'download_at_utc': download_at_utc,
                        'download_at_jst': download_at_jst,
                        'ip_address': ip_address,
                        'client_type': '',
                        'user_agent': '',
                        'raw_json': json.dumps({
                            'user_id': user_id,
                            'user_email': user_email,
                            'file_size': file_size,
                            'size_kb': size_kb,
                            'parent_folder': parent_folder,
                            'details': details,
                            'operation': operation
                        }, ensure_ascii=False)
                    }

                    # Insert into database
                    db.insert_download_event(event)

                    imported += 1

                except Exception as e:
                    logger.warning(f"Error importing row: {e}, row: {row}")
                    skipped += 1
                    continue

            logger.info(f"  Imported: {imported:,} preview records")
            logger.info(f"  Skipped: {skipped:,} records")

            total_imported += imported
            total_skipped += skipped

        logger.info(f"\n✓ インポート完了: {total_imported:,} プレビューレコード")
        logger.info(f"  スキップ: {total_skipped:,} レコード")

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = import_preview_data()
    if success:
        print("\n[OK] プレビューデータインポート成功")
    else:
        print("\n[FAILED] プレビューデータインポート失敗")
