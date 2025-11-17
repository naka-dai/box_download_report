"""
CSV Importer
Box管理コンソールのUser ActivityレポートCSVをSQLiteにインポート
"""

import os
import csv
import json
import logging
from datetime import datetime
from typing import List
from db import Database

logger = logging.getLogger(__name__)


class CSVImporter:
    """CSVファイルをSQLiteデータベースにインポート"""

    def __init__(self, db: Database):
        """
        Initialize the CSV Importer.

        Args:
            db: Database instance
        """
        self.db = db

    def import_user_activity_csv(self, csv_path: str) -> int:
        """
        Import User Activity CSV file into database.

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of records imported
        """
        try:
            logger.info(f"Importing CSV: {csv_path}")

            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'cp932', 'shift-jis']
            data = None

            for encoding in encodings:
                try:
                    with open(csv_path, 'r', encoding=encoding) as f:
                        reader = csv.DictReader(f)
                        data = list(reader)
                    logger.info(f"Successfully read CSV with encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

            if not data:
                logger.error(f"Failed to read CSV file: {csv_path}")
                return 0

            logger.info(f"Read {len(data):,} rows from CSV")

            # Import download events only
            imported = 0
            skipped = 0

            for row in data:
                # Get operation type (操作)
                operation = row.get('操作', '').strip()

                # Only import download events
                if operation != 'ダウンロード':
                    skipped += 1
                    continue

                try:
                    # Parse date (日付) - format: "2025-11-10 06:08:38"
                    date_str = row.get('日付', '').strip()
                    if not date_str:
                        logger.warning(f"Skipping row with no date: {row}")
                        skipped += 1
                        continue

                    # Parse datetime (assuming JST)
                    download_at = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

                    # Extract fields
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
                    # Convert datetime to ISO format strings
                    download_at_utc = download_at.strftime('%Y-%m-%dT%H:%M:%S')
                    download_at_jst = download_at.strftime('%Y-%m-%dT%H:%M:%S')

                    # Create unique event_id from row data
                    event_id = f"{user_id}_{file_id}_{download_at.strftime('%Y%m%d%H%M%S')}"

                    # Build event dictionary matching db.py signature
                    event = {
                        'event_id': event_id,
                        'stream_type': 'user_activity_csv',
                        'event_type': 'DOWNLOAD',
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
                    self.db.insert_download_event(event)

                    imported += 1

                except Exception as e:
                    logger.warning(f"Error importing row: {e}, row: {row}")
                    skipped += 1
                    continue

            logger.info(f"Import complete: {imported:,} downloads imported, {skipped:,} rows skipped")

            return imported

        except Exception as e:
            logger.error(f"Error importing CSV: {e}", exc_info=True)
            return 0

    def import_multiple_csvs(self, csv_paths: List[str]) -> int:
        """
        Import multiple CSV files.

        Args:
            csv_paths: List of CSV file paths

        Returns:
            Total number of records imported
        """
        total_imported = 0

        for csv_path in csv_paths:
            if not os.path.exists(csv_path):
                logger.warning(f"CSV file not found: {csv_path}")
                continue

            imported = self.import_user_activity_csv(csv_path)
            total_imported += imported

        logger.info(f"Total imported from {len(csv_paths)} files: {total_imported:,} downloads")

        return total_imported
