"""Download and analyze the User Activity report."""

import os
import json
import csv
import logging
from boxsdk import Client, JWTAuth

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_and_analyze_report():
    """Download the User Activity report and analyze its contents."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"
        report_folder_id = "351470992741"  # User Activity run on 2025-11-17 13-39-03

        logger.info("="*80)
        logger.info("User Activityレポートをダウンロードして分析")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Get folder contents
        logger.info(f"\nレポートフォルダ {report_folder_id} の内容を取得中...")
        folder = client.folder(report_folder_id).get()
        logger.info(f"フォルダ名: {folder.name}")

        items = folder.get_items(limit=100, fields=['id', 'name', 'type', 'modified_at', 'size', 'created_at'])
        item_list = list(items)

        logger.info(f"アイテム数: {len(item_list)}\n")

        # Download all CSV files
        output_dir = r"C:\box_reports"
        os.makedirs(output_dir, exist_ok=True)

        csv_files = []
        for item in item_list:
            if item.type == 'file' and item.name.lower().endswith('.csv'):
                logger.info(f"CSVファイル: {item.name}")
                logger.info(f"  ID: {item.id}")
                logger.info(f"  Size: {item.size:,} bytes")

                output_path = os.path.join(output_dir, item.name)
                logger.info(f"  ダウンロード中: {output_path}")

                file_obj = client.file(item.id)
                with open(output_path, 'wb') as f:
                    file_obj.download_to(f)

                logger.info(f"  ✓ ダウンロード完了\n")
                csv_files.append(output_path)

        if not csv_files:
            logger.warning("CSVファイルが見つかりませんでした")
            return False

        # Analyze each CSV file
        for csv_path in csv_files:
            logger.info("="*80)
            logger.info(f"CSVファイル分析: {os.path.basename(csv_path)}")
            logger.info("="*80)

            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'cp932', 'shift-jis']
            data = None

            for encoding in encodings:
                try:
                    with open(csv_path, 'r', encoding=encoding) as f:
                        reader = csv.reader(f)
                        data = list(reader)
                    logger.info(f"✓ エンコーディング: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue

            if not data:
                logger.error(f"CSVファイルを読み込めませんでした")
                continue

            logger.info(f"総行数: {len(data):,} 行")

            # Show header
            if len(data) > 0:
                logger.info(f"\nヘッダー（{len(data[0])} 列）:")
                for i, col in enumerate(data[0], 1):
                    logger.info(f"  {i:2d}. {col}")

            # Show first few data rows
            if len(data) > 1:
                logger.info(f"\n最初のデータ行（最大5行）:")
                for i, row in enumerate(data[1:min(6, len(data))], 1):
                    logger.info(f"\n行 {i}:")
                    for j, (col_name, value) in enumerate(zip(data[0], row), 1):
                        logger.info(f"  {col_name}: {value}")

            # Check if it contains download events
            if len(data) > 0:
                header = [col.lower() for col in data[0]]

                # Look for relevant columns
                has_event_type = any('event' in col or 'type' in col or 'action' in col for col in header)
                has_file = any('file' in col or 'item' in col for col in header)
                has_user = any('user' in col for col in header)
                has_date = any('date' in col or 'time' in col for col in header)

                logger.info(f"\n列の分析:")
                logger.info(f"  イベントタイプ列: {'あり' if has_event_type else 'なし'}")
                logger.info(f"  ファイル関連列: {'あり' if has_file else 'なし'}")
                logger.info(f"  ユーザー関連列: {'あり' if has_user else 'なし'}")
                logger.info(f"  日時関連列: {'あり' if has_date else 'なし'}")

                # Count rows with download-related activity
                download_count = 0
                for row in data[1:]:
                    row_str = ' '.join(row).lower()
                    if 'download' in row_str or 'ダウンロード' in row_str:
                        download_count += 1

                logger.info(f"\nダウンロード関連の行数: {download_count:,} 行")

        logger.info("\n" + "="*80)
        logger.info("分析完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = download_and_analyze_report()
    if success:
        print("\n[OK] ダウンロードと分析が成功しました")
    else:
        print("\n[FAILED] 失敗しました")
