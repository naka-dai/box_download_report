"""Get and download the latest report from Box Reports folder."""

import os
import json
import csv
import logging
from boxsdk import Client, JWTAuth
from box_reports_fetcher import BoxReportsFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_latest_report():
    """Get the latest report."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"
        reports_folder_id = "248280918136"

        logger.info("="*80)
        logger.info("最新レポート取得")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Create reports fetcher
        fetcher = BoxReportsFetcher(client, reports_folder_id)

        # Get latest report (no name filter, just the newest CSV)
        logger.info("\n最新のCSVレポートを検索中...")
        latest = fetcher.get_latest_report(max_age_days=1)

        if not latest:
            logger.warning("レポートが見つかりませんでした")
            return False

        logger.info(f"\n✓ 最新レポートが見つかりました:")
        logger.info(f"  ファイル名: {latest['name']}")
        logger.info(f"  ID: {latest['id']}")
        logger.info(f"  Modified: {latest['modified_at']}")
        logger.info(f"  Size: {latest['size']:,} bytes")
        if 'parent_name' in latest:
            logger.info(f"  親フォルダ: {latest['parent_name']}")

        # Download the report
        output_dir = r"C:\box_reports"
        output_path = os.path.join(output_dir, latest['name'])

        logger.info(f"\nダウンロード中: {output_path}")
        success = fetcher.download_report(latest['id'], output_path)

        if not success:
            logger.error("ダウンロード失敗")
            return False

        logger.info(f"✓ ダウンロード完了")

        # Analyze the CSV
        logger.info("\n" + "="*80)
        logger.info(f"CSVファイル分析: {latest['name']}")
        logger.info("="*80)

        # Try different encodings
        encodings = ['utf-8', 'utf-8-sig', 'cp932', 'shift-jis']
        data = None

        for encoding in encodings:
            try:
                with open(output_path, 'r', encoding=encoding) as f:
                    reader = csv.reader(f)
                    data = list(reader)
                logger.info(f"✓ エンコーディング: {encoding}")
                break
            except UnicodeDecodeError:
                continue

        if not data:
            logger.error(f"CSVファイルを読み込めませんでした")
            return False

        logger.info(f"総行数: {len(data):,} 行")

        # Show header
        if len(data) > 0:
            logger.info(f"\nヘッダー（{len(data[0])} 列）:")
            for i, col in enumerate(data[0], 1):
                logger.info(f"  {i:2d}. {col}")

        # Show data summary
        if len(data) > 1:
            logger.info(f"\nデータ行数: {len(data) - 1:,} 行")

            logger.info(f"\n最初の3行:")
            for i, row in enumerate(data[1:min(4, len(data))], 1):
                logger.info(f"\n行 {i}:")
                for j, (col_name, value) in enumerate(zip(data[0], row), 1):
                    logger.info(f"  {col_name}: {value}")

            # Check column types
            if len(data) > 0:
                header_lower = [col.lower() for col in data[0]]

                # Find important columns
                date_cols = [i for i, col in enumerate(header_lower) if 'date' in col or '日' in col]
                user_cols = [i for i, col in enumerate(header_lower) if 'user' in col or 'ユーザー' in col]
                action_cols = [i for i, col in enumerate(header_lower) if 'action' in col or '操作' in col or 'event' in col]
                file_cols = [i for i, col in enumerate(header_lower) if 'file' in col or 'item' in col or '対象' in col or 'ファイル' in col]

                logger.info(f"\n重要な列:")
                logger.info(f"  日付関連列: {[data[0][i] for i in date_cols]}")
                logger.info(f"  ユーザー関連列: {[data[0][i] for i in user_cols]}")
                logger.info(f"  操作関連列: {[data[0][i] for i in action_cols]}")
                logger.info(f"  ファイル関連列: {[data[0][i] for i in file_cols]}")

                # Count unique values in action column
                if action_cols:
                    action_idx = action_cols[0]
                    actions = {}
                    for row in data[1:]:
                        if action_idx < len(row):
                            action = row[action_idx]
                            actions[action] = actions.get(action, 0) + 1

                    logger.info(f"\n操作タイプの集計:")
                    for action, count in sorted(actions.items(), key=lambda x: x[1], reverse=True):
                        logger.info(f"  {action}: {count:,} 件")

        logger.info("\n" + "="*80)
        logger.info("完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = get_latest_report()
    if success:
        print("\n[OK] 成功")
    else:
        print("\n[FAILED] 失敗")
