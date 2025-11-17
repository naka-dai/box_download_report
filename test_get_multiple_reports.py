"""Get multiple recent reports from Box Reports folder."""

import os
import json
import logging
from boxsdk import Client, JWTAuth
from box_reports_fetcher import BoxReportsFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_multiple_reports():
    """Get the latest 5 reports."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"
        reports_folder_id = "248280918136"

        logger.info("="*80)
        logger.info("複数の最新レポート取得")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Create reports fetcher
        fetcher = BoxReportsFetcher(client, reports_folder_id)

        # Get recent reports (last 365 days - effectively all recent reports)
        logger.info("\n最近のレポートを検索中...")
        recent_reports = fetcher.list_recent_reports(days=365)

        if not recent_reports:
            logger.warning("レポートが見つかりませんでした")
            return False

        logger.info(f"\n✓ {len(recent_reports)}件のレポートが見つかりました\n")

        # Show top 10
        for i, report in enumerate(recent_reports[:10], 1):
            logger.info(f"{i:2d}. {report['name']}")
            logger.info(f"     ID: {report['id']}")
            logger.info(f"     Modified: {report['modified_at']}")
            logger.info(f"     Size: {report['size']:,} bytes")
            if 'parent_name' in report:
                logger.info(f"     親フォルダ: {report['parent_name']}")
            logger.info("")

        # Download the latest 3 reports
        logger.info("="*80)
        logger.info("最新3件をダウンロード")
        logger.info("="*80)

        output_dir = r"C:\box_reports"
        os.makedirs(output_dir, exist_ok=True)

        downloaded = []
        for i, report in enumerate(recent_reports[:3], 1):
            logger.info(f"\n[{i}/3] ダウンロード中: {report['name']}")
            output_path = os.path.join(output_dir, report['name'])

            success = fetcher.download_report(report['id'], output_path)

            if success:
                file_size = os.path.getsize(output_path)
                logger.info(f"  ✓ 完了: {file_size:,} bytes")
                downloaded.append(output_path)
            else:
                logger.error(f"  ✗ 失敗")

        logger.info("\n" + "="*80)
        logger.info(f"ダウンロード完了: {len(downloaded)}/3 件")
        logger.info("="*80)

        if downloaded:
            logger.info("\nダウンロードされたファイル:")
            for path in downloaded:
                logger.info(f"  - {path}")

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = get_multiple_reports()
    if success:
        print("\n[OK] 成功")
    else:
        print("\n[FAILED] 失敗")
