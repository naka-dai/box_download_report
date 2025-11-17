"""Test downloading reports from Box Reports folder."""

import os
import json
import logging
from boxsdk import Client, JWTAuth
from box_reports_fetcher import BoxReportsFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_download_report():
    """Test fetching and downloading reports from Box Reports folder."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"
        reports_folder_id = "248280918136"

        logger.info("="*80)
        logger.info("Box Reportsフォルダからレポート取得テスト")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Create reports fetcher
        fetcher = BoxReportsFetcher(client, reports_folder_id)

        # List recent reports (last 30 days)
        logger.info("\n" + "="*80)
        logger.info("過去30日間のレポート一覧")
        logger.info("="*80)
        recent_reports = fetcher.list_recent_reports(days=30)

        if recent_reports:
            logger.info(f"\n見つかったレポート: {len(recent_reports)} 件\n")
            for i, report in enumerate(recent_reports[:20], 1):
                logger.info(f"{i:2d}. {report['name']}")
                logger.info(f"     ID: {report['id']}")
                logger.info(f"     Modified: {report['modified_at']}")
                logger.info(f"     Size: {report['size']:,} bytes")
                logger.info("")
        else:
            logger.warning("過去30日間のレポートが見つかりませんでした")

        # Get latest report
        logger.info("\n" + "="*80)
        logger.info("最新レポートを検索")
        logger.info("="*80)
        latest = fetcher.get_latest_report()

        if latest:
            logger.info(f"\n最新レポート: {latest['name']}")
            logger.info(f"  File ID: {latest['id']}")
            logger.info(f"  Modified: {latest['modified_at']}")
            logger.info(f"  Size: {latest['size']:,} bytes")

            # Ask if user wants to download
            logger.info("\n最新レポートをダウンロードしますか？")
            logger.info("(このテストでは自動的にダウンロードします)")

            # Download to temp directory
            output_dir = r"C:\box_reports"
            output_path = os.path.join(output_dir, latest['name'])

            logger.info(f"\nダウンロード先: {output_path}")
            success = fetcher.download_report(latest['id'], output_path)

            if success:
                logger.info(f"\n✓ ダウンロード成功！")
                logger.info(f"  保存先: {output_path}")

                # Show first few lines of CSV
                if output_path.endswith('.csv'):
                    logger.info("\nCSVファイルの最初の5行:")
                    logger.info("-" * 80)
                    try:
                        with open(output_path, 'r', encoding='utf-8') as f:
                            for i, line in enumerate(f):
                                if i >= 5:
                                    break
                                logger.info(line.rstrip())
                    except UnicodeDecodeError:
                        # Try with different encoding
                        with open(output_path, 'r', encoding='utf-8-sig') as f:
                            for i, line in enumerate(f):
                                if i >= 5:
                                    break
                                logger.info(line.rstrip())
                    logger.info("-" * 80)
            else:
                logger.error("✗ ダウンロード失敗")

        else:
            logger.warning("レポートが見つかりませんでした")

        # Search for event/download reports specifically
        logger.info("\n" + "="*80)
        logger.info("イベント/ダウンロード関連レポートを検索")
        logger.info("="*80)

        for pattern in ['event', 'download', 'ダウンロード', 'イベント']:
            report = fetcher.get_latest_report(name_pattern=pattern, max_age_days=90)
            if report:
                logger.info(f"\n'{pattern}' パターンで見つかりました:")
                logger.info(f"  {report['name']}")
                logger.info(f"  Modified: {report['modified_at']}")
                break
        else:
            logger.info("\nイベント/ダウンロード関連のレポートが見つかりませんでした")
            logger.info("管理コンソールでイベントレポートを作成してください。")

        logger.info("\n" + "="*80)
        logger.info("テスト完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_download_report()
    if success:
        print("\n[OK] テスト成功")
    else:
        print("\n[FAILED] テスト失敗")
