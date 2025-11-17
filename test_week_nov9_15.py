"""Test fetching download events from Nov 9-15, 2024."""

import os
import json
import logging
from datetime import datetime, timezone
from boxsdk import Client, JWTAuth
from events_optimized import OptimizedEventsFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_nov9_15():
    """Fetch download events from Nov 9-15, 2024."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        folder_id = "243194687037"
        admin_user_id = "16623033409"

        logger.info("="*80)
        logger.info("11/09-11/15 ダウンロードイベント取得テスト")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Create events fetcher
        fetcher = OptimizedEventsFetcher(client, folder_id)

        # Set date range for Nov 9-15, 2024 (UTC)
        start_time = datetime(2024, 11, 9, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 11, 15, 23, 59, 59, tzinfo=timezone.utc)

        logger.info(f"期間: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} (UTC)")
        logger.info(f"      {start_time.strftime('%Y-%m-%d')} ~ {end_time.strftime('%Y-%m-%d')}")

        # Fetch download events (WITH folder filter - only folder 243194687037)
        logger.info(f"\nフォルダ {folder_id} 配下のダウンロードイベントを取得中...")
        events = fetcher.fetch_download_events(
            start_time=start_time,
            end_time=end_time,
            use_folder_filter=True  # フォルダフィルタあり（指定フォルダ配下のみ）
        )

        logger.info(f"\n✓ 取得完了！")
        logger.info(f"  ダウンロードイベント総数: {len(events):,} 件")

        if events:
            # Group by user
            user_stats = {}
            for event in events:
                user = event['user_login']
                user_stats[user] = user_stats.get(user, 0) + 1

            logger.info(f"\n  ユニークユーザー数: {len(user_stats)} 人")

            logger.info("\nトップ10ユーザー:")
            sorted_users = sorted(user_stats.items(), key=lambda x: x[1], reverse=True)
            for i, (user, count) in enumerate(sorted_users[:10], 1):
                logger.info(f"  {i:2d}. {user:40s} {count:5d} 回")

            # Group by date
            date_stats = {}
            for event in events:
                date_str = event['download_at_jst'][:10]  # Extract YYYY-MM-DD
                date_stats[date_str] = date_stats.get(date_str, 0) + 1

            logger.info("\n日別ダウンロード数:")
            for date_str in sorted(date_stats.keys()):
                logger.info(f"  {date_str}: {date_stats[date_str]:5d} 回")

            logger.info("\n最新5件のダウンロード:")
            for i, event in enumerate(events[:5], 1):
                logger.info(f"{i}. {event['user_name']} ({event['user_login']})")
                logger.info(f"   ファイル: {event['file_name']}")
                logger.info(f"   日時: {event['download_at_jst']}")
                logger.info(f"   IP: {event['ip_address']}")
                logger.info("")

        else:
            logger.warning("\n⚠ ダウンロードイベントが見つかりませんでした")

        logger.info("="*80)
        logger.info("テスト完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_nov9_15()
    if success:
        print("\n[OK] テスト成功")
    else:
        print("\n[FAILED] テスト失敗")
