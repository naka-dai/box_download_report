"""Test fetching download events from last week only."""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import Client, JWTAuth
from events_optimized import OptimizedEventsFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_last_week():
    """Fetch download events from last week (7 days)."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        folder_id = "243194687037"
        admin_user_id = "16623033409"

        logger.info("="*80)
        logger.info("先週のダウンロードイベント取得テスト")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Create events fetcher
        fetcher = OptimizedEventsFetcher(client, folder_id)

        # Get last week's date range
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)

        logger.info(f"期間: {week_ago.strftime('%Y-%m-%d %H:%M')} ~ {now.strftime('%Y-%m-%d %H:%M')} (UTC)")
        logger.info(f"      {(week_ago + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')} ~ {(now + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M')} (JST)")

        # Fetch download events (without folder filter for faster testing)
        logger.info("\nダウンロードイベントを取得中...")
        events = fetcher.fetch_download_events(
            start_time=week_ago,
            end_time=now,
            use_folder_filter=False  # フォルダフィルタなし（全体）
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

            logger.info("\n最新5件のダウンロード:")
            for i, event in enumerate(events[:5], 1):
                logger.info(f"{i}. {event['user_name']} ({event['user_login']})")
                logger.info(f"   ファイル: {event['file_name']}")
                logger.info(f"   日時: {event['download_at_jst']}")
                logger.info(f"   IP: {event['ip_address']}")
                logger.info("")

        else:
            logger.warning("\n⚠ ダウンロードイベントが見つかりませんでした")
            logger.info("  過去7日間にダウンロードがなかった可能性があります")

        logger.info("="*80)
        logger.info("テスト完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_last_week()
    if success:
        print("\n[OK] テスト成功")
    else:
        print("\n[FAILED] テスト失敗")
