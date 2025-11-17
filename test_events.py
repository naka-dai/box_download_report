"""Test optimized events fetching."""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import Client, JWTAuth
from events_optimized import OptimizedEventsFetcher

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_events_fetching():
    """Test event fetching with optimized approach using JWT authentication."""
    try:
        # Get credentials
        config_path = os.path.expanduser("~/.box/config.json")
        folder_id = os.getenv('BOX_ROOT_FOLDER_ID', '243194687037')
        admin_user_id = "16623033409"  # Known admin user ID

        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            return False

        logger.info("="*80)
        logger.info("Testing Optimized Events Fetching with JWT")
        logger.info("="*80)
        logger.info(f"Config file: {config_path}")
        logger.info(f"Target Folder ID: {folder_id}")
        logger.info(f"Admin User ID: {admin_user_id}")

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)

        # Create client as admin user (to access admin_logs)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Create optimized events fetcher
        fetcher = OptimizedEventsFetcher(client, folder_id)

        # Test 1: Get events from last 7 days
        logger.info("\n" + "="*80)
        logger.info("Test 1: Fetching download events from last 7 days")
        logger.info("="*80)

        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)

        logger.info(f"Time range: {week_ago} to {now} (UTC)")

        # Fetch events WITHOUT folder filtering first (to see all downloads)
        logger.info("\n--- Fetching ALL download events (no folder filter) ---")
        all_events = fetcher.fetch_download_events(
            start_time=week_ago,
            end_time=now,
            use_folder_filter=False
        )

        logger.info(f"\nTotal download events found: {len(all_events)}")

        if all_events:
            logger.info("\nSample events (first 5):")
            for i, event in enumerate(all_events[:5], 1):
                logger.info(f"{i}. User: {event['user_login']}")
                logger.info(f"   File: {event['file_name']} (ID: {event['file_id']})")
                logger.info(f"   Time: {event['download_at_jst']}")
                logger.info(f"   IP: {event['ip_address']}")
                logger.info("")

        # Test 2: Get events WITH folder filtering
        logger.info("\n" + "="*80)
        logger.info("Test 2: Fetching download events from target folder only")
        logger.info("="*80)

        filtered_events = fetcher.fetch_download_events(
            start_time=week_ago,
            end_time=now,
            use_folder_filter=True
        )

        logger.info(f"\nDownload events in target folder: {len(filtered_events)}")

        if filtered_events:
            logger.info("\nFiltered events (first 5):")
            for i, event in enumerate(filtered_events[:5], 1):
                logger.info(f"{i}. User: {event['user_login']}")
                logger.info(f"   File: {event['file_name']} (ID: {event['file_id']})")
                logger.info(f"   Time: {event['download_at_jst']}")
                logger.info("")

        # Summary
        logger.info("\n" + "="*80)
        logger.info("SUMMARY")
        logger.info("="*80)
        logger.info(f"Total download events (all folders): {len(all_events)}")
        logger.info(f"Download events in target folder: {len(filtered_events)}")

        if all_events:
            filter_ratio = (len(filtered_events) / len(all_events)) * 100
            logger.info(f"Filter ratio: {filter_ratio:.1f}%")

        # User statistics
        if filtered_events:
            users = {}
            for event in filtered_events:
                user = event['user_login']
                users[user] = users.get(user, 0) + 1

            logger.info(f"\nUnique users with downloads: {len(users)}")
            logger.info("\nTop 10 users by download count:")
            sorted_users = sorted(users.items(), key=lambda x: x[1], reverse=True)
            for i, (user, count) in enumerate(sorted_users[:10], 1):
                logger.info(f"  {i:2d}. {user:40s} {count:5d} downloads")

        logger.info("\n" + "="*80)
        logger.info("Events fetching test completed successfully!")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"Error during events test: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_events_fetching()
    if success:
        print("\n[OK] Events fetching test PASSED")
    else:
        print("\n[FAILED] Events fetching test FAILED")
