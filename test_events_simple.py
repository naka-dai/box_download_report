"""Simple test to verify events are fetched correctly."""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import Client, JWTAuth
from events_optimized import OptimizedEventsFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_simple():
    """Simple test without folder filtering."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        folder_id = "243194687037"
        admin_user_id = "16623033409"

        logger.info("Loading config...")
        with open(config_path, 'r') as f:
            config = json.load(f)

        logger.info("Creating JWT client...")
        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        logger.info("Creating events fetcher...")
        fetcher = OptimizedEventsFetcher(client, folder_id)

        # Test with last 7 days, NO folder filtering
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)

        logger.info(f"Fetching events from {week_ago} to {now}")
        logger.info("Folder filtering: DISABLED")

        events = fetcher.fetch_download_events(
            start_time=week_ago,
            end_time=now,
            use_folder_filter=False  # Don't filter by folder
        )

        logger.info(f"\\n✓ SUCCESS! Fetched {len(events)} download events!")

        if events:
            logger.info("\\nFirst 5 events:")
            for i, event in enumerate(events[:5], 1):
                logger.info(f"{i}. User: {event['user_login']}")
                logger.info(f"   File: {event['file_name']}")
                logger.info(f"   Time: {event['download_at_jst']}")
                logger.info("")

        return True

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_simple()
    if success:
        print("\\n[OK] Test PASSED")
    else:
        print("\\n[FAILED] Test FAILED")
