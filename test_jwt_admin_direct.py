"""Test JWT with known admin user ID."""

import os
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import JWTAuth, Client
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_with_admin_user():
    """Test using known admin user ID."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")

        logger.info("="*80)
        logger.info("Testing JWT + As-User with Known Admin")
        logger.info("="*80)

        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)

        # Known admin user ID from earlier test
        admin_user_id = "16623033409"  # daisuke.nakahara@nikko-net.co.jp

        logger.info(f"Using admin user ID: {admin_user_id}")

        # Create client as admin user
        logger.info("\n--- Creating Client as Admin User ---")
        admin_client = service_client.as_user(service_client.user(admin_user_id))

        # Verify we're acting as the admin
        current_user = admin_client.user().get()
        logger.info(f"Acting as: {current_user.name} ({current_user.login})")

        # Try to fetch enterprise events
        logger.info("\n--- Fetching Enterprise Events ---")

        events_response = admin_client.events().get_events(
            stream_type='admin_logs',
            limit=20
        )

        events = list(events_response)
        logger.info(f"\n✓ SUCCESS! Fetched {len(events)} enterprise events!")

        if events:
            logger.info("\nEvent types found:")
            event_types = {}
            for event in events:
                etype = event.get('event_type', 'UNKNOWN')
                event_types[etype] = event_types.get(etype, 0) + 1

            for etype, count in sorted(event_types.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {etype}: {count}")

            # Show sample download events
            download_events = [e for e in events if e.get('event_type') == 'DOWNLOAD']
            if download_events:
                logger.info(f"\nSample DOWNLOAD events ({len(download_events)} found):")
                for i, event in enumerate(download_events[:5], 1):
                    created_by = event.get('created_by', {})
                    source = event.get('source', {})
                    logger.info(f"{i}. User: {created_by.get('name', 'Unknown')}")
                    logger.info(f"   File: {source.get('name', 'Unknown')}")
                    logger.info(f"   Time: {event.get('created_at', 'Unknown')}")

        logger.info("\n" + "="*80)
        logger.info("✓ JWT + As-User authentication WORKS!")
        logger.info("="*80)
        logger.info("\nThis approach can be used for the main program.")
        logger.info(f"Admin User ID to use: {admin_user_id}")

        return True

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_with_admin_user()
    if success:
        print("\n[OK] Test PASSED")
    else:
        print("\n[FAILED] Test FAILED")
