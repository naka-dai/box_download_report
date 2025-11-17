"""Test Box API connection and data retrieval."""

import os
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import Client, OAuth2

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_with_developer_token():
    """Test Box connection using developer token."""
    try:
        # Get credentials from environment variables
        developer_token = os.getenv('BOX_DEVELOPER_TOKEN')
        folder_id = os.getenv('BOX_ROOT_FOLDER_ID')

        if not developer_token:
            logger.error("BOX_DEVELOPER_TOKEN not found in environment variables")
            return False

        if not folder_id:
            logger.error("BOX_ROOT_FOLDER_ID not found in environment variables")
            return False

        logger.info(f"Using Developer Token (last 4 chars): ...{developer_token[-4:]}")
        logger.info(f"Target Folder ID: {folder_id}")

        # Create OAuth2 object with developer token
        oauth = OAuth2(
            client_id='',
            client_secret='',
            access_token=developer_token,
        )

        # Create Box client
        client = Client(oauth)

        # Test 1: Get current user
        logger.info("\n=== Test 1: Getting current user ===")
        user = client.user().get()
        logger.info(f"Authenticated as: {user.name} ({user.login})")

        # Test 2: Get target folder
        logger.info(f"\n=== Test 2: Getting folder {folder_id} ===")
        folder = client.folder(folder_id).get()
        logger.info(f"Folder name: {folder.name}")
        logger.info(f"Folder ID: {folder.id}")

        # Test 3: List items in folder
        logger.info(f"\n=== Test 3: Listing items in folder ===")
        items = folder.get_items(limit=10)
        item_count = 0
        file_count = 0
        folder_count = 0

        for item in items:
            item_count += 1
            if item.type == 'file':
                file_count += 1
                logger.info(f"  File: {item.name} (ID: {item.id})")
            elif item.type == 'folder':
                folder_count += 1
                logger.info(f"  Folder: {item.name} (ID: {item.id})")

        logger.info(f"\nTotal items shown: {item_count} (Files: {file_count}, Folders: {folder_count})")

        # Test 4: Get events (limited test)
        logger.info(f"\n=== Test 4: Getting recent events ===")
        try:
            # Get events from the last 24 hours
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)

            created_after = yesterday.strftime('%Y-%m-%dT%H:%M:%S-00:00')
            created_before = now.strftime('%Y-%m-%dT%H:%M:%S-00:00')

            logger.info(f"Fetching events from {created_after} to {created_before}")

            # Note: Enterprise events require admin privileges
            events = client.events().get_events(
                stream_type='admin_logs',
                limit=10,
                created_after=created_after,
                created_before=created_before
            )

            event_list = list(events)
            logger.info(f"Retrieved {len(event_list)} events")

            for i, event in enumerate(event_list[:5], 1):
                event_type = event.get('event_type', 'UNKNOWN')
                created_at = event.get('created_at', 'UNKNOWN')
                logger.info(f"  Event {i}: {event_type} at {created_at}")

        except Exception as e:
            logger.warning(f"Could not fetch events (may require admin privileges): {e}")

        logger.info("\n=== Box connection test completed successfully ===")
        return True

    except Exception as e:
        logger.error(f"Error during Box connection test: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_with_developer_token()
    if success:
        print("\n✓ Box connection test PASSED")
    else:
        print("\n✗ Box connection test FAILED")
