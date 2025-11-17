"""Simple Box API connection test."""

import os
import logging
from boxsdk import Client, DevelopmentClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_box_connection():
    """Test Box connection."""
    try:
        # Get credentials from environment variables
        developer_token = os.getenv('BOX_DEVELOPER_TOKEN')
        folder_id = os.getenv('BOX_ROOT_FOLDER_ID', '243194687037')

        logger.info(f"Developer Token (last 4 chars): ...{developer_token[-4:] if developer_token else 'NOT FOUND'}")
        logger.info(f"Target Folder ID: {folder_id}")

        if not developer_token:
            logger.error("BOX_DEVELOPER_TOKEN not found. Please set it or provide a valid token.")
            logger.info("\nTo get a Developer Token:")
            logger.info("1. Go to Box Developer Console")
            logger.info("2. Select your app")
            logger.info("3. Go to Configuration tab")
            logger.info("4. Scroll down to 'Developer Token' section")
            logger.info("5. Generate a new token (valid for 60 minutes)")
            return False

        # Create client with developer token
        from boxsdk import OAuth2

        auth = OAuth2(
            client_id='',
            client_secret='',
            access_token=developer_token,
        )
        client = Client(auth)

        # Test 1: Get current user
        logger.info("\n=== Test 1: Getting current user ===")
        try:
            user = client.user().get()
            logger.info(f"Success! Authenticated as: {user.name} ({user.login})")
        except Exception as e:
            logger.error(f"Failed to get user info: {e}")
            logger.info("Note: Your Developer Token may have expired. Please generate a new one.")
            return False

        # Test 2: Get target folder
        logger.info(f"\n=== Test 2: Getting folder {folder_id} ===")
        try:
            folder = client.folder(folder_id).get()
            logger.info(f"Success! Folder name: {folder.name}")
            logger.info(f"Folder ID: {folder.id}")
        except Exception as e:
            logger.error(f"Failed to get folder: {e}")
            return False

        # Test 3: List items in folder (first 20)
        logger.info(f"\n=== Test 3: Listing items in folder ===")
        try:
            items = folder.get_items(limit=20)
            item_count = 0
            file_count = 0
            folder_count = 0

            for item in items:
                item_count += 1
                if item.type == 'file':
                    file_count += 1
                    logger.info(f"  [FILE] {item.name} (ID: {item.id})")
                elif item.type == 'folder':
                    folder_count += 1
                    logger.info(f"  [FOLDER] {item.name} (ID: {item.id})")

            logger.info(f"\nTotal items shown: {item_count} (Files: {file_count}, Folders: {folder_count})")
        except Exception as e:
            logger.error(f"Failed to list items: {e}")
            return False

        logger.info("\n" + "="*60)
        logger.info("Box connection test completed successfully!")
        logger.info("="*60)
        return True

    except Exception as e:
        logger.error(f"Error during Box connection test: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_box_connection()
    if success:
        print("\n[OK] Box connection test PASSED")
    else:
        print("\n[FAILED] Box connection test FAILED")
