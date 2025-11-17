"""Test JWT authentication and events fetching."""

import os
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import JWTAuth, Client
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_jwt_authentication():
    """Test JWT authentication and enterprise events access."""
    try:
        # Look for config.json in common locations
        possible_paths = [
            r"C:\box\config.json",
            r"C:\Users\1763\Documents\box\config.json",
            r"C:\dev\python\box_download_report\config.json",
            os.path.expanduser("~/.box/config.json"),
            "./config.json",
        ]

        config_path = None
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break

        if not config_path:
            logger.error("config.json not found. Please specify the path.")
            logger.info("\nPlease provide the path to your Box JWT config.json file.")
            logger.info("You can download it from Box Developer Console:")
            logger.info("1. Go to https://app.box.com/developers/console")
            logger.info("2. Select your app")
            logger.info("3. Go to Configuration tab")
            logger.info("4. Click 'Generate a Public/Private Keypair'")
            logger.info("5. Save the downloaded config.json")
            return False

        logger.info("="*80)
        logger.info("Testing JWT Authentication")
        logger.info("="*80)
        logger.info(f"Using config file: {config_path}")

        # Load config
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # Extract client ID for display
        client_id = config.get('boxAppSettings', {}).get('clientID', 'Unknown')
        enterprise_id = config.get('enterpriseID', 'Unknown')
        logger.info(f"Client ID: {client_id}")
        logger.info(f"Enterprise ID: {enterprise_id}")

        # Create JWT auth
        logger.info("\nAuthenticating with JWT...")
        auth = JWTAuth.from_settings_dictionary(config)
        client = Client(auth)

        # Test 1: Get current user (service account)
        logger.info("\n--- Test 1: Getting service account info ---")
        user = client.user().get()
        logger.info(f"Authenticated as: {user.name} ({user.login})")
        logger.info(f"User ID: {user.id}")

        # Test 2: Try to get enterprise events
        logger.info("\n--- Test 2: Fetching enterprise events (last 24 hours) ---")

        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)

        logger.info(f"Time range: {yesterday} to {now} (UTC)")

        try:
            events_response = client.events().get_events(
                stream_type='admin_logs',
                limit=10
            )

            events = list(events_response)
            logger.info(f"\nSuccessfully fetched {len(events)} events!")

            if events:
                logger.info("\nSample events:")
                for i, event in enumerate(events[:5], 1):
                    event_type = event.get('event_type', 'UNKNOWN')
                    created_at = event.get('created_at', 'UNKNOWN')
                    created_by = event.get('created_by', {})
                    user_name = created_by.get('name', 'Unknown')

                    logger.info(f"{i}. {event_type} by {user_name} at {created_at}")

                # Count download events
                download_count = sum(1 for e in events if e.get('event_type') == 'DOWNLOAD')
                logger.info(f"\nDownload events in sample: {download_count}")

        except Exception as e:
            logger.error(f"Failed to fetch enterprise events: {e}")
            logger.info("\nThis may be because:")
            logger.info("1. The app needs 'Manage enterprise properties' scope")
            logger.info("2. The app needs to be authorized by Enterprise Admin")
            logger.info("3. In Box Developer Console -> Authorization tab -> Review and Submit")
            return False

        logger.info("\n" + "="*80)
        logger.info("JWT Authentication test completed successfully!")
        logger.info("="*80)
        logger.info("\nYou can now use this config.json for the main program.")
        logger.info(f"Set environment variable: BOX_CONFIG_PATH={config_path}")

        return True

    except FileNotFoundError as e:
        logger.error(f"Config file not found: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during JWT authentication test: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_jwt_authentication()
    if success:
        print("\n[OK] JWT authentication test PASSED")
    else:
        print("\n[FAILED] JWT authentication test FAILED")
        print("\nPlease provide the path to your config.json file")
