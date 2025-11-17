"""Test JWT authentication with As-User header (impersonate admin)."""

import os
import logging
from datetime import datetime, timedelta, timezone
from boxsdk import JWTAuth, Client
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_jwt_as_admin_user():
    """Test JWT authentication accessing events as admin user."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")

        if not os.path.exists(config_path):
            logger.error(f"Config file not found: {config_path}")
            return False

        logger.info("="*80)
        logger.info("Testing JWT Authentication with As-User (Admin)")
        logger.info("="*80)
        logger.info(f"Config file: {config_path}")

        # Load config
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        enterprise_id = config.get('enterpriseID', 'Unknown')
        logger.info(f"Enterprise ID: {enterprise_id}")

        # Create JWT auth
        logger.info("\nAuthenticating with JWT...")
        auth = JWTAuth.from_settings_dictionary(config)

        # Get service account client first
        service_client = Client(auth)

        # Test 1: Get service account info
        logger.info("\n--- Test 1: Service Account Info ---")
        service_user = service_client.user().get()
        logger.info(f"Service Account: {service_user.name} ({service_user.login})")

        # Test 2: Get admin users in the enterprise
        logger.info("\n--- Test 2: Finding Admin Users ---")

        try:
            # Try to get users (requires admin privileges)
            users = service_client.users(limit=100)

            admin_users = []
            for user in users:
                # Check if user has admin role
                if user.role and 'admin' in user.role.lower():
                    admin_users.append({
                        'id': user.id,
                        'login': user.login,
                        'name': user.name,
                        'role': user.role
                    })
                    logger.info(f"  Admin found: {user.name} ({user.login}) - Role: {user.role}")

            if not admin_users:
                logger.warning("No admin users found. Trying with current user...")
                # Use a specific admin user ID if known
                logger.info("\nPlease specify an admin user ID to use.")
                logger.info("You can find this in Box Admin Console -> Users")
                return False

            # Use the first admin user
            admin_user_id = admin_users[0]['id']
            logger.info(f"\nUsing admin user: {admin_users[0]['name']} (ID: {admin_user_id})")

        except Exception as e:
            logger.warning(f"Could not list users: {e}")
            logger.info("\nTrying with known admin user ID...")
            # If you know the admin user ID, you can hardcode it here
            # For example: admin_user_id = "16623033409"
            return False

        # Test 3: Create client as admin user
        logger.info("\n--- Test 3: Creating Client as Admin User ---")

        admin_client = service_client.as_user(service_client.user(admin_user_id))

        admin_as_user = admin_client.user().get()
        logger.info(f"Now acting as: {admin_as_user.name} ({admin_as_user.login})")

        # Test 4: Try to get enterprise events as admin
        logger.info("\n--- Test 4: Fetching Enterprise Events as Admin ---")

        try:
            events_response = admin_client.events().get_events(
                stream_type='admin_logs',
                limit=10
            )

            events = list(events_response)
            logger.info(f"\n✓ Success! Fetched {len(events)} events as admin user!")

            if events:
                logger.info("\nSample events:")
                for i, event in enumerate(events[:5], 1):
                    event_type = event.get('event_type', 'UNKNOWN')
                    created_at = event.get('created_at', 'UNKNOWN')
                    created_by = event.get('created_by', {})
                    user_name = created_by.get('name', 'Unknown')
                    logger.info(f"{i}. {event_type} by {user_name} at {created_at}")

                download_count = sum(1 for e in events if e.get('event_type') == 'DOWNLOAD')
                logger.info(f"\nDownload events: {download_count}")

            logger.info("\n" + "="*80)
            logger.info("SUCCESS! JWT + As-User authentication works!")
            logger.info("="*80)
            return True

        except Exception as e:
            logger.error(f"Failed to fetch events as admin: {e}")
            return False

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_jwt_as_admin_user()
    if success:
        print("\n[OK] JWT As-User test PASSED")
    else:
        print("\n[FAILED] JWT As-User test FAILED")
