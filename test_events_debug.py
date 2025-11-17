"""Debug Box Events API response structure."""

import os
import json
import logging
from boxsdk import Client, JWTAuth

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_events_response():
    """Debug the structure of events API response."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"

        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        logger.info("Fetching events...")

        # Get raw events response
        events_response = client.events().get_events(
            stream_type='admin_logs',
            limit=5
        )

        logger.info(f"Events response type: {type(events_response)}")
        logger.info(f"Events response dir: {dir(events_response)}")

        # Try to inspect the response
        events_list = list(events_response)
        logger.info(f"Events list length: {len(events_list)}")

        if events_list:
            first_event = events_list[0]
            logger.info(f"First event type: {type(first_event)}")
            logger.info(f"First event content: {first_event}")

            if isinstance(first_event, str):
                logger.info("Event is a string - attempting to parse as JSON")
                try:
                    event_dict = json.loads(first_event)
                    logger.info(f"Parsed event keys: {event_dict.keys()}")
                except Exception as e:
                    logger.error(f"Failed to parse event as JSON: {e}")
            elif hasattr(first_event, '__dict__'):
                logger.info(f"Event __dict__: {first_event.__dict__}")
            elif hasattr(first_event, 'response_object'):
                logger.info(f"Event has response_object: {first_event.response_object}")

        return True

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = debug_events_response()
    if success:
        print("\n[OK] Debug PASSED")
    else:
        print("\n[FAILED] Debug FAILED")
