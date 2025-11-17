"""Box Events API module for fetching download events."""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Set
from boxsdk import Client

logger = logging.getLogger(__name__)


class EventsFetcher:
    """Fetcher for Box download events."""

    def __init__(self, client: Client):
        """
        Initialize events fetcher.

        Args:
            client: Authenticated Box client
        """
        self.client = client

    def fetch_download_events(
        self,
        start_time: datetime,
        end_time: datetime,
        target_file_ids: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch download events for a specific time period.

        Args:
            start_time: Start time (UTC)
            end_time: End time (UTC)
            target_file_ids: Set of target file IDs to filter

        Returns:
            List of download event dictionaries
        """
        logger.info(f"Fetching events from {start_time} to {end_time}")

        events = []
        stream_position = 0

        # Box Events API uses admin_logs stream for enterprise events
        try:
            # Get events using enterprise events API
            # Note: created_after and created_before are in RFC 3339 format
            created_after = start_time.strftime('%Y-%m-%dT%H:%M:%S-00:00')
            created_before = end_time.strftime('%Y-%m-%dT%H:%M:%S-00:00')

            logger.info(f"Querying Box events API with created_after={created_after}, created_before={created_before}")

            # Use admin_logs_streaming for enterprise events
            # The Box SDK provides get_events() method
            options = {
                'stream_type': 'admin_logs',
                'created_after': created_after,
                'created_before': created_before,
            }

            event_count = 0
            filtered_count = 0

            # Fetch events in batches
            while True:
                try:
                    # Get events from Box
                    # Note: Box SDK's get_events() returns an iterator
                    events_response = self.client.events().get_events(
                        stream_type='admin_logs',
                        limit=500,
                        stream_position=stream_position if stream_position > 0 else None,
                        created_after=created_after,
                        created_before=created_before
                    )

                    batch_events = list(events_response)

                    if not batch_events:
                        break

                    for event in batch_events:
                        event_count += 1

                        # Filter for DOWNLOAD events only
                        if event.get('event_type') != 'DOWNLOAD':
                            continue

                        # Check if the file is in our target folder
                        source = event.get('source')
                        if not source or source.get('type') != 'file':
                            continue

                        file_id = source.get('id')
                        if file_id not in target_file_ids:
                            continue

                        filtered_count += 1

                        # Parse and structure the event
                        parsed_event = self._parse_event(event)
                        if parsed_event:
                            events.append(parsed_event)

                    # Update stream position for next batch
                    if len(batch_events) < 500:
                        break

                    stream_position += len(batch_events)

                except Exception as e:
                    logger.error(f"Error fetching events batch: {e}")
                    break

            logger.info(f"Processed {event_count} total events, filtered to {filtered_count} download events")

        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            raise

        logger.info(f"Fetched {len(events)} download events")
        return events

    def _parse_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse a Box event into our standardized format.

        Args:
            event: Raw Box event

        Returns:
            Parsed event dictionary
        """
        try:
            # Extract basic event info
            event_id = event.get('event_id', '')
            event_type = event.get('event_type', '')
            created_at_str = event.get('created_at', '')

            # Parse created_at (UTC)
            created_at_utc = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))

            # Convert to JST (UTC+9)
            jst = timezone(timedelta(hours=9))
            created_at_jst = created_at_utc.astimezone(jst)

            # Extract user info
            created_by = event.get('created_by', {})
            user_login = created_by.get('login', '')
            user_name = created_by.get('name', '')

            # Extract file info
            source = event.get('source', {})
            file_id = source.get('id', '')
            file_name = source.get('name', '')

            # Extract additional info
            ip_address = event.get('ip_address', '')

            # Extract client type and user agent from additional_details
            additional_details = event.get('additional_details', {})
            client_type = additional_details.get('client_type', '')
            user_agent = additional_details.get('user_agent', '')

            # Serialize raw event as JSON
            raw_json = json.dumps(event, ensure_ascii=False)

            return {
                'event_id': event_id,
                'stream_type': 'admin_logs',
                'event_type': event_type,
                'user_login': user_login,
                'user_name': user_name,
                'file_id': file_id,
                'file_name': file_name,
                'download_at_utc': created_at_utc.isoformat(),
                'download_at_jst': created_at_jst.isoformat(),
                'ip_address': ip_address,
                'client_type': client_type,
                'user_agent': user_agent,
                'raw_json': raw_json,
            }

        except Exception as e:
            logger.warning(f"Failed to parse event {event.get('event_id')}: {e}")
            return None

    def get_events_for_period(
        self,
        target_date: datetime,
        period_type: str,
        target_file_ids: Set[str]
    ) -> List[Dict[str, Any]]:
        """
        Get events for a specific period (confirmed or tentative).

        Args:
            target_date: Target date (in JST)
            period_type: 'confirmed' (full day) or 'tentative' (until now)
            target_file_ids: Set of target file IDs

        Returns:
            List of download events
        """
        # Convert JST to UTC for API query
        jst = timezone(timedelta(hours=9))

        if period_type == 'confirmed':
            # Full day: 00:00 to 24:00 JST
            start_jst = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_jst = start_jst + timedelta(days=1)
        else:  # tentative
            # From 00:00 JST to now
            start_jst = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_jst = datetime.now(jst)

        # Convert to UTC
        start_utc = start_jst.astimezone(timezone.utc)
        end_utc = end_jst.astimezone(timezone.utc)

        logger.info(f"Fetching {period_type} events: {start_jst} to {end_jst} (JST)")

        return self.fetch_download_events(start_utc, end_utc, target_file_ids)
