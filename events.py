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
        stream_position = None  # Start from beginning (or use 'now' for most recent)

        # Box Events API uses admin_logs stream for enterprise events
        try:
            # Get events using enterprise events API
            logger.info(f"Querying Box events API for period: {start_time} to {end_time}")

            event_count = 0
            filtered_count = 0
            date_filtered_count = 0

            # Fetch events in batches
            # Note: Box Events API does not support created_after/created_before filters
            # We need to fetch events and filter by date on the client side
            while True:
                try:
                    # Get events from Box
                    # Box SDK's get_events() only supports: limit, stream_position, stream_type
                    # stream_position can be: None (start), 0 (start), 'now' (current), or a position string
                    events_response = self.client.events().get_events(
                        stream_type='admin_logs',
                        limit=500,
                        stream_position=stream_position if stream_position is not None else 0
                    )

                    batch_events = list(events_response.get('entries', []))

                    # Update stream position for next batch
                    next_stream_position = events_response.get('next_stream_position')
                    if next_stream_position:
                        stream_position = next_stream_position

                    if not batch_events:
                        break

                    for event in batch_events:
                        event_count += 1

                        # Filter by date range
                        # Event created_at is in ISO 8601 format: "2025-11-18T12:34:56-08:00"
                        created_at_str = event.get('created_at')
                        if created_at_str:
                            try:
                                from dateutil import parser
                                event_time = parser.parse(created_at_str)

                                # Check if event is within our date range
                                if event_time < start_time or event_time >= end_time:
                                    # Event is outside our date range
                                    # If event is before our range, we can stop fetching
                                    if event_time < start_time:
                                        logger.info(f"Reached events before target period, stopping fetch")
                                        break
                                    continue

                                date_filtered_count += 1
                            except Exception as e:
                                logger.warning(f"Failed to parse event timestamp {created_at_str}: {e}")
                                continue

                        # Filter for DOWNLOAD events only
                        if event.get('event_type') != 'DOWNLOAD':
                            continue

                        # Check if the source is a file
                        source = event.get('source')
                        if not source or source.get('type') != 'file':
                            continue

                        # Note: Folder filtering disabled - collecting ALL download events
                        # file_id = source.get('id')
                        # if file_id not in target_file_ids:
                        #     continue

                        filtered_count += 1

                        # Parse and structure the event
                        parsed_event = self._parse_event(event)
                        if parsed_event:
                            events.append(parsed_event)

                    # Check if we should continue fetching
                    # stream_position is already updated from next_stream_position
                    if len(batch_events) < 500:
                        break

                except Exception as e:
                    logger.error(f"Error fetching events batch: {e}")
                    break

            logger.info(f"Processed {event_count} total events")
            logger.info(f"Date filtered: {date_filtered_count} events in range")
            logger.info(f"Download events in target folder: {filtered_count}")

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
