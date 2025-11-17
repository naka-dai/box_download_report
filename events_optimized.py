"""Optimized Box Events API module - Event-driven approach."""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from boxsdk import Client

logger = logging.getLogger(__name__)


class OptimizedEventsFetcher:
    """Optimized fetcher for Box download events using event-driven approach."""

    def __init__(self, client: Client, root_folder_id: str):
        """
        Initialize events fetcher.

        Args:
            client: Authenticated Box client
            root_folder_id: Root folder ID to monitor
        """
        self.client = client
        self.root_folder_id = root_folder_id
        self._folder_cache = {}  # Cache for folder ancestry checks

    def fetch_download_events(
        self,
        start_time: datetime,
        end_time: datetime,
        use_folder_filter: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Fetch download events for a specific time period.

        This is the optimized approach:
        1. Get ALL download events first (no file scanning needed)
        2. Check if each file belongs to target folder using parent chain

        Args:
            start_time: Start time (UTC)
            end_time: End time (UTC)
            use_folder_filter: If True, filter by root folder. If False, get all downloads.

        Returns:
            List of download event dictionaries
        """
        logger.info(f"Fetching download events from {start_time} to {end_time}")
        logger.info(f"Folder filtering: {'Enabled' if use_folder_filter else 'Disabled'}")

        events = []

        # Format times for Box API
        created_after = start_time.strftime('%Y-%m-%dT%H:%M:%S-00:00')
        created_before = end_time.strftime('%Y-%m-%dT%H:%M:%S-00:00')

        logger.info(f"API query: created_after={created_after}, created_before={created_before}")

        event_count = 0
        download_count = 0
        filtered_count = 0
        out_of_range_count = 0  # Track events outside time range
        max_out_of_range = 5000  # Stop if we see too many out-of-range events in a row
        max_batches = 200  # Maximum number of batches to process

        try:
            # Fetch events in batches
            stream_position = None
            batch_number = 0

            while batch_number < max_batches:
                try:
                    # Get events from Box (admin_logs stream)
                    logger.debug(f"Fetching events batch (position: {stream_position})...")

                    # Box SDK parameters for enterprise events
                    # Note: Time filtering is done via stream_type='admin_logs' and manual filtering
                    events_response = self.client.events().get_events(
                        stream_type='admin_logs',
                        limit=500,
                        stream_position=stream_position
                    )

                    # Extract events from response (events_response is a dict with 'entries' key)
                    if isinstance(events_response, dict):
                        batch_events = events_response.get('entries', [])
                        next_position = events_response.get('next_stream_position')
                    else:
                        batch_events = list(events_response)
                        next_position = None

                    if not batch_events:
                        logger.debug("No more events to fetch")
                        break

                    batch_number += 1
                    logger.info(f"Processing batch {batch_number}/{max_batches}: {len(batch_events)} events...")

                    for event in batch_events:
                        event_count += 1

                        # Convert Event object to dictionary if needed
                        if not isinstance(event, dict):
                            if hasattr(event, 'response_object'):
                                event = event.response_object
                            elif hasattr(event, '__dict__'):
                                event = event.__dict__
                            else:
                                logger.warning(f"Skipping unsupported event type: {type(event)}")
                                continue

                        # Filter for DOWNLOAD events only
                        event_type = event.get('event_type')
                        if event_type != 'DOWNLOAD':
                            continue

                        # Filter by time range
                        created_at_str = event.get('created_at', '')
                        if created_at_str:
                            try:
                                event_time = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                                if event_time < start_time or event_time > end_time:
                                    out_of_range_count += 1
                                    # If we've seen many events outside range, stop fetching
                                    if out_of_range_count >= max_out_of_range:
                                        logger.info(f"Stopping: {out_of_range_count} consecutive events outside time range")
                                        break
                                    continue
                                else:
                                    out_of_range_count = 0  # Reset counter when we find an in-range event
                            except:
                                pass

                        download_count += 1

                        # Extract file info from event
                        source = event.get('source', {})
                        if source.get('type') != 'file':
                            continue

                        file_id = source.get('id')
                        if not file_id:
                            continue

                        # If folder filtering is enabled, check if file is in target folder
                        if use_folder_filter:
                            if not self._is_file_in_folder(file_id, self.root_folder_id):
                                continue

                        filtered_count += 1

                        # Parse and add the event
                        parsed_event = self._parse_event(event)
                        if parsed_event:
                            events.append(parsed_event)

                    # Check if we should continue
                    if len(batch_events) < 500:
                        break

                    # Update position for next batch using next_stream_position
                    stream_position = next_position

                    # If no next position, we've reached the end
                    if stream_position is None:
                        logger.debug("No next_stream_position, reached end of events")
                        break

                except StopIteration:
                    logger.debug("Event stream exhausted")
                    break
                except Exception as e:
                    logger.error(f"Error fetching events batch: {e}")
                    break

            if batch_number >= max_batches:
                logger.info(f"Reached maximum batch limit ({max_batches}). Stopping event fetch.")

            logger.info(f"Event processing summary:")
            logger.info(f"  Total events processed: {event_count:,}")
            logger.info(f"  Download events found: {download_count:,}")
            logger.info(f"  Events in target folder: {filtered_count:,}")

        except Exception as e:
            logger.error(f"Error fetching events: {e}", exc_info=True)
            raise

        return events

    def _is_file_in_folder(self, file_id: str, target_folder_id: str) -> bool:
        """
        Check if a file is in the target folder (or any subfolder).
        Uses parent chain traversal - much faster than scanning all files.

        Args:
            file_id: File ID to check
            target_folder_id: Target folder ID

        Returns:
            True if file is in target folder tree
        """
        # Check cache first
        cache_key = f"{file_id}:{target_folder_id}"
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        try:
            # Get file info (includes parent folder)
            file_obj = self.client.file(file_id).get(fields=['id', 'name', 'path_collection'])

            # Get path collection (ancestor folders)
            path_collection = file_obj.get('path_collection', {})
            entries = path_collection.get('entries', [])

            # Check if target folder is in the ancestor chain
            for entry in entries:
                if entry.get('id') == target_folder_id:
                    self._folder_cache[cache_key] = True
                    return True

            # Also check immediate parent
            parent = file_obj.get('parent', {})
            parent_id = parent.get('id')

            # Traverse up the parent chain
            current_folder_id = parent_id
            max_depth = 20  # Safety limit
            depth = 0

            while current_folder_id and depth < max_depth:
                if current_folder_id == target_folder_id:
                    self._folder_cache[cache_key] = True
                    return True

                # Get parent folder
                try:
                    folder = self.client.folder(current_folder_id).get(fields=['id', 'parent'])
                    parent = folder.get('parent')
                    current_folder_id = parent.get('id') if parent else None
                except:
                    break

                depth += 1

            # Not in target folder
            self._folder_cache[cache_key] = False
            return False

        except Exception as e:
            logger.warning(f"Could not check folder ancestry for file {file_id}: {e}")
            # On error, assume not in folder
            return False

    def _parse_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a Box event into standardized format.

        Args:
            event: Raw Box event

        Returns:
            Parsed event dictionary or None on error
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
