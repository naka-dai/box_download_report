"""Data aggregation module for daily reports."""

import logging
from typing import List, Dict, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataAggregator:
    """Aggregator for download event data."""

    def __init__(self):
        """Initialize aggregator."""
        pass

    def aggregate_by_file(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Aggregate download events by file.

        Args:
            events: List of download events

        Returns:
            List of file aggregations sorted by download count
        """
        file_stats = defaultdict(lambda: {
            'file_id': '',
            'file_name': '',
            'download_count': 0
        })

        for event in events:
            file_id = event.get('file_id', '')
            file_name = event.get('file_name', '')

            if file_id:
                file_stats[file_id]['file_id'] = file_id
                file_stats[file_id]['file_name'] = file_name
                file_stats[file_id]['download_count'] += 1

        # Convert to list and sort by download count
        result = list(file_stats.values())
        result.sort(key=lambda x: x['download_count'], reverse=True)

        logger.info(f"Aggregated {len(result)} files from {len(events)} events")
        return result

    def aggregate_by_user_and_file(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Aggregate download events by user and file.

        Args:
            events: List of download events

        Returns:
            List of user-file aggregations sorted by download count
        """
        user_file_stats = defaultdict(lambda: {
            'user_login': '',
            'user_name': '',
            'file_id': '',
            'file_name': '',
            'download_count': 0,
            'last_download_at': ''
        })

        for event in events:
            user_login = event.get('user_login', '')
            user_name = event.get('user_name', '')
            file_id = event.get('file_id', '')
            file_name = event.get('file_name', '')
            download_at = event.get('download_at_jst', '')

            # Create composite key
            key = (user_login, file_id)

            if user_login and file_id:
                user_file_stats[key]['user_login'] = user_login
                user_file_stats[key]['user_name'] = user_name
                user_file_stats[key]['file_id'] = file_id
                user_file_stats[key]['file_name'] = file_name
                user_file_stats[key]['download_count'] += 1

                # Update last download time if newer
                if download_at > user_file_stats[key]['last_download_at']:
                    user_file_stats[key]['last_download_at'] = download_at

        # Convert to list and sort by download count
        result = list(user_file_stats.values())
        result.sort(key=lambda x: x['download_count'], reverse=True)

        logger.info(f"Aggregated {len(result)} user-file combinations from {len(events)} events")
        return result

    def aggregate_by_user(self, events: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Aggregate download events by user.

        Args:
            events: List of download events

        Returns:
            Dictionary of user statistics (keyed by user_login)
        """
        user_stats = defaultdict(lambda: {
            'user_login': '',
            'user_name': '',
            'download_count': 0,
            'actual_download_count': 0,  # DOWNLOAD only
            'preview_count': 0,          # PREVIEW only
            'unique_files': set(),
            'events': []
        })

        for event in events:
            user_login = event.get('user_login', '')
            user_name = event.get('user_name', '')
            file_id = event.get('file_id', '')
            event_type = event.get('event_type', 'DOWNLOAD')

            if user_login:
                user_stats[user_login]['user_login'] = user_login
                user_stats[user_login]['user_name'] = user_name
                user_stats[user_login]['download_count'] += 1  # Total (DL + PV)

                # Track DL/PV separately
                if event_type == 'PREVIEW':
                    user_stats[user_login]['preview_count'] += 1
                else:
                    user_stats[user_login]['actual_download_count'] += 1

                if file_id:
                    user_stats[user_login]['unique_files'].add(file_id)
                user_stats[user_login]['events'].append(event)

        # Convert sets to counts for easier processing
        result = {}
        for user_login, stats in user_stats.items():
            result[user_login] = {
                'user_login': stats['user_login'],
                'user_name': stats['user_name'],
                'download_count': stats['download_count'],  # Total (DL + PV)
                'actual_download_count': stats['actual_download_count'],  # DL only
                'preview_count': stats['preview_count'],  # PV only
                'unique_files_count': len(stats['unique_files']),
                'events': stats['events']
            }

        logger.info(f"Aggregated {len(result)} users from {len(events)} events")
        return result

    def get_user_events(self, events: List[Dict[str, Any]], user_login: str) -> List[Dict[str, Any]]:
        """
        Get all events for a specific user.

        Args:
            events: List of all events
            user_login: User login ID

        Returns:
            List of events for the specified user
        """
        user_events = [event for event in events if event.get('user_login') == user_login]
        return user_events

    def get_offhour_events(
        self,
        events: List[Dict[str, Any]],
        business_start_hour: int,
        business_start_minute: int,
        business_end_hour: int,
        business_end_minute: int
    ) -> List[Dict[str, Any]]:
        """
        Filter events that occurred outside business hours.

        Args:
            events: List of download events
            business_start_hour: Business hours start hour (JST)
            business_start_minute: Business hours start minute
            business_end_hour: Business hours end hour (JST)
            business_end_minute: Business hours end minute

        Returns:
            List of events outside business hours
        """
        from datetime import datetime

        offhour_events = []

        for event in events:
            download_at_jst = event.get('download_at_jst', '')
            if not download_at_jst:
                continue

            try:
                dt = datetime.fromisoformat(download_at_jst)
                hour = dt.hour
                minute = dt.minute

                # Convert time to minutes for easier comparison
                event_minutes = hour * 60 + minute
                start_minutes = business_start_hour * 60 + business_start_minute
                end_minutes = business_end_hour * 60 + business_end_minute

                # Check if outside business hours
                if event_minutes < start_minutes or event_minutes >= end_minutes:
                    offhour_events.append(event)

            except Exception as e:
                logger.warning(f"Failed to parse datetime {download_at_jst}: {e}")
                continue

        logger.info(f"Found {len(offhour_events)} off-hour events out of {len(events)} total events")
        return offhour_events

    def count_offhour_downloads_by_user(
        self,
        events: List[Dict[str, Any]],
        business_start_hour: int,
        business_start_minute: int,
        business_end_hour: int,
        business_end_minute: int
    ) -> Dict[str, int]:
        """
        Count off-hour downloads for each user.

        Args:
            events: List of download events
            business_start_hour: Business hours start hour (JST)
            business_start_minute: Business hours start minute
            business_end_hour: Business hours end hour (JST)
            business_end_minute: Business hours end minute

        Returns:
            Dictionary of user_login -> off-hour download count
        """
        offhour_events = self.get_offhour_events(
            events,
            business_start_hour,
            business_start_minute,
            business_end_hour,
            business_end_minute
        )

        offhour_counts = defaultdict(int)
        for event in offhour_events:
            user_login = event.get('user_login', '')
            if user_login:
                offhour_counts[user_login] += 1

        return dict(offhour_counts)
