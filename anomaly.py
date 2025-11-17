"""Anomaly detection module for unusual download patterns."""

import logging
from typing import List, Dict, Any, Set
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Detector for anomalous download behavior."""

    def __init__(
        self,
        download_count_threshold: int,
        unique_files_threshold: int,
        offhour_threshold: int,
        spike_window_minutes: int,
        spike_threshold: int
    ):
        """
        Initialize anomaly detector.

        Args:
            download_count_threshold: Threshold for total downloads per user
            unique_files_threshold: Threshold for unique files per user
            offhour_threshold: Threshold for off-hour downloads
            spike_window_minutes: Time window for spike detection (minutes)
            spike_threshold: Threshold for downloads within spike window
        """
        self.download_count_threshold = download_count_threshold
        self.unique_files_threshold = unique_files_threshold
        self.offhour_threshold = offhour_threshold
        self.spike_window_minutes = spike_window_minutes
        self.spike_threshold = spike_threshold

    def detect_basic_anomalies(
        self,
        user_stats: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Detect basic anomalies based on download count and unique files.

        Args:
            user_stats: Dictionary of user statistics from aggregator

        Returns:
            Dictionary of anomalous users with their stats and anomaly reasons
        """
        anomalous_users = {}

        for user_login, stats in user_stats.items():
            download_count = stats.get('download_count', 0)
            unique_files_count = stats.get('unique_files_count', 0)

            anomaly_types = []

            # Check download count threshold
            if download_count >= self.download_count_threshold:
                anomaly_types.append({
                    'type': 'download_count',
                    'value': download_count,
                    'threshold': self.download_count_threshold
                })

            # Check unique files threshold
            if unique_files_count >= self.unique_files_threshold:
                anomaly_types.append({
                    'type': 'unique_files',
                    'value': unique_files_count,
                    'threshold': self.unique_files_threshold
                })

            # If any anomaly detected, add to results
            if anomaly_types:
                anomalous_users[user_login] = {
                    **stats,
                    'anomaly_types': anomaly_types
                }

        logger.info(f"Detected {len(anomalous_users)} users with basic anomalies")
        return anomalous_users

    def detect_offhour_anomalies(
        self,
        offhour_counts: Dict[str, int]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Detect anomalies based on off-hour downloads.

        Args:
            offhour_counts: Dictionary of user_login -> off-hour download count

        Returns:
            Dictionary of users with off-hour anomalies
        """
        anomalous_users = {}

        for user_login, count in offhour_counts.items():
            if count >= self.offhour_threshold:
                anomalous_users[user_login] = {
                    'user_login': user_login,
                    'anomaly_type': 'offhour',
                    'offhour_download_count': count,
                    'threshold': self.offhour_threshold
                }

        logger.info(f"Detected {len(anomalous_users)} users with off-hour anomalies")
        return anomalous_users

    def detect_spike_anomalies(
        self,
        user_stats: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Detect spike anomalies (many downloads in short time window).

        Args:
            user_stats: Dictionary of user statistics with events

        Returns:
            Dictionary of users with spike anomalies
        """
        anomalous_users = {}

        for user_login, stats in user_stats.items():
            events = stats.get('events', [])

            if len(events) < self.spike_threshold:
                continue

            # Sort events by time
            sorted_events = sorted(
                events,
                key=lambda e: e.get('download_at_jst', '')
            )

            # Convert to datetime objects
            event_times = []
            for event in sorted_events:
                download_at = event.get('download_at_jst', '')
                if download_at:
                    try:
                        dt = datetime.fromisoformat(download_at)
                        event_times.append(dt)
                    except Exception as e:
                        logger.warning(f"Failed to parse datetime {download_at}: {e}")
                        continue

            if len(event_times) < self.spike_threshold:
                continue

            # Check for spikes using sliding window
            window = timedelta(minutes=self.spike_window_minutes)
            max_count_in_window = 0
            spike_start_time = None

            for i, start_time in enumerate(event_times):
                end_time = start_time + window
                count = 0

                for event_time in event_times[i:]:
                    if event_time <= end_time:
                        count += 1
                    else:
                        break

                if count > max_count_in_window:
                    max_count_in_window = count
                    spike_start_time = start_time

            # Check if spike threshold exceeded
            if max_count_in_window >= self.spike_threshold:
                anomalous_users[user_login] = {
                    'user_login': user_login,
                    'user_name': stats.get('user_name', ''),
                    'anomaly_type': 'spike',
                    'max_downloads_in_window': max_count_in_window,
                    'window_minutes': self.spike_window_minutes,
                    'threshold': self.spike_threshold,
                    'spike_start_time': spike_start_time.isoformat() if spike_start_time else None
                }

        logger.info(f"Detected {len(anomalous_users)} users with spike anomalies")
        return anomalous_users

    def detect_all_anomalies(
        self,
        user_stats: Dict[str, Dict[str, Any]],
        offhour_counts: Dict[str, int]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Detect all types of anomalies.

        Args:
            user_stats: Dictionary of user statistics
            offhour_counts: Dictionary of off-hour download counts

        Returns:
            Dictionary of all anomalous users with combined anomaly information
        """
        # Detect basic anomalies
        basic_anomalies = self.detect_basic_anomalies(user_stats)

        # Detect off-hour anomalies
        offhour_anomalies = self.detect_offhour_anomalies(offhour_counts)

        # Detect spike anomalies
        spike_anomalies = self.detect_spike_anomalies(user_stats)

        # Combine all anomalies
        all_anomalous_users = {}

        # Start with basic anomalies
        for user_login, data in basic_anomalies.items():
            all_anomalous_users[user_login] = data

        # Add off-hour anomalies
        for user_login, data in offhour_anomalies.items():
            if user_login in all_anomalous_users:
                # Add to existing anomaly types
                if 'anomaly_types' not in all_anomalous_users[user_login]:
                    all_anomalous_users[user_login]['anomaly_types'] = []

                all_anomalous_users[user_login]['anomaly_types'].append({
                    'type': 'offhour',
                    'value': data['offhour_download_count'],
                    'threshold': data['threshold']
                })
            else:
                # Create new entry
                all_anomalous_users[user_login] = {
                    'user_login': user_login,
                    'user_name': user_stats.get(user_login, {}).get('user_name', ''),
                    'download_count': user_stats.get(user_login, {}).get('download_count', 0),
                    'unique_files_count': user_stats.get(user_login, {}).get('unique_files_count', 0),
                    'events': user_stats.get(user_login, {}).get('events', []),
                    'anomaly_types': [{
                        'type': 'offhour',
                        'value': data['offhour_download_count'],
                        'threshold': data['threshold']
                    }]
                }

        # Add spike anomalies
        for user_login, data in spike_anomalies.items():
            if user_login in all_anomalous_users:
                if 'anomaly_types' not in all_anomalous_users[user_login]:
                    all_anomalous_users[user_login]['anomaly_types'] = []

                all_anomalous_users[user_login]['anomaly_types'].append({
                    'type': 'spike',
                    'value': data['max_downloads_in_window'],
                    'threshold': data['threshold'],
                    'window_minutes': data['window_minutes'],
                    'spike_start_time': data.get('spike_start_time')
                })
            else:
                all_anomalous_users[user_login] = {
                    'user_login': user_login,
                    'user_name': data.get('user_name', ''),
                    'download_count': user_stats.get(user_login, {}).get('download_count', 0),
                    'unique_files_count': user_stats.get(user_login, {}).get('unique_files_count', 0),
                    'events': user_stats.get(user_login, {}).get('events', []),
                    'anomaly_types': [{
                        'type': 'spike',
                        'value': data['max_downloads_in_window'],
                        'threshold': data['threshold'],
                        'window_minutes': data['window_minutes'],
                        'spike_start_time': data.get('spike_start_time')
                    }]
                }

        logger.info(f"Total anomalous users detected: {len(all_anomalous_users)}")
        return all_anomalous_users

    def get_anomaly_summary(self, anomalous_users: Dict[str, Dict[str, Any]]) -> str:
        """
        Get a human-readable summary of anomalies.

        Args:
            anomalous_users: Dictionary of anomalous users

        Returns:
            Summary string
        """
        if not anomalous_users:
            return "No anomalies detected."

        summary_lines = [f"Detected {len(anomalous_users)} anomalous users:"]

        for user_login, data in anomalous_users.items():
            user_name = data.get('user_name', 'Unknown')
            anomaly_types = data.get('anomaly_types', [])

            anomaly_descriptions = []
            for anomaly in anomaly_types:
                atype = anomaly['type']
                value = anomaly['value']
                threshold = anomaly['threshold']

                if atype == 'download_count':
                    anomaly_descriptions.append(f"Downloads: {value} (threshold: {threshold})")
                elif atype == 'unique_files':
                    anomaly_descriptions.append(f"Unique files: {value} (threshold: {threshold})")
                elif atype == 'offhour':
                    anomaly_descriptions.append(f"Off-hour downloads: {value} (threshold: {threshold})")
                elif atype == 'spike':
                    window = anomaly.get('window_minutes', 'N/A')
                    anomaly_descriptions.append(f"Spike: {value} downloads in {window} minutes (threshold: {threshold})")

            summary_lines.append(f"  - {user_name} ({user_login}): {', '.join(anomaly_descriptions)}")

        return '\n'.join(summary_lines)
