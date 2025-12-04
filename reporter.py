"""CSV report generation module."""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class CSVReporter:
    """CSV report generator."""

    def __init__(self, output_dir: str):
        """
        Initialize CSV reporter.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_file_downloads_report(
        self,
        file_stats: List[Dict[str, Any]],
        date_str: str,
        period_type: str
    ) -> str:
        """
        Write file downloads report.

        Args:
            file_stats: List of file statistics
            date_str: Date string (YYYYMMDD)
            period_type: 'confirmed' or 'tentative'

        Returns:
            Output file path
        """
        filename = f"box_file_downloads_{date_str}_{period_type}.csv"
        filepath = self.output_dir / filename

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=['file_id', 'file_name', 'download_count'])
            writer.writeheader()

            for stat in file_stats:
                writer.writerow({
                    'file_id': stat.get('file_id', ''),
                    'file_name': stat.get('file_name', ''),
                    'download_count': stat.get('download_count', 0)
                })

        logger.info(f"Written file downloads report: {filepath}")
        return str(filepath)

    def write_user_file_downloads_report(
        self,
        user_file_stats: List[Dict[str, Any]],
        date_str: str,
        period_type: str
    ) -> str:
        """
        Write user-file downloads report.

        Args:
            user_file_stats: List of user-file statistics
            date_str: Date string (YYYYMMDD)
            period_type: 'confirmed' or 'tentative'

        Returns:
            Output file path
        """
        filename = f"box_user_file_downloads_{date_str}_{period_type}.csv"
        filepath = self.output_dir / filename

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'user_login',
                'user_name',
                'file_id',
                'file_name',
                'download_count',
                'last_download_at'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for stat in user_file_stats:
                writer.writerow({
                    'user_login': stat.get('user_login', ''),
                    'user_name': stat.get('user_name', ''),
                    'file_id': stat.get('file_id', ''),
                    'file_name': stat.get('file_name', ''),
                    'download_count': stat.get('download_count', 0),
                    'last_download_at': stat.get('last_download_at', '')
                })

        logger.info(f"Written user-file downloads report: {filepath}")
        return str(filepath)

    def write_access_log(
        self,
        events: List[Dict[str, Any]],
        date_str: str,
        period_type: str,
        output_dir: str = None
    ) -> str:
        """
        Write detailed access log.

        Args:
            events: List of download events
            date_str: Date string (YYYYMMDD)
            period_type: 'confirmed' or 'tentative'
            output_dir: Optional custom output directory

        Returns:
            Output file path
        """
        output_path = Path(output_dir) if output_dir else self.output_dir
        output_path.mkdir(parents=True, exist_ok=True)

        filename = f"access_log_{date_str}_{period_type}.csv"
        filepath = output_path / filename

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'event_id',
                'stream_type',
                'event_type',
                'user_login',
                'user_name',
                'file_id',
                'file_name',
                'download_at_utc',
                'download_at_jst',
                'ip_address',
                'client_type',
                'user_agent'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for event in events:
                writer.writerow({
                    'event_id': event.get('event_id', ''),
                    'stream_type': event.get('stream_type', ''),
                    'event_type': event.get('event_type', ''),
                    'user_login': event.get('user_login', ''),
                    'user_name': event.get('user_name', ''),
                    'file_id': event.get('file_id', ''),
                    'file_name': event.get('file_name', ''),
                    'download_at_utc': event.get('download_at_utc', ''),
                    'download_at_jst': event.get('download_at_jst', ''),
                    'ip_address': event.get('ip_address', ''),
                    'client_type': event.get('client_type', ''),
                    'user_agent': event.get('user_agent', '')
                })

        logger.info(f"Written access log: {filepath}")
        return str(filepath)

    def write_anomaly_details(
        self,
        anomalous_users: Dict[str, Dict[str, Any]],
        date_str: str,
        period_type: str,
        output_dir: str = None,
        max_rows: int = None
    ) -> str:
        """
        Write anomaly details CSV (for email attachment).

        Args:
            anomalous_users: Dictionary of anomalous users with their events
            date_str: Date string (YYYYMMDD)
            period_type: 'confirmed' or 'tentative'
            output_dir: Optional custom output directory
            max_rows: Maximum number of rows to write (None for unlimited)

        Returns:
            Output file path
        """
        from datetime import datetime, timedelta, timezone

        output_path = Path(output_dir) if output_dir else self.output_dir
        output_path.mkdir(parents=True, exist_ok=True)

        # Determine anomaly types for filename
        all_anomaly_types = set()
        for data in anomalous_users.values():
            for anomaly in data.get('anomaly_types', []):
                all_anomaly_types.add(anomaly.get('type', 'unknown'))

        # Create filename with anomaly types
        if len(all_anomaly_types) == 1:
            type_suffix = list(all_anomaly_types)[0]
        elif len(all_anomaly_types) > 1:
            type_suffix = 'mixed'
        else:
            type_suffix = 'unknown'

        filename = f"anomaly_details_{date_str}_{period_type}_{type_suffix}.csv"
        filepath = output_path / filename

        # Collect all events from anomalous users with anomaly info
        all_events = []
        for user_login, data in anomalous_users.items():
            # Build anomaly type string and details
            anomaly_types = data.get('anomaly_types', [])
            types_str = '+'.join([a.get('type', '') for a in anomaly_types])
            details_str = '; '.join([
                f"{a.get('type', '')}:{a.get('value', '')}/" + str(a.get('threshold', ''))
                for a in anomaly_types
            ])

            events = data.get('events', [])
            for event in events:
                event_copy = event.copy()
                event_copy['anomaly_types'] = types_str
                event_copy['anomaly_details'] = details_str
                all_events.append(event_copy)

        # Sort by user and download time
        all_events.sort(key=lambda e: (e.get('user_login', ''), e.get('download_at_jst', '')))

        # Apply max_rows limit if specified
        if max_rows and len(all_events) > max_rows:
            logger.warning(f"Anomaly details has {len(all_events)} rows, truncating to {max_rows}")
            all_events = all_events[:max_rows]

        # JST timezone
        jst = timezone(timedelta(hours=9))

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'anomaly_types',
                'anomaly_details',
                'event_type',
                'user_login',
                'user_name',
                'file_id',
                'file_name',
                'download_at_jst',
                'ip_address'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for event in all_events:
                # download_at_jstのみを使用（DBには常にJSTが格納されている）
                download_time = event.get('download_at_jst', '')

                # フォーマット変換（ISO形式→表示形式）
                if download_time:
                    try:
                        if 'T' in download_time:
                            dt = datetime.fromisoformat(download_time)
                            download_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        pass  # そのまま使用

                # event_type を日本語表記に変換
                event_type = event.get('event_type', 'DOWNLOAD')
                event_type_display = 'プレビュー' if event_type == 'PREVIEW' else 'ダウンロード'

                writer.writerow({
                    'anomaly_types': event.get('anomaly_types', ''),
                    'anomaly_details': event.get('anomaly_details', ''),
                    'event_type': event_type_display,
                    'user_login': event.get('user_login', ''),
                    'user_name': event.get('user_name', ''),
                    'file_id': event.get('file_id', ''),
                    'file_name': event.get('file_name', ''),
                    'download_at_jst': download_time,
                    'ip_address': event.get('ip_address', '')
                })

        logger.info(f"Written anomaly details: {filepath} ({len(all_events)} rows)")
        return str(filepath)

    def write_monthly_user_summary(
        self,
        user_summary: List[Dict[str, Any]],
        month_str: str
    ) -> str:
        """
        Write monthly user summary report.

        Args:
            user_summary: List of user summary data
            month_str: Month string (YYYYMM)

        Returns:
            Output file path
        """
        filename = f"monthly_user_summary_{month_str}.csv"
        filepath = self.output_dir / filename

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'month',
                'user_login',
                'user_name',
                'total_downloads',
                'active_days'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for summary in user_summary:
                writer.writerow({
                    'month': summary.get('month', ''),
                    'user_login': summary.get('user_login', ''),
                    'user_name': summary.get('user_name', ''),
                    'total_downloads': summary.get('total_downloads', 0),
                    'active_days': summary.get('active_days', 0)
                })

        logger.info(f"Written monthly user summary: {filepath}")
        return str(filepath)

    def write_monthly_file_summary(
        self,
        file_summary: List[Dict[str, Any]],
        month_str: str
    ) -> str:
        """
        Write monthly file summary report.

        Args:
            file_summary: List of file summary data
            month_str: Month string (YYYYMM)

        Returns:
            Output file path
        """
        filename = f"monthly_file_summary_{month_str}.csv"
        filepath = self.output_dir / filename

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = [
                'month',
                'file_id',
                'file_name',
                'total_downloads',
                'unique_users'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for summary in file_summary:
                writer.writerow({
                    'month': summary.get('month', ''),
                    'file_id': summary.get('file_id', ''),
                    'file_name': summary.get('file_name', ''),
                    'total_downloads': summary.get('total_downloads', 0),
                    'unique_users': summary.get('unique_users', 0)
                })

        logger.info(f"Written monthly file summary: {filepath}")
        return str(filepath)
