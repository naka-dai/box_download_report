"""Monthly summary generation module."""

import logging
from typing import List, Dict, Any
from datetime import datetime
from collections import defaultdict
from db import Database

logger = logging.getLogger(__name__)


class MonthlySummaryGenerator:
    """Generator for monthly summary reports."""

    def __init__(self, db: Database):
        """
        Initialize monthly summary generator.

        Args:
            db: Database instance
        """
        self.db = db

    def generate_monthly_summaries(self, month: str) -> None:
        """
        Generate monthly summaries for a specific month.

        Args:
            month: Month in YYYY-MM format
        """
        logger.info(f"Generating monthly summaries for {month}")

        # Generate user summary
        user_summary = self._generate_user_summary(month)
        self._save_user_summary(month, user_summary)

        # Generate file summary
        file_summary = self._generate_file_summary(month)
        self._save_file_summary(month, file_summary)

        logger.info(f"Monthly summaries generated for {month}")

    def _generate_user_summary(self, month: str) -> List[Dict[str, Any]]:
        """
        Generate user summary for a month.

        Args:
            month: Month in YYYY-MM format

        Returns:
            List of user summary data
        """
        # Calculate date range for the month
        year, month_num = month.split('-')
        start_date = f"{year}-{month_num}-01T00:00:00+09:00"

        # Calculate end date (first day of next month)
        if month_num == '12':
            next_year = str(int(year) + 1)
            next_month = '01'
        else:
            next_year = year
            next_month = str(int(month_num) + 1).zfill(2)
        end_date = f"{next_year}-{next_month}-01T00:00:00+09:00"

        # Get all downloads for the month
        downloads = self.db.get_downloads_by_period(start_date, end_date)

        # Aggregate by user
        user_stats = defaultdict(lambda: {
            'user_login': '',
            'user_name': '',
            'total_downloads': 0,
            'download_dates': set()
        })

        for download in downloads:
            user_login = download.get('user_login', '')
            user_name = download.get('user_name', '')
            download_at = download.get('download_at_jst', '')

            if user_login:
                user_stats[user_login]['user_login'] = user_login
                user_stats[user_login]['user_name'] = user_name
                user_stats[user_login]['total_downloads'] += 1

                # Extract date (YYYY-MM-DD) from datetime
                if download_at:
                    try:
                        dt = datetime.fromisoformat(download_at)
                        date_str = dt.strftime('%Y-%m-%d')
                        user_stats[user_login]['download_dates'].add(date_str)
                    except Exception as e:
                        logger.warning(f"Failed to parse date {download_at}: {e}")

        # Convert to list format
        summary = []
        for user_login, stats in user_stats.items():
            summary.append({
                'user_login': stats['user_login'],
                'user_name': stats['user_name'],
                'total_downloads': stats['total_downloads'],
                'active_days': len(stats['download_dates'])
            })

        # Sort by total downloads (descending)
        summary.sort(key=lambda x: x['total_downloads'], reverse=True)

        logger.info(f"Generated user summary for {month}: {len(summary)} users")
        return summary

    def _generate_file_summary(self, month: str) -> List[Dict[str, Any]]:
        """
        Generate file summary for a month.

        Args:
            month: Month in YYYY-MM format

        Returns:
            List of file summary data
        """
        # Calculate date range for the month
        year, month_num = month.split('-')
        start_date = f"{year}-{month_num}-01T00:00:00+09:00"

        # Calculate end date (first day of next month)
        if month_num == '12':
            next_year = str(int(year) + 1)
            next_month = '01'
        else:
            next_year = year
            next_month = str(int(month_num) + 1).zfill(2)
        end_date = f"{next_year}-{next_month}-01T00:00:00+09:00"

        # Get all downloads for the month
        downloads = self.db.get_downloads_by_period(start_date, end_date)

        # Aggregate by file
        file_stats = defaultdict(lambda: {
            'file_id': '',
            'file_name': '',
            'total_downloads': 0,
            'unique_users': set()
        })

        for download in downloads:
            file_id = download.get('file_id', '')
            file_name = download.get('file_name', '')
            user_login = download.get('user_login', '')

            if file_id:
                file_stats[file_id]['file_id'] = file_id
                file_stats[file_id]['file_name'] = file_name
                file_stats[file_id]['total_downloads'] += 1

                if user_login:
                    file_stats[file_id]['unique_users'].add(user_login)

        # Convert to list format
        summary = []
        for file_id, stats in file_stats.items():
            summary.append({
                'file_id': stats['file_id'],
                'file_name': stats['file_name'],
                'total_downloads': stats['total_downloads'],
                'unique_users': len(stats['unique_users'])
            })

        # Sort by total downloads (descending)
        summary.sort(key=lambda x: x['total_downloads'], reverse=True)

        logger.info(f"Generated file summary for {month}: {len(summary)} files")
        return summary

    def _save_user_summary(self, month: str, summary: List[Dict[str, Any]]) -> None:
        """
        Save user summary to database.

        Args:
            month: Month in YYYY-MM format
            summary: User summary data
        """
        for item in summary:
            self.db.upsert_monthly_user_summary(
                month=month,
                user_login=item['user_login'],
                user_name=item['user_name'],
                total_downloads=item['total_downloads'],
                active_days=item['active_days']
            )

        logger.info(f"Saved {len(summary)} user summaries to database")

    def _save_file_summary(self, month: str, summary: List[Dict[str, Any]]) -> None:
        """
        Save file summary to database.

        Args:
            month: Month in YYYY-MM format
            summary: File summary data
        """
        for item in summary:
            self.db.upsert_monthly_file_summary(
                month=month,
                file_id=item['file_id'],
                file_name=item['file_name'],
                total_downloads=item['total_downloads'],
                unique_users=item['unique_users']
            )

        logger.info(f"Saved {len(summary)} file summaries to database")

    def should_generate_monthly_summary(self, current_date: datetime) -> str:
        """
        Check if monthly summary should be generated.

        Args:
            current_date: Current date

        Returns:
            Month string (YYYY-MM) if summary should be generated, None otherwise
        """
        # Generate summary on the 1st day of the month for the previous month
        if current_date.day == 1:
            # Get previous month
            if current_date.month == 1:
                prev_year = current_date.year - 1
                prev_month = 12
            else:
                prev_year = current_date.year
                prev_month = current_date.month - 1

            month_str = f"{prev_year}-{str(prev_month).zfill(2)}"
            logger.info(f"Monthly summary should be generated for {month_str}")
            return month_str

        return None
