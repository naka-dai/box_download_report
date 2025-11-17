"""
Box Reports Fetcher
Box管理コンソールで作成されたレポートCSVを取得するモジュール
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from boxsdk import Client

logger = logging.getLogger(__name__)


class BoxReportsFetcher:
    """Box Reportsフォルダからレポートファイルを取得"""

    def __init__(self, client: Client, reports_folder_id: str = "248280918136"):
        """
        Initialize the Box Reports Fetcher.

        Args:
            client: Authenticated Box client
            reports_folder_id: Box Reports folder ID (default: 248280918136)
        """
        self.client = client
        self.reports_folder_id = reports_folder_id

    def get_latest_report(
        self,
        name_pattern: Optional[str] = None,
        max_age_days: Optional[int] = None,
        search_subfolders: bool = True
    ) -> Optional[Dict]:
        """
        Get the latest report file from Box Reports folder.

        Args:
            name_pattern: Optional filename pattern to filter (e.g., "download", "event")
            max_age_days: Optional maximum age in days (e.g., 7 for files within last week)
            search_subfolders: Search in subfolders (default: True)

        Returns:
            Dictionary with file info, or None if no matching file found
        """
        try:
            logger.info(f"Searching for latest report in folder {self.reports_folder_id}...")

            # Get all items in Box Reports folder
            folder = self.client.folder(self.reports_folder_id)
            items = folder.get_items(limit=1000, fields=['id', 'name', 'type', 'modified_at', 'size', 'created_at'])

            # Filter CSV files
            csv_files = []
            folders_to_search = []

            for item in items:
                if item.type == 'folder' and search_subfolders:
                    folders_to_search.append(item)
                elif item.type == 'file':
                    # Check if it's a CSV file
                    if not item.name.lower().endswith('.csv'):
                        continue

                    # Apply name pattern filter if provided
                    if name_pattern and name_pattern.lower() not in item.name.lower():
                        continue

                    # Apply age filter if provided
                    if max_age_days:
                        try:
                            modified_date = datetime.fromisoformat(item.modified_at.replace('Z', '+00:00'))
                            age = datetime.now(modified_date.tzinfo) - modified_date
                            if age.days > max_age_days:
                                continue
                        except:
                            pass

                    csv_files.append({
                        'id': item.id,
                        'name': item.name,
                        'modified_at': item.modified_at,
                        'created_at': item.created_at,
                        'size': item.size,
                        'parent_id': self.reports_folder_id
                    })

            # Search in subfolders
            if search_subfolders:
                logger.info(f"Searching {len(folders_to_search)} subfolders...")
                for subfolder in folders_to_search:
                    try:
                        sub_items = subfolder.get_items(limit=100, fields=['id', 'name', 'type', 'modified_at', 'size', 'created_at'])
                        for sub_item in sub_items:
                            if sub_item.type != 'file':
                                continue

                            # Check if it's a CSV file
                            if not sub_item.name.lower().endswith('.csv'):
                                continue

                            # Apply name pattern filter if provided
                            if name_pattern and name_pattern.lower() not in sub_item.name.lower():
                                continue

                            # Apply age filter if provided
                            if max_age_days:
                                try:
                                    modified_date = datetime.fromisoformat(sub_item.modified_at.replace('Z', '+00:00'))
                                    age = datetime.now(modified_date.tzinfo) - modified_date
                                    if age.days > max_age_days:
                                        continue
                                except:
                                    pass

                            csv_files.append({
                                'id': sub_item.id,
                                'name': sub_item.name,
                                'modified_at': sub_item.modified_at,
                                'created_at': sub_item.created_at,
                                'size': sub_item.size,
                                'parent_id': subfolder.id,
                                'parent_name': subfolder.name
                            })
                    except Exception as e:
                        logger.warning(f"Could not access subfolder {subfolder.name}: {e}")
                        continue

            if not csv_files:
                logger.warning("No matching CSV files found in Box Reports folder")
                return None

            # Sort by modified date (most recent first)
            csv_files.sort(key=lambda x: x['modified_at'], reverse=True)

            latest = csv_files[0]
            logger.info(f"Found latest report: {latest['name']}")
            logger.info(f"  File ID: {latest['id']}")
            logger.info(f"  Modified: {latest['modified_at']}")
            logger.info(f"  Size: {latest['size']:,} bytes")

            return latest

        except Exception as e:
            logger.error(f"Error getting latest report: {e}", exc_info=True)
            return None

    def download_report(self, file_id: str, output_path: str) -> bool:
        """
        Download a report file from Box.

        Args:
            file_id: Box file ID
            output_path: Local path to save the file

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Downloading file {file_id} to {output_path}...")

            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Download file
            file_obj = self.client.file(file_id)
            with open(output_path, 'wb') as f:
                file_obj.download_to(f)

            file_size = os.path.getsize(output_path)
            logger.info(f"Download complete: {file_size:,} bytes")

            return True

        except Exception as e:
            logger.error(f"Error downloading report: {e}", exc_info=True)
            return False

    def list_recent_reports(self, days: int = 7) -> List[Dict]:
        """
        List all reports created/modified in the last N days.

        Args:
            days: Number of days to look back

        Returns:
            List of file info dictionaries
        """
        try:
            logger.info(f"Listing reports from last {days} days...")

            # Get all items in Box Reports folder
            folder = self.client.folder(self.reports_folder_id)
            items = folder.get_items(limit=1000, fields=['id', 'name', 'type', 'modified_at', 'size', 'created_at'])

            # Filter CSV files within time range
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_files = []

            for item in items:
                if item.type != 'file':
                    continue

                if not item.name.lower().endswith('.csv'):
                    continue

                try:
                    # Parse modified date
                    modified_date = datetime.fromisoformat(item.modified_at.replace('Z', '+00:00'))

                    # Make cutoff_date timezone-aware
                    if modified_date.tzinfo:
                        cutoff_date = cutoff_date.replace(tzinfo=modified_date.tzinfo)

                    if modified_date >= cutoff_date:
                        recent_files.append({
                            'id': item.id,
                            'name': item.name,
                            'modified_at': item.modified_at,
                            'created_at': item.created_at,
                            'size': item.size
                        })
                except Exception as e:
                    logger.warning(f"Could not parse date for {item.name}: {e}")
                    continue

            # Sort by modified date (most recent first)
            recent_files.sort(key=lambda x: x['modified_at'], reverse=True)

            logger.info(f"Found {len(recent_files)} reports from last {days} days")

            return recent_files

        except Exception as e:
            logger.error(f"Error listing recent reports: {e}", exc_info=True)
            return []

    def get_report_by_name(self, name_pattern: str) -> Optional[Dict]:
        """
        Get a specific report file by name pattern.

        Args:
            name_pattern: Filename pattern to search for

        Returns:
            Dictionary with file info, or None if not found
        """
        return self.get_latest_report(name_pattern=name_pattern)
