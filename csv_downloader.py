"""
CSV Downloader
Box管理コンソールのUser ActivityレポートCSVをBox APIからダウンロード
"""

import os
import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class CSVDownloader:
    """Box APIからUser Activity CSVファイルをダウンロード"""

    def __init__(self, box_client, download_dir: str = None):
        """
        Initialize the CSV Downloader.

        Args:
            box_client: BoxClient instance
            download_dir: Directory to save downloaded CSV files (default: ./data)
        """
        self.box_client = box_client

        # Use executable directory if running as EXE, otherwise use current directory
        if download_dir:
            self.download_dir = Path(download_dir)
        else:
            import sys
            if getattr(sys, 'frozen', False):
                # Running as compiled exe
                base_dir = Path(sys.executable).parent
            else:
                # Running as script
                base_dir = Path(__file__).parent
            self.download_dir = base_dir / 'data'

        # Create download directory if it doesn't exist
        self.download_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"CSV download directory: {self.download_dir}")

    def parse_folder_date(self, folder_name: str) -> Optional[datetime]:
        """
        Parse date from User Activity folder name.

        Format: "User Activity run on 2025-11-25 02-32-30"

        Args:
            folder_name: Folder name

        Returns:
            datetime object or None if parsing fails
        """
        try:
            # Extract date and time from folder name
            # Pattern: "User Activity run on YYYY-MM-DD HH-MM-SS"
            match = re.search(r'(\d{4})-(\d{2})-(\d{2})\s+(\d{2})-(\d{2})-(\d{2})', folder_name)
            if match:
                year, month, day, hour, minute, second = match.groups()
                return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        except Exception as e:
            logger.warning(f"Failed to parse folder date: {folder_name}, error: {e}")
        return None

    def find_latest_user_activity_folder(self, box_reports_folder_id: str) -> Optional[tuple]:
        """
        Find the latest User Activity folder in Box Reports folder.

        Args:
            box_reports_folder_id: Box Reports folder ID

        Returns:
            Tuple of (folder_id, folder_name, folder_date) or None
        """
        try:
            logger.info(f"Searching for User Activity folders in Box Reports (ID: {box_reports_folder_id})")

            # Get items in Box Reports folder
            from boxsdk import Client
            client: Client = self.box_client.client
            folder = client.folder(folder_id=box_reports_folder_id)
            items = folder.get_items()

            # Filter User Activity folders
            user_activity_folders = []
            for item in items:
                if item.type == 'folder' and item.name.startswith('User Activity run on'):
                    folder_date = self.parse_folder_date(item.name)
                    if folder_date:
                        user_activity_folders.append((item.id, item.name, folder_date))

            if not user_activity_folders:
                logger.warning("No User Activity folders found")
                return None

            # Sort by date (newest first)
            user_activity_folders.sort(key=lambda x: x[2], reverse=True)

            latest = user_activity_folders[0]
            logger.info(f"Found {len(user_activity_folders)} User Activity folders")
            logger.info(f"Latest folder: {latest[1]} (ID: {latest[0]}, Date: {latest[2]})")

            return latest

        except Exception as e:
            logger.error(f"Error finding User Activity folder: {e}", exc_info=True)
            return None

    def download_csv_files(self, folder_id: str, folder_name: str) -> List[str]:
        """
        Download all CSV files from a User Activity folder.

        Args:
            folder_id: Folder ID to download from
            folder_name: Folder name (for logging)

        Returns:
            List of downloaded file paths
        """
        try:
            logger.info(f"Downloading CSV files from folder: {folder_name} (ID: {folder_id})")

            from boxsdk import Client
            client: Client = self.box_client.client
            folder = client.folder(folder_id=folder_id)
            items = folder.get_items()

            downloaded_files = []
            csv_count = 0

            for item in items:
                if item.type == 'file' and item.name.lower().endswith('.csv'):
                    csv_count += 1
                    try:
                        # Download file
                        file_content = client.file(file_id=item.id).content()

                        # Save to local directory
                        local_path = self.download_dir / item.name
                        with open(local_path, 'wb') as f:
                            f.write(file_content)

                        logger.info(f"Downloaded: {item.name} ({len(file_content):,} bytes) -> {local_path}")
                        downloaded_files.append(str(local_path))

                    except Exception as e:
                        logger.error(f"Failed to download {item.name}: {e}")
                        continue

            logger.info(f"Downloaded {len(downloaded_files)} out of {csv_count} CSV files")
            return downloaded_files

        except Exception as e:
            logger.error(f"Error downloading CSV files: {e}", exc_info=True)
            return []

    def download_latest_user_activity_csvs(self, box_reports_folder_id: str = "248280918136") -> List[str]:
        """
        Find and download CSV files from the latest User Activity folder.

        Args:
            box_reports_folder_id: Box Reports folder ID (default: 248280918136)

        Returns:
            List of downloaded CSV file paths
        """
        # Find latest User Activity folder
        folder_info = self.find_latest_user_activity_folder(box_reports_folder_id)

        if not folder_info:
            logger.error("No User Activity folder found")
            return []

        folder_id, folder_name, folder_date = folder_info

        # Download CSV files from the folder
        csv_files = self.download_csv_files(folder_id, folder_name)

        return csv_files
