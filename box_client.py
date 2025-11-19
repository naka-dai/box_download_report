"""Box API client module using JWT authentication."""

import json
from pathlib import Path
from typing import Optional
from boxsdk import Client, JWTAuth
from boxsdk.object.folder import Folder
import logging

logger = logging.getLogger(__name__)


class BoxClient:
    """Box API client wrapper using JWT authentication."""

    def __init__(self, config_path: str):
        """
        Initialize Box client with JWT authentication.

        Args:
            config_path: Path to Box config.json file
        """
        self.config_path = config_path
        self.client: Optional[Client] = None
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with Box using JWT."""
        try:
            config_path = Path(self.config_path)
            if not config_path.exists():
                raise FileNotFoundError(f"Box config file not found: {self.config_path}")

            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            auth = JWTAuth.from_settings_dictionary(config)
            self.client = Client(auth)

            # Test authentication by getting current user
            user = self.client.user().get()
            logger.info(f"Successfully authenticated as: {user.name} ({user.login})")

        except Exception as e:
            logger.error(f"Failed to authenticate with Box: {e}")
            raise

    def get_client(self) -> Client:
        """
        Get the authenticated Box client.

        Returns:
            Authenticated Box client
        """
        if not self.client:
            raise RuntimeError("Box client not initialized")
        return self.client

    def get_folder(self, folder_id: str) -> Folder:
        """
        Get a Box folder by ID.

        Args:
            folder_id: Box folder ID

        Returns:
            Box Folder object
        """
        if not self.client:
            raise RuntimeError("Box client not initialized")

        try:
            folder = self.client.folder(folder_id).get()
            logger.info(f"Retrieved folder: {folder.name} (ID: {folder_id})")
            return folder
        except Exception as e:
            logger.error(f"Failed to retrieve folder {folder_id}: {e}")
            raise

    def get_all_file_ids_in_folder(self, folder_id: str) -> set[str]:
        """
        Recursively get all file IDs within a folder and its subfolders.

        Args:
            folder_id: Root folder ID

        Returns:
            Set of file IDs
        """
        if not self.client:
            raise RuntimeError("Box client not initialized")

        file_ids = set()

        def traverse_folder(current_folder_id: str) -> None:
            """Recursively traverse folders."""
            try:
                folder = self.client.folder(current_folder_id)
                items = folder.get_items()

                for item in items:
                    if item.type == 'file':
                        file_ids.add(item.id)
                    elif item.type == 'folder':
                        traverse_folder(item.id)
            except Exception as e:
                logger.warning(f"Error traversing folder {current_folder_id}: {e}")

        logger.info(f"Starting to traverse folder: {folder_id}")
        traverse_folder(folder_id)
        logger.info(f"Found {len(file_ids)} files in folder tree")

        return file_ids

    def get_latest_user_activity_folder(self, reports_folder_id: str) -> Optional[str]:
        """
        Find the latest 'User Activity run on ~' folder in Box Reports folder.

        Args:
            reports_folder_id: Box Reports parent folder ID

        Returns:
            Latest User Activity folder ID, or None if not found
        """
        if not self.client:
            raise RuntimeError("Box client not initialized")

        try:
            folder = self.client.folder(reports_folder_id)
            items = folder.get_items()

            user_activity_folders = []

            for item in items:
                if item.type == 'folder' and item.name.startswith('User Activity run on '):
                    user_activity_folders.append({
                        'id': item.id,
                        'name': item.name
                    })

            if not user_activity_folders:
                logger.warning("No 'User Activity run on ~' folders found")
                return None

            # Sort by name (descending) to get the latest
            # Format: "User Activity run on 2025-11-19 01-43-09"
            user_activity_folders.sort(key=lambda x: x['name'], reverse=True)
            latest_folder = user_activity_folders[0]

            logger.info(f"Found {len(user_activity_folders)} User Activity folders")
            logger.info(f"Latest User Activity folder: {latest_folder['name']} (ID: {latest_folder['id']})")

            return latest_folder['id']

        except Exception as e:
            logger.error(f"Failed to find latest User Activity folder: {e}")
            return None

    def get_file_info(self, file_id: str) -> dict:
        """
        Get file information.

        Args:
            file_id: Box file ID

        Returns:
            Dictionary with file information
        """
        if not self.client:
            raise RuntimeError("Box client not initialized")

        try:
            file = self.client.file(file_id).get()
            return {
                'id': file.id,
                'name': file.name,
                'size': file.size,
                'created_at': file.created_at,
                'modified_at': file.modified_at,
            }
        except Exception as e:
            logger.warning(f"Failed to get file info for {file_id}: {e}")
            return {
                'id': file_id,
                'name': 'Unknown',
                'size': None,
                'created_at': None,
                'modified_at': None,
            }
