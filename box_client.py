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
