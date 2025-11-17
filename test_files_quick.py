"""Quick test to count files in Box folder (limited depth)."""

import os
import logging
from boxsdk import Client, OAuth2

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def count_files_quick(client, folder_id, max_depth=2, current_depth=0):
    """Quick file count with limited depth."""
    file_count = 0
    folder_count = 0

    if current_depth >= max_depth:
        return file_count, folder_count

    try:
        folder = client.folder(folder_id)
        items = folder.get_items(limit=100)  # Limit to 100 items per folder

        for item in items:
            if item.type == 'file':
                file_count += 1
            elif item.type == 'folder':
                folder_count += 1
                sub_files, sub_folders = count_files_quick(client, item.id, max_depth, current_depth + 1)
                file_count += sub_files
                folder_count += sub_folders
    except Exception as e:
        logger.error(f"Error: {e}")

    return file_count, folder_count


# Main
developer_token = os.getenv('BOX_DEVELOPER_TOKEN')
folder_id = os.getenv('BOX_ROOT_FOLDER_ID', '243194687037')

print(f"Scanning folder: {folder_id}")
print(f"Token: ...{developer_token[-4:]}")

auth = OAuth2(client_id='', client_secret='', access_token=developer_token)
client = Client(auth)

# Get folder name
folder = client.folder(folder_id).get()
print(f"Folder name: {folder.name}")
print("\nScanning (max depth: 2, max 100 items per folder)...")

files, folders = count_files_quick(client, folder_id, max_depth=2)

print(f"\nResults:")
print(f"  Files found: {files}")
print(f"  Folders found: {folders}")
print(f"  Total items: {files + folders}")
print("\n[OK] Quick file count completed")
