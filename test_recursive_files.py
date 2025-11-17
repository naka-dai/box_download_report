"""Test recursive file retrieval from Box folder."""

import os
import logging
from boxsdk import Client, OAuth2

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_all_files_recursive(client, folder_id, max_depth=10, current_depth=0):
    """
    Recursively get all files in a folder and its subfolders.

    Args:
        client: Box client
        folder_id: Folder ID to start from
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth

    Returns:
        List of file dictionaries
    """
    files = []

    if current_depth >= max_depth:
        logger.warning(f"Max depth {max_depth} reached at folder {folder_id}")
        return files

    try:
        folder = client.folder(folder_id)
        items = folder.get_items(limit=1000)

        for item in items:
            if item.type == 'file':
                files.append({
                    'id': item.id,
                    'name': item.name,
                    'size': getattr(item, 'size', 0),
                    'folder_id': folder_id,
                    'depth': current_depth
                })
                logger.debug(f"{'  ' * current_depth}[FILE] {item.name} (ID: {item.id})")

            elif item.type == 'folder':
                logger.debug(f"{'  ' * current_depth}[FOLDER] {item.name} (ID: {item.id})")
                # Recursively get files from subfolder
                subfolder_files = get_all_files_recursive(
                    client,
                    item.id,
                    max_depth,
                    current_depth + 1
                )
                files.extend(subfolder_files)

    except Exception as e:
        logger.error(f"Error processing folder {folder_id}: {e}")

    return files


def test_recursive_file_retrieval():
    """Test recursive file retrieval."""
    try:
        # Get credentials
        developer_token = os.getenv('BOX_DEVELOPER_TOKEN')
        folder_id = os.getenv('BOX_ROOT_FOLDER_ID', '243194687037')

        if not developer_token:
            logger.error("BOX_DEVELOPER_TOKEN not found")
            return False

        logger.info(f"Starting recursive file retrieval for folder: {folder_id}")
        logger.info(f"Developer Token: ...{developer_token[-4:]}")

        # Create client
        auth = OAuth2(
            client_id='',
            client_secret='',
            access_token=developer_token,
        )
        client = Client(auth)

        # Get folder info
        folder = client.folder(folder_id).get()
        logger.info(f"\nTarget Folder: {folder.name} (ID: {folder.id})")
        logger.info(f"Starting recursive scan...\n")

        # Get all files recursively (limit depth to avoid too long processing)
        max_depth = 3  # Limit to 3 levels deep for testing
        logger.info(f"Max recursion depth: {max_depth}")

        all_files = get_all_files_recursive(client, folder_id, max_depth=max_depth)

        # Summary
        logger.info("\n" + "="*80)
        logger.info("RECURSIVE SCAN RESULTS")
        logger.info("="*80)
        logger.info(f"Total files found: {len(all_files)}")

        # Calculate total size
        total_size = sum(f.get('size', 0) for f in all_files)
        total_size_mb = total_size / (1024 * 1024)
        total_size_gb = total_size / (1024 * 1024 * 1024)

        logger.info(f"Total size: {total_size:,} bytes ({total_size_mb:.2f} MB / {total_size_gb:.2f} GB)")

        # Show sample files (first 20)
        logger.info("\nSample files (first 20):")
        for i, file_info in enumerate(all_files[:20], 1):
            size_kb = file_info.get('size', 0) / 1024
            depth = file_info.get('depth', 0)
            indent = "  " * depth
            logger.info(f"{i:3d}. {indent}{file_info['name'][:50]:50s} ({size_kb:>8.1f} KB) [ID: {file_info['id']}]")

        if len(all_files) > 20:
            logger.info(f"... and {len(all_files) - 20} more files")

        # Depth distribution
        depth_counts = {}
        for file_info in all_files:
            depth = file_info.get('depth', 0)
            depth_counts[depth] = depth_counts.get(depth, 0) + 1

        logger.info("\nFiles by depth level:")
        for depth in sorted(depth_counts.keys()):
            logger.info(f"  Level {depth}: {depth_counts[depth]:,} files")

        logger.info("\n" + "="*80)
        logger.info("Recursive file retrieval test completed successfully!")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"Error during recursive file retrieval test: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_recursive_file_retrieval()
    if success:
        print("\n[OK] Recursive file retrieval test PASSED")
    else:
        print("\n[FAILED] Recursive file retrieval test FAILED")
