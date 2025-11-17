"""Test finding and accessing the Box Reports folder."""

import os
import json
import logging
from boxsdk import Client, JWTAuth

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_reports_folder():
    """Find the Box Reports folder and list its contents."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"

        logger.info("="*80)
        logger.info("Box Reportsフォルダ検索テスト")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Get user info
        user = client.user().get()
        logger.info(f"\n認証ユーザー: {user.name} ({user.login})")

        # Search for "Box Reports" folder
        logger.info("\n'Box Reports'フォルダを検索中...")
        search_results = client.search().query(
            query="Box Reports",
            type="folder",
            limit=20
        )

        reports_folders = []
        for item in search_results:
            if item.type == 'folder' and 'reports' in item.name.lower():
                reports_folders.append(item)
                logger.info(f"  見つかりました: {item.name} (ID: {item.id})")

        if not reports_folders:
            logger.warning("\n⚠ 'Box Reports'フォルダが見つかりませんでした")

            # Try to get root folder items
            logger.info("\nルートフォルダ（0）の内容を確認中...")
            root_folder = client.folder('0').get()
            items = root_folder.get_items(limit=100)

            logger.info("\nルートフォルダ内のフォルダ一覧:")
            for item in items:
                if item.type == 'folder':
                    logger.info(f"  - {item.name} (ID: {item.id})")
                    if 'report' in item.name.lower():
                        reports_folders.append(item)

            return None

        # List contents of each Reports folder found
        for reports_folder in reports_folders:
            logger.info(f"\n{'='*80}")
            logger.info(f"フォルダ: {reports_folder.name} (ID: {reports_folder.id})")
            logger.info(f"{'='*80}")

            try:
                items = reports_folder.get_items(limit=50)
                item_list = list(items)

                logger.info(f"\nアイテム数: {len(item_list)}")

                if item_list:
                    logger.info("\n最新10件のアイテム:")
                    # Sort by modified date (most recent first)
                    sorted_items = sorted(
                        item_list,
                        key=lambda x: x.modified_at if hasattr(x, 'modified_at') else '',
                        reverse=True
                    )

                    for i, item in enumerate(sorted_items[:10], 1):
                        item_type = "📁" if item.type == 'folder' else "📄"
                        modified = item.modified_at if hasattr(item, 'modified_at') else 'N/A'
                        size = f"{item.size:,} bytes" if hasattr(item, 'size') and item.size else 'N/A'

                        logger.info(f"{i:2d}. {item_type} {item.name}")
                        logger.info(f"     ID: {item.id}, Type: {item.type}")
                        logger.info(f"     Modified: {modified}, Size: {size}")
                        logger.info("")
                else:
                    logger.info("\n  (フォルダは空です)")

            except Exception as e:
                logger.error(f"フォルダの内容取得エラー: {e}")

        logger.info("="*80)
        logger.info("テスト完了")
        logger.info("="*80)

        return reports_folders[0] if reports_folders else None

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return None


if __name__ == '__main__':
    folder = find_reports_folder()
    if folder:
        print(f"\n[OK] Box Reportsフォルダが見つかりました: {folder.name} (ID: {folder.id})")
    else:
        print("\n[FAILED] Box Reportsフォルダが見つかりませんでした")
