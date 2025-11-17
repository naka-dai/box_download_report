"""Test accessing the newly created report."""

import os
import json
import logging
from boxsdk import Client, JWTAuth

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_get_report():
    """Test accessing report ID 2048408329549."""
    try:
        config_path = os.path.expanduser("~/.box/config.json")
        admin_user_id = "16623033409"
        report_id = "2048408329549"

        logger.info("="*80)
        logger.info("作成されたレポートを取得")
        logger.info("="*80)

        # Load config and create JWT client
        with open(config_path, 'r') as f:
            config = json.load(f)

        auth = JWTAuth.from_settings_dictionary(config)
        service_client = Client(auth)
        client = service_client.as_user(service_client.user(admin_user_id))

        # Try to access as folder first
        logger.info(f"\nレポートID {report_id} をフォルダとしてアクセス...")
        try:
            folder = client.folder(report_id).get()
            logger.info(f"✓ フォルダとして取得成功:")
            logger.info(f"  名前: {folder.name}")
            logger.info(f"  ID: {folder.id}")
            logger.info(f"  Type: {folder.type}")
            logger.info(f"  Modified: {folder.modified_at}")

            # Get folder contents
            logger.info(f"\nフォルダの内容を取得中...")
            items = folder.get_items(limit=100, fields=['id', 'name', 'type', 'modified_at', 'size', 'created_at'])
            item_list = list(items)

            logger.info(f"\nアイテム数: {len(item_list)}")

            if item_list:
                logger.info("\nアイテム一覧:")
                for i, item in enumerate(item_list, 1):
                    item_type = "📁" if item.type == 'folder' else "📄"
                    modified = item.modified_at if hasattr(item, 'modified_at') else 'N/A'
                    size = f"{item.size:,} bytes" if hasattr(item, 'size') and item.size else 'N/A'

                    logger.info(f"{i:2d}. {item_type} {item.name}")
                    logger.info(f"     ID: {item.id}, Type: {item.type}")
                    logger.info(f"     Modified: {modified}, Size: {size}")
                    logger.info("")

                    # If it's a CSV file, download it
                    if item.type == 'file' and item.name.lower().endswith('.csv'):
                        logger.info(f"  → CSVファイルが見つかりました！")
                        output_dir = r"C:\box_reports"
                        os.makedirs(output_dir, exist_ok=True)
                        output_path = os.path.join(output_dir, item.name)

                        logger.info(f"  ダウンロード中: {output_path}")
                        file_obj = client.file(item.id)
                        with open(output_path, 'wb') as f:
                            file_obj.download_to(f)

                        file_size = os.path.getsize(output_path)
                        logger.info(f"  ✓ ダウンロード完了: {file_size:,} bytes")

                        # Show first few lines
                        logger.info(f"\n  CSVファイルの最初の5行:")
                        logger.info("  " + "-" * 78)
                        try:
                            with open(output_path, 'r', encoding='utf-8') as f:
                                for i, line in enumerate(f):
                                    if i >= 5:
                                        break
                                    logger.info(f"  {line.rstrip()}")
                        except UnicodeDecodeError:
                            # Try with different encoding
                            try:
                                with open(output_path, 'r', encoding='utf-8-sig') as f:
                                    for i, line in enumerate(f):
                                        if i >= 5:
                                            break
                                        logger.info(f"  {line.rstrip()}")
                            except:
                                with open(output_path, 'r', encoding='cp932') as f:
                                    for i, line in enumerate(f):
                                        if i >= 5:
                                            break
                                        logger.info(f"  {line.rstrip()}")
                        logger.info("  " + "-" * 78)

            else:
                logger.info("  (フォルダは空です)")

        except Exception as e:
            logger.error(f"フォルダとしてアクセス失敗: {e}")

            # Try as file
            logger.info(f"\nレポートID {report_id} をファイルとしてアクセス...")
            try:
                file_obj = client.file(report_id).get()
                logger.info(f"✓ ファイルとして取得成功:")
                logger.info(f"  名前: {file_obj.name}")
                logger.info(f"  ID: {file_obj.id}")
                logger.info(f"  Type: {file_obj.type}")
                logger.info(f"  Size: {file_obj.size:,} bytes")
            except Exception as e2:
                logger.error(f"ファイルとしてもアクセス失敗: {e2}")

        logger.info("\n" + "="*80)
        logger.info("テスト完了")
        logger.info("="*80)

        return True

    except Exception as e:
        logger.error(f"エラー: {e}", exc_info=True)
        return False


if __name__ == '__main__':
    success = test_get_report()
    if success:
        print("\n[OK] テスト成功")
    else:
        print("\n[FAILED] テスト失敗")
