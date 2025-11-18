"""
Box Daily Update - All-in-One Script
データ収集とダッシュボード生成を1つにまとめたスクリプト

実行順序:
1. BoxレポートCSVファイルからデータインポート
2. Box APIから最新データを取得してSQLiteに保存
3. 期間フィルター付きダッシュボードを生成
"""

import sys
import os
import glob
from pathlib import Path
from datetime import datetime

def main():
    """メイン処理"""
    print("=" * 80)
    print("Box Daily Update - データ収集とダッシュボード生成")
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)

    # ステップ1: CSVファイルからデータインポート
    print("\n[ステップ1] BoxレポートCSVファイルからデータインポート中...")
    print("-" * 80)

    try:
        from db import Database
        from csv_importer import CSVImporter
        from config import Config

        # CSVファイルを検索
        csv_dir = Config.REPORT_OUTPUT_DIR
        csv_pattern = os.path.join(csv_dir, "user_activity*.csv")
        csv_files = sorted(glob.glob(csv_pattern), reverse=True)

        if csv_files:
            print(f"見つかったCSVファイル: {len(csv_files)}件")
            # 最新の3ファイルのみ処理（ページ分割されたファイル対応）
            recent_csvs = csv_files[:10]

            with Database(Config.DB_PATH) as db:
                importer = CSVImporter(db)
                imported_count = importer.import_multiple_csvs(recent_csvs)
                print(f"[OK] CSVインポート完了: {imported_count:,}件のレコードを処理")
        else:
            print(f"[INFO] CSVファイルが見つかりません: {csv_pattern}")
            print("[INFO] Box APIからのデータ取得に進みます")

    except Exception as e:
        print(f"[WARNING] CSVインポート中にエラーが発生しました: {e}")
        print("[INFO] Box APIからのデータ取得に進みます")
        import traceback
        traceback.print_exc()

    # ステップ2: Box APIからデータ収集
    print("\n[ステップ2] Box APIからデータ収集中...")
    print("-" * 80)

    try:
        # main.pyをインポートして実行
        import main as data_collector
        data_collector.main()
        print("[OK] データ収集完了")

    except Exception as e:
        print(f"[WARNING] データ収集中にエラーが発生しました: {e}")
        print("[INFO] ダッシュボード生成を続行します（既存データを使用）")
        import traceback
        traceback.print_exc()

    # ステップ3: ダッシュボード生成
    print("\n[ステップ3] 期間フィルター付きダッシュボード生成中...")
    print("-" * 80)

    try:
        # generate_period_allinone_full.pyをインポートして実行
        import generate_period_allinone_full as dashboard_generator
        dashboard_generator.generate_dashboard()
        print("[OK] ダッシュボード生成完了")

    except Exception as e:
        print(f"[ERROR] ダッシュボード生成中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # 完了メッセージ
    print("\n" + "=" * 80)
    print("✓ Box Daily Update 完了")
    print(f"終了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)
    print("\n生成されたダッシュボード:")

    dashboard_path = Path("data") / "dashboard_period_allinone_full.html"
    if dashboard_path.exists():
        abs_path = dashboard_path.absolute()
        print(f"  {abs_path}")
        print(f"  file:///{str(abs_path).replace(chr(92), '/')}")
    else:
        print("  [WARNING] ダッシュボードファイルが見つかりません")

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[INFO] ユーザーによって中断されました")
        sys.exit(130)
    except Exception as e:
        print(f"\n[ERROR] 予期しないエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
