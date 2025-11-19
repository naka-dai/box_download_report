"""
Box Daily Update - All-in-One Script
データ収集とダッシュボード生成を1つにまとめたスクリプト

実行順序:
1. Box APIから最新データを取得してSQLiteに保存
2. 期間フィルター付きダッシュボードを生成
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load .env from the same directory as this script/exe
if getattr(sys, 'frozen', False):
    # Running as compiled exe
    application_path = os.path.dirname(sys.executable)
else:
    # Running as script
    application_path = os.path.dirname(os.path.abspath(__file__))

env_path = os.path.join(application_path, '.env')
if os.path.exists(env_path):
    load_dotenv(env_path, override=True)
    print(f"[INFO] Loaded .env from: {env_path}")
else:
    print(f"[WARNING] .env not found at: {env_path}")
    load_dotenv(override=True)  # Try default behavior

def main():
    """メイン処理"""
    print("=" * 80)
    print("Box Daily Update - データ収集とダッシュボード生成")
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)

    # ステップ1: Box APIからデータ収集
    print("\n[ステップ1] Box APIからデータ収集中...")
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

    # ステップ2: ダッシュボード生成
    print("\n[ステップ2] 期間フィルター付きダッシュボード生成中...")
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
    print("[SUCCESS] Box Daily Update 完了")
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
