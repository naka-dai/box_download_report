"""
Box Daily Update - All-in-One Script
データ収集とダッシュボード生成を1つにまとめたスクリプト

実行順序:
1. Box APIから最新データを取得してSQLiteに保存
2. 期間フィルター付きダッシュボードを生成
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

def main():
    """メイン処理"""
    script_dir = Path(__file__).parent

    print("=" * 80)
    print("Box Daily Update - データ収集とダッシュボード生成")
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)

    # ステップ1: データ収集バッチ実行
    print("\n[ステップ1] Box APIからデータ収集中...")
    print("-" * 80)

    main_script = script_dir / "main.py"
    if not main_script.exists():
        print(f"[ERROR] main.py が見つかりません: {main_script}")
        return 1

    try:
        # main.pyを実行
        result = subprocess.run(
            [sys.executable, str(main_script)],
            cwd=str(script_dir),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore'
        )

        # 出力を表示
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        if result.returncode != 0:
            print(f"[WARNING] データ収集バッチが警告付きで終了しました (終了コード: {result.returncode})")
            # エラーでも続行（データが既にある場合があるため）
        else:
            print("[OK] データ収集完了")

    except Exception as e:
        print(f"[ERROR] データ収集中にエラーが発生しました: {e}")
        print("[INFO] ダッシュボード生成を続行します（既存データを使用）")

    # ステップ2: ダッシュボード生成
    print("\n[ステップ2] 期間フィルター付きダッシュボード生成中...")
    print("-" * 80)

    dashboard_script = script_dir / "generate_period_allinone_full.py"
    if not dashboard_script.exists():
        print(f"[ERROR] generate_period_allinone_full.py が見つかりません: {dashboard_script}")
        return 1

    try:
        # generate_period_allinone_full.pyを実行
        result = subprocess.run(
            [sys.executable, str(dashboard_script)],
            cwd=str(script_dir),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore'
        )

        # 出力を表示
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        if result.returncode != 0:
            print(f"[ERROR] ダッシュボード生成が失敗しました (終了コード: {result.returncode})")
            return 1

        print("[OK] ダッシュボード生成完了")

    except Exception as e:
        print(f"[ERROR] ダッシュボード生成中にエラーが発生しました: {e}")
        return 1

    # 完了メッセージ
    print("\n" + "=" * 80)
    print("✓ Box Daily Update 完了")
    print(f"終了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)
    print("\n生成されたダッシュボード:")

    dashboard_path = script_dir / "data" / "dashboard_period_allinone_full.html"
    if dashboard_path.exists():
        print(f"  {dashboard_path}")
        print(f"  file:///{str(dashboard_path).replace(chr(92), '/')}")
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
