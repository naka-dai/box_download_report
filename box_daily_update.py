"""
Box Daily Update - All-in-One Script
データ収集とダッシュボード生成を1つにまとめたスクリプト

実行順序:
1. User Activity CSVファイルからデータインポート（オプション）
2. Box APIから最新データを取得してSQLiteに保存（オプション）
3. 期間フィルター付きダッシュボードを生成
4. Netlifyへダッシュボードをデプロイ（オプション）
"""

import sys
import os
import subprocess
import shutil
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
print(f"[DEBUG] Application path: {application_path}")
print(f"[DEBUG] Looking for .env at: {env_path}")
if os.path.exists(env_path):
    load_dotenv(env_path, override=True)
    print(f"[INFO] Loaded .env from: {env_path}")
    print(f"[DEBUG] DB_PATH from env: {os.getenv('DB_PATH')}")
    print(f"[DEBUG] SKIP_DATA_COLLECTION: {os.getenv('SKIP_DATA_COLLECTION')}")
    print(f"[DEBUG] SKIP_GITHUB_PUSH: {os.getenv('SKIP_GITHUB_PUSH')}")
else:
    print(f"[WARNING] .env not found at: {env_path}")
    load_dotenv(override=True)  # Try default behavior


def run_git_command(cmd: list, cwd: Path = None) -> tuple[int, str]:
    """Run git command and return (returncode, output)."""
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd or Path.cwd(),
            capture_output=True,
            text=True,
            encoding='utf-8',
            check=False
        )
        return result.returncode, result.stdout + result.stderr
    except Exception as e:
        return 1, str(e)


def push_to_github(dashboard_path: Path, repo_root: Path) -> bool:
    """
    Push dashboard to GitHub Pages.

    Args:
        dashboard_path: Path to dashboard HTML file
        repo_root: Path to repository root

    Returns:
        True if successful, False otherwise
    """
    # Check if we're in a git repository
    returncode, _ = run_git_command(['git', 'rev-parse', '--git-dir'], repo_root)
    if returncode != 0:
        print("[INFO] Not in a git repository. Skipping GitHub push.")
        return False

    # Check if dashboard file exists
    if not dashboard_path.exists():
        print(f"[WARNING] Dashboard file not found: {dashboard_path}")
        return False

    print("\n[ステップ3] GitHub Pagesへダッシュボードをプッシュ中...")
    print("-" * 80)

    try:
        # Get current branch
        returncode, output = run_git_command(['git', 'branch', '--show-current'], repo_root)
        if returncode != 0:
            print(f"[WARNING] Failed to get current branch: {output}")
            return False

        current_branch = output.strip()
        print(f"[INFO] Current branch: {current_branch}")

        # Stash any uncommitted changes
        if current_branch != "gh-pages":
            print("[INFO] Stashing uncommitted changes...")
            run_git_command(['git', 'stash', 'push', '-m', 'Auto-stash before dashboard update'], repo_root)

        # Checkout gh-pages branch
        print("[INFO] Switching to gh-pages branch...")
        returncode, output = run_git_command(['git', 'checkout', 'gh-pages'], repo_root)
        if returncode != 0:
            print(f"[WARNING] Failed to checkout gh-pages: {output}")
            print("[INFO] gh-pages branch may not exist. Skipping GitHub push.")
            run_git_command(['git', 'checkout', current_branch], repo_root)
            return False

        # Copy dashboard to index.html in repo root
        target_path = repo_root / "index.html"
        print(f"[INFO] Copying dashboard to {target_path}...")
        shutil.copy2(dashboard_path, target_path)

        # Update README with timestamp
        readme_path = repo_root / "README.md"
        if readme_path.exists():
            with open(readme_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Update timestamp in README
            import re
            timestamp = datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')
            content = re.sub(
                r'最終更新: \d{4}年\d{2}月\d{2}日 \d{2}:\d{2}:\d{2}',
                f'最終更新: {timestamp}',
                content
            )

            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print("[INFO] README updated")

        # Stage changes
        print("[INFO] Staging changes...")
        run_git_command(['git', 'add', 'index.html', 'README.md'], repo_root)

        # Commit
        commit_msg = f"""Update dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>
"""
        print("[INFO] Committing changes...")
        returncode, output = run_git_command(['git', 'commit', '-m', commit_msg], repo_root)
        if returncode != 0:
            print(f"[WARNING] Failed to commit: {output}")
            run_git_command(['git', 'checkout', current_branch], repo_root)
            return False

        # Push to GitHub
        print("[INFO] Pushing to GitHub...")
        returncode, output = run_git_command(['git', 'push', 'origin', 'gh-pages'], repo_root)
        if returncode != 0:
            print(f"[WARNING] Failed to push to GitHub: {output}")
            print("[INFO] コミットはローカルに保存されました")
        else:
            print("[OK] GitHub Pagesへのプッシュ完了")
            print("     URL: https://naka-dai.github.io/box_download_report/")

        # Switch back to original branch
        print(f"[INFO] Switching back to {current_branch} branch...")
        run_git_command(['git', 'checkout', current_branch], repo_root)

        # Pop stash if exists
        if current_branch != "gh-pages":
            run_git_command(['git', 'stash', 'pop'], repo_root)

        return returncode == 0

    except Exception as e:
        print(f"[ERROR] GitHub push failed: {e}")
        import traceback
        traceback.print_exc()
        # Try to restore original state
        run_git_command(['git', 'checkout', current_branch], repo_root)
        return False


def main():
    """メイン処理"""
    print("=" * 80)
    print("Box Daily Update - データ収集とダッシュボード生成")
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)

    # ステップ0: User Activity CSVをBox APIからダウンロードしてインポート（オプション）
    skip_csv_import = os.getenv("SKIP_CSV_IMPORT", "").lower() in ("1", "true", "yes")

    if skip_csv_import:
        print("\n[ステップ0] CSVインポートをスキップ (SKIP_CSV_IMPORT=1)")
        print("-" * 80)
    else:
        print("\n[ステップ0] User Activity CSVをBox APIからダウンロードしてインポート中...")
        print("-" * 80)

        try:
            from db import Database
            from csv_importer import CSVImporter
            from csv_downloader import CSVDownloader
            from box_client import BoxClient

            # Box APIクライアントを初期化
            print("[INFO] Box APIクライアント初期化中...")
            config_path = os.getenv("BOX_CONFIG_PATH", "config.json")
            box_client = BoxClient(config_path)

            # CSVダウンローダーを初期化
            # ダウンロード先: EXEファイルと同じフォルダ/data
            csv_downloader = CSVDownloader(box_client)

            # Box Reports フォルダIDを取得
            box_reports_folder_id = os.getenv("BOX_ROOT_FOLDER_ID", "248280918136")

            # 最新のUser Activity フォルダからCSVファイルをダウンロード
            print(f"[INFO] Box Reports フォルダ (ID: {box_reports_folder_id}) から最新のUser Activity CSVをダウンロード中...")
            csv_files = csv_downloader.download_latest_user_activity_csvs(box_reports_folder_id)

            if csv_files:
                print(f"[OK] {len(csv_files)}個のCSVファイルをダウンロード完了")

                # ダウンロードしたCSVファイルをデータベースにインポート
                db_path = os.getenv("DB_PATH", "C:\\box_reports\\box_audit.db")
                with Database(db_path) as db:
                    importer = CSVImporter(db)
                    imported_count = importer.import_multiple_csvs(csv_files)
                    print(f"[OK] CSVインポート完了: {imported_count:,}件のイベントをインポート")
            else:
                print("[WARNING] CSVファイルのダウンロードに失敗しました")
                print("[INFO] Box APIからのデータ取得に進みます")

        except Exception as e:
            print(f"[WARNING] CSVダウンロード/インポート中にエラーが発生しました: {e}")
            print("[INFO] Box APIからのデータ取得に進みます")
            import traceback
            traceback.print_exc()

    # ステップ1: Box APIからデータ収集
    skip_data_collection = os.getenv("SKIP_DATA_COLLECTION", "").lower() in ("1", "true", "yes")

    if skip_data_collection:
        print("\n[ステップ1] Box APIからのデータ収集をスキップ (SKIP_DATA_COLLECTION=1)")
        print("-" * 80)
        print("[INFO] 既存のデータベースを使用してダッシュボードを生成します")
    else:
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

    # ステップ3: Netlifyへデプロイ（オプション）
    skip_netlify_deploy = os.getenv("SKIP_NETLIFY_DEPLOY", "").lower() in ("1", "true", "yes")

    if skip_netlify_deploy:
        print("\n[ステップ3] Netlifyへのデプロイをスキップ (SKIP_NETLIFY_DEPLOY=1)")
        print("-" * 80)
        print("[INFO] ダッシュボードはローカルにのみ保存されました")
    else:
        # ダッシュボードのパスを取得
        dashboard_output_dir = os.getenv("REPORT_OUTPUT_DIR", "C:\\box_reports")
        dashboard_path = Path(dashboard_output_dir) / "dashboard_period_allinone_full.html"

        # Netlifyへデプロイ（ダッシュボードが存在する場合のみ）
        if dashboard_path.exists():
            try:
                import update_netlify_dashboard
                update_netlify_dashboard.deploy_to_netlify(
                    dashboard_path,
                    os.getenv("NETLIFY_SITE_ID", "47255fce-725c-48f1-a865-db146b183555")
                )
            except Exception as e:
                print(f"[WARNING] Netlify deploy failed: {e}")
                print("[INFO] ダッシュボードはローカルに保存されました")
                import traceback
                traceback.print_exc()
        else:
            print(f"\n[WARNING] ダッシュボードファイルが見つかりません: {dashboard_path}")

    # 完了メッセージ
    print("\n" + "=" * 80)
    print("[SUCCESS] Box Daily Update 完了")
    print(f"終了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)
    print("\n生成されたダッシュボード:")

    dashboard_output_dir = os.getenv("REPORT_OUTPUT_DIR", "C:\\box_reports")
    dashboard_path = Path(dashboard_output_dir) / "dashboard_period_allinone_full.html"
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
