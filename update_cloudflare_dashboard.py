"""
Update Cloudflare Pages Dashboard - Deploy dashboard to Cloudflare Pages
ダッシュボードをCloudflare Pagesにデプロイ
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from datetime import datetime


def deploy_to_cloudflare(dashboard_path: Path, project_name: str) -> bool:
    """
    Deploy dashboard to Cloudflare Pages using Wrangler CLI.

    Args:
        dashboard_path: Path to dashboard HTML file
        project_name: Cloudflare Pages project name

    Returns:
        True if successful, False otherwise
    """
    print("\n" + "=" * 80)
    print("Cloudflare Pages Dashboard Deployment")
    print(f"開始時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    print("=" * 80)

    # Check if dashboard file exists
    if not dashboard_path.exists():
        print(f"[ERROR] Dashboard file not found: {dashboard_path}")
        return False

    print(f"[INFO] Dashboard file: {dashboard_path}")
    print(f"[INFO] File size: {dashboard_path.stat().st_size:,} bytes")
    print(f"[INFO] Last modified: {datetime.fromtimestamp(dashboard_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")

    # Create temporary directory for deployment
    temp_dir = Path("temp_cloudflare_deploy")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(exist_ok=True)

    # Copy dashboard to index.html in temp directory
    target_file = temp_dir / "index.html"
    print(f"\n[INFO] Copying dashboard to {target_file}...")
    shutil.copy2(dashboard_path, target_file)

    # Create robots.txt to block search engines
    robots_file = temp_dir / "robots.txt"
    robots_content = """User-agent: *
Disallow: /
"""
    robots_file.write_text(robots_content)
    print(f"[INFO] Created robots.txt to block search engine indexing")

    # Deploy to Cloudflare Pages
    print(f"\n[INFO] Deploying to Cloudflare Pages (project: {project_name})...")
    print("-" * 80)

    try:
        # Use npx wrangler for deployment
        # --branch=main を指定してProduction環境にデプロイ
        import platform
        if platform.system() == "Windows":
            cmd = [
                "npx.cmd", "wrangler", "pages", "deploy",
                str(temp_dir),
                "--project-name", project_name,
                "--branch=main",
                "--commit-dirty=true"
            ]
        else:
            cmd = [
                "npx", "wrangler", "pages", "deploy",
                str(temp_dir),
                "--project-name", project_name,
                "--branch=main",
                "--commit-dirty=true"
            ]

        print(f"[DEBUG] Command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=False
        )

        # Print stdout with error handling
        if result.stdout:
            try:
                print(result.stdout)
            except UnicodeEncodeError:
                print(result.stdout.encode('ascii', 'ignore').decode('ascii'))

        if result.returncode == 0:
            print("\n" + "=" * 80)
            print("[SUCCESS] Cloudflare Pages deployment completed!")
            print(f"終了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
            print("=" * 80)
            print(f"\nDeployed URL: https://{project_name}.pages.dev/")
            return True
        else:
            print(f"\n[ERROR] Cloudflare Pages deployment failed:")
            if result.stderr:
                try:
                    print(result.stderr)
                except UnicodeEncodeError:
                    print(result.stderr.encode('ascii', 'ignore').decode('ascii'))
            return False

    except FileNotFoundError:
        print("[ERROR] npx/wrangler not found. Please install Node.js and run: npm install -g wrangler")
        return False
    except Exception as e:
        print(f"[ERROR] Deployment error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"[INFO] Cleaned up temp directory: {temp_dir}")


def main():
    """メイン処理"""
    # Get dashboard path from environment or use default
    dashboard_output_dir = os.getenv("REPORT_OUTPUT_DIR", "C:\\box_reports")
    dashboard_path = Path(dashboard_output_dir) / "dashboard_period_allinone_full.html"

    # Cloudflare Pages project name
    project_name = os.getenv("CLOUDFLARE_PAGES_PROJECT", "box-dashboard-report")

    # Deploy to Cloudflare Pages
    success = deploy_to_cloudflare(dashboard_path, project_name)

    return 0 if success else 1


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
