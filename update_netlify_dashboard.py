"""
Update Netlify Dashboard - Deploy dashboard to Netlify
ダッシュボードをNetlifyにデプロイ
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def deploy_to_netlify(dashboard_path: Path, site_id: str) -> bool:
    """
    Deploy dashboard to Netlify using Netlify CLI.

    Args:
        dashboard_path: Path to dashboard HTML file
        site_id: Netlify site ID

    Returns:
        True if successful, False otherwise
    """
    print("\n" + "=" * 80)
    print("Netlify Dashboard Deployment")
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
    temp_dir = Path("temp_netlify_deploy")
    temp_dir.mkdir(exist_ok=True)

    # Copy dashboard to index.html in temp directory
    import shutil
    target_file = temp_dir / "index.html"
    print(f"\n[INFO] Copying dashboard to {target_file}...")
    shutil.copy2(dashboard_path, target_file)

    # Create netlify.toml to skip build
    netlify_toml = temp_dir / "netlify.toml"
    with open(netlify_toml, 'w', encoding='utf-8') as f:
        f.write('[build]\n')
        f.write('  command = "echo Deploying pre-built dashboard"\n')
        f.write('  publish = "."\n')
    print(f"[INFO] Created netlify.toml to skip build process")

    # Deploy to Netlify
    print(f"\n[INFO] Deploying to Netlify (site: {site_id})...")
    print("-" * 80)

    try:
        # Use netlify.cmd on Windows
        import platform
        netlify_cmd = "netlify.cmd" if platform.system() == "Windows" else "netlify"

        cmd = [
            netlify_cmd, "deploy",
            "--prod",
            "--dir", str(temp_dir),
            "--site", site_id,
            "--message", f"Auto-update dashboard {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ]

        print(f"[DEBUG] Command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',  # Replace Unicode errors
            check=False
        )

        # Print stdout with error handling
        if result.stdout:
            try:
                print(result.stdout)
            except UnicodeEncodeError:
                # Fallback: print without special characters
                print(result.stdout.encode('ascii', 'ignore').decode('ascii'))

        if result.returncode == 0:
            print("\n" + "=" * 80)
            print("[SUCCESS] Netlify deployment completed!")
            print(f"終了時刻: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
            print("=" * 80)
            print("\nDeployed URL: https://box-dashboard-report.netlify.app/")
            return True
        else:
            print(f"\n[ERROR] Netlify deployment failed:")
            # Print stderr with error handling
            if result.stderr:
                try:
                    print(result.stderr)
                except UnicodeEncodeError:
                    print(result.stderr.encode('ascii', 'ignore').decode('ascii'))
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

    # Netlify site ID
    site_id = os.getenv("NETLIFY_SITE_ID", "47255fce-725c-48f1-a865-db146b183555")

    # Deploy to Netlify
    success = deploy_to_netlify(dashboard_path, site_id)

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
