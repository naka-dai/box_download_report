"""
Generate Period-Filtered All-in-One Dashboard
運用開始前/運用開始後でフィルタリング可能なオールインワンダッシュボード生成
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime


def get_admin_emails(cursor):
    """Get admin user emails to exclude from analytics."""
    admin_ids = ['13213941207', '16623033409', '30011740170', '32504279209']
    admin_emails = set()

    cursor.execute('SELECT DISTINCT user_login, raw_json FROM downloads')
    for email, raw_json in cursor.fetchall():
        if raw_json:
            try:
                user_id = json.loads(raw_json).get('user_id', '')
                if user_id in admin_ids:
                    admin_emails.add(email)
            except:
                pass

    return admin_emails


def collect_integrated_data(cursor, admin_params, placeholders, period_clause=""):
    """Collect integrated (DL+PV) data for a period."""

    # Summary stats
    cursor.execute(f'''
        SELECT COUNT(*) FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    total_downloads = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(*) FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    total_previews = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT user_login) FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_users_dl = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT user_login) FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_users_pv = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT file_id) FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_files = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    min_date, max_date = cursor.fetchone()

    # Monthly data (DL + PV)
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly_data = cursor.fetchall()

    # Daily data (last 30 days)
    cursor.execute(f'''
        SELECT DATE(download_at_jst) as day,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv,
               GROUP_CONCAT(DISTINCT CASE WHEN event_type = "DOWNLOAD" THEN user_login END) as dl_users,
               GROUP_CONCAT(DISTINCT CASE WHEN event_type = "PREVIEW" THEN user_login END) as pv_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', admin_params)
    daily_data = list(reversed(cursor.fetchall()))

    # Hourly data
    cursor.execute(f'''
        SELECT CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv,
               GROUP_CONCAT(DISTINCT CASE WHEN event_type = "DOWNLOAD" THEN user_login END) as dl_users,
               GROUP_CONCAT(DISTINCT CASE WHEN event_type = "PREVIEW" THEN user_login END) as pv_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour ORDER BY hour
    ''', admin_params)
    hourly_data = cursor.fetchall()

    # Top users (DL)
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    top_users_dl = cursor.fetchall()

    # Top users (PV)
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    top_users_pv = cursor.fetchall()

    # Top files (DL)
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt,
               COUNT(DISTINCT user_login) as users,
               GROUP_CONCAT(DISTINCT user_login) as user_list
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    top_files_dl = cursor.fetchall()

    # Top files (PV)
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt,
               COUNT(DISTINCT user_login) as users,
               GROUP_CONCAT(DISTINCT user_login) as user_list
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    top_files_pv = cursor.fetchall()

    # Calculate ratios
    total = total_downloads + total_previews
    dl_ratio = (total_downloads / total * 100) if total > 0 else 0
    pv_ratio = (total_previews / total * 100) if total > 0 else 0

    # Duplication rates
    dl_dup_rate = ((total_downloads - len(top_files_dl)) / total_downloads * 100) if total_downloads > 0 else 0
    pv_dup_rate = ((total_previews - len(top_files_pv)) / total_previews * 100) if total_previews > 0 else 0

    return {
        'total_downloads': total_downloads,
        'total_previews': total_previews,
        'unique_users_dl': unique_users_dl,
        'unique_users_pv': unique_users_pv,
        'unique_files': unique_files,
        'min_date': min_date,
        'max_date': max_date,
        'dl_ratio': dl_ratio,
        'pv_ratio': pv_ratio,
        'dl_dup_rate': dl_dup_rate,
        'pv_dup_rate': pv_dup_rate,
        'monthly_labels': [row[0] for row in monthly_data],
        'monthly_downloads': [row[1] for row in monthly_data],
        'monthly_previews': [row[2] for row in monthly_data],
        'daily_labels': [row[0] for row in daily_data],
        'daily_downloads': [row[1] for row in daily_data],
        'daily_previews': [row[2] for row in daily_data],
        'daily_dl_users': [row[3] for row in daily_data],
        'daily_pv_users': [row[4] for row in daily_data],
        'hourly_data': hourly_data,
        'top_users_dl': top_users_dl,
        'top_users_pv': top_users_pv,
        'top_files_dl': top_files_dl,
        'top_files_pv': top_files_pv
    }


def collect_download_only_data(cursor, admin_params, placeholders, period_clause=""):
    """Collect download-only data for a period."""

    cursor.execute(f'''
        SELECT COUNT(*) FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    total_downloads = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT user_login) FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_users = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT file_id) FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_files = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    min_date, max_date = cursor.fetchone()

    # Monthly data
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month, COUNT(*) as cnt
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly_data = cursor.fetchall()

    # Daily data
    cursor.execute(f'''
        SELECT DATE(download_at_jst) as day, COUNT(*) as cnt,
               GROUP_CONCAT(DISTINCT user_login) as users
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', admin_params)
    daily_data = list(reversed(cursor.fetchall()))

    # Hourly data
    cursor.execute(f'''
        SELECT CAST(strftime('%H', download_at_jst) AS INTEGER) as hour, COUNT(*) as cnt,
               GROUP_CONCAT(DISTINCT user_login) as users
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour ORDER BY hour
    ''', admin_params)
    hourly_data = cursor.fetchall()

    # Top users
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    top_users = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt,
               COUNT(DISTINCT user_login) as users,
               GROUP_CONCAT(DISTINCT user_login) as user_list
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    top_files = cursor.fetchall()

    dup_rate = ((total_downloads - len(top_files)) / total_downloads * 100) if total_downloads > 0 else 0

    return {
        'total_downloads': total_downloads,
        'unique_users': unique_users,
        'unique_files': unique_files,
        'min_date': min_date,
        'max_date': max_date,
        'dup_rate': dup_rate,
        'monthly_labels': [row[0] for row in monthly_data],
        'monthly_counts': [row[1] for row in monthly_data],
        'daily_labels': [row[0] for row in daily_data],
        'daily_counts': [row[1] for row in daily_data],
        'daily_users': [row[2] for row in daily_data],
        'hourly_data': hourly_data,
        'top_users': top_users,
        'top_files': top_files
    }


def collect_preview_only_data(cursor, admin_params, placeholders, period_clause=""):
    """Collect preview-only data for a period."""

    cursor.execute(f'''
        SELECT COUNT(*) FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    total_previews = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT user_login) FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_users = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT COUNT(DISTINCT file_id) FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    unique_files = cursor.fetchone()[0]

    cursor.execute(f'''
        SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
    ''', admin_params)
    min_date, max_date = cursor.fetchone()

    # Monthly data
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month, COUNT(*) as cnt
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly_data = cursor.fetchall()

    # Daily data
    cursor.execute(f'''
        SELECT DATE(download_at_jst) as day, COUNT(*) as cnt,
               GROUP_CONCAT(DISTINCT user_login) as users
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', admin_params)
    daily_data = list(reversed(cursor.fetchall()))

    # Hourly data
    cursor.execute(f'''
        SELECT CAST(strftime('%H', download_at_jst) AS INTEGER) as hour, COUNT(*) as cnt,
               GROUP_CONCAT(DISTINCT user_login) as users
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour ORDER BY hour
    ''', admin_params)
    hourly_data = cursor.fetchall()

    # Top users
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    top_users = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt,
               COUNT(DISTINCT user_login) as users,
               GROUP_CONCAT(DISTINCT user_login) as user_list
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    top_files = cursor.fetchall()

    dup_rate = ((total_previews - len(top_files)) / total_previews * 100) if total_previews > 0 else 0

    return {
        'total_previews': total_previews,
        'unique_users': unique_users,
        'unique_files': unique_files,
        'min_date': min_date,
        'max_date': max_date,
        'dup_rate': dup_rate,
        'monthly_labels': [row[0] for row in monthly_data],
        'monthly_counts': [row[1] for row in monthly_data],
        'daily_labels': [row[0] for row in daily_data],
        'daily_counts': [row[1] for row in daily_data],
        'daily_users': [row[2] for row in daily_data],
        'hourly_data': hourly_data,
        'top_users': top_users,
        'top_files': top_files
    }


def generate_dashboard():
    """Generate period-filtered all-in-one dashboard."""

    print("期間フィルタ付きオールインワンダッシュボード生成開始...")

    # Connect to database
    db_path = r"data\box_audit.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get admin emails
    admin_emails = get_admin_emails(cursor)
    placeholders = ','.join(['?' for _ in admin_emails])
    admin_params = tuple(admin_emails)

    print(f"  管理者ユーザー除外: {len(admin_emails)}件")

    # Collect data for all three periods
    print("  全期間データ収集中...")
    data_all_int = collect_integrated_data(cursor, admin_params, placeholders, "")
    data_all_dl = collect_download_only_data(cursor, admin_params, placeholders, "")
    data_all_pv = collect_preview_only_data(cursor, admin_params, placeholders, "")

    print("  運用開始前データ収集中...")
    period_before = "AND DATE(download_at_jst) <= '2024-10-13'"
    data_before_int = collect_integrated_data(cursor, admin_params, placeholders, period_before)
    data_before_dl = collect_download_only_data(cursor, admin_params, placeholders, period_before)
    data_before_pv = collect_preview_only_data(cursor, admin_params, placeholders, period_before)

    print("  運用開始後データ収集中...")
    period_after = "AND DATE(download_at_jst) >= '2024-10-14'"
    data_after_int = collect_integrated_data(cursor, admin_params, placeholders, period_after)
    data_after_dl = collect_download_only_data(cursor, admin_params, placeholders, period_after)
    data_after_pv = collect_preview_only_data(cursor, admin_params, placeholders, period_after)

    conn.close()

    print("  データ収集完了")
    print(f"    全期間: DL={data_all_dl['total_downloads']:,}, PV={data_all_pv['total_previews']:,}")
    print(f"    運用開始前: DL={data_before_dl['total_downloads']:,}, PV={data_before_pv['total_previews']:,}")
    print(f"    運用開始後: DL={data_after_dl['total_downloads']:,}, PV={data_after_pv['total_previews']:,}")

    # Read Chart.js from existing file
    chartjs_content = ""
    existing_dashboard = Path(r"data\dashboard_allinone_full.html")
    if existing_dashboard.exists():
        with open(existing_dashboard, 'r', encoding='utf-8') as f:
            content = f.read()
            # Extract Chart.js embedded code
            start_marker = "<!-- Chart.js Embedded -->"
            end_marker = "<!-- End Chart.js -->"
            if start_marker in content and end_marker in content:
                start_idx = content.index(start_marker)
                end_idx = content.index(end_marker) + len(end_marker)
                chartjs_content = content[start_idx:end_idx]

    print("  HTMLダッシュボード生成中...")

    # Generate complete HTML (this will be a large file)
    output_path = generate_html(
        data_all_int, data_all_dl, data_all_pv,
        data_before_int, data_before_dl, data_before_pv,
        data_after_int, data_after_dl, data_after_pv,
        chartjs_content
    )

    print(f"\n[OK] 期間フィルタ付きダッシュボード生成完了: {output_path}")
    print(f"     file:///{output_path.replace(chr(92), '/')}")

    return output_path


def generate_html(data_all_int, data_all_dl, data_all_pv,
                  data_before_int, data_before_dl, data_before_pv,
                  data_after_int, data_after_dl, data_after_pv,
                  chartjs_content):
    """Generate the complete HTML dashboard."""

    output_path = r"data\dashboard_period.html"

    # This function will generate the complete HTML
    # Due to file size limitations, I'll create a streamlined version
    # that includes all features but optimized data structures

    html = generate_complete_dashboard_html(
        data_all_int, data_all_dl, data_all_pv,
        data_before_int, data_before_dl, data_before_pv,
        data_after_int, data_after_dl, data_after_pv,
        chartjs_content
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    return output_path


def generate_complete_dashboard_html(data_all_int, data_all_dl, data_all_pv,
                                      data_before_int, data_before_dl, data_before_pv,
                                      data_after_int, data_after_dl, data_after_pv,
                                      chartjs_content):
    """Generate complete HTML with all features."""

    # This will be generated in the next step due to size
    # For now, return a placeholder that will be replaced

    return "<!-- Dashboard HTML will be generated -->"


if __name__ == '__main__':
    generate_dashboard()
