"""
Generate Period-Filtered All-in-One Dashboard (Full Rich UI Version)
期間フィルタ付きオールインワンダッシュボード生成（完全リッチUI版）

リッチUI機能:
- 月別グラフクリックで詳細モーダル表示
- 日別/時間帯別グラフにユーザー内訳ツールチップ
- トップユーザーテーブルにトグル機能（トップ10/全員）
- トップファイルにユーザー詳細ツールチップ
- 重複率の表示
- DL/PVバッジ表示

期間フィルタ:
- 全期間
- 運用開始前（～2025-10-13）
- 運用開始後（2025-10-14～）

タブ切り替え:
- 統合レポート（DL+PV）
- ダウンロードのみ集計
- プレビューのみ集計
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path


def collect_all_data(cursor, admin_params, placeholders, period_clause, period_key):
    """Collect all data (integrated, download, preview) for a specific period."""

    data = {'period_key': period_key}

    # Basic stats
    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['total_downloads'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['total_previews'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_users_download'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_users_preview'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT file_id) FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_files'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    min_date, max_date = cursor.fetchone()
    data['min_date'] = min_date or 'N/A'
    data['max_date'] = max_date or 'N/A'

    # === INTEGRATED DATA ===
    # Monthly statistics with user breakdown
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_data_raw = cursor.fetchall()

    # Process monthly data to get detailed user breakdown
    monthly_data_with_users = []
    for month, dl_count, pv_count, unique_users_count in monthly_data_raw:
        # Get detailed breakdown for this month
        cursor.execute(f'''
            SELECT
                user_name,
                SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl_count,
                SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv_count,
                COUNT(*) as total
            FROM downloads
            WHERE strftime('%Y-%m', download_at_jst) = ? AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY total DESC
        ''', (month,) + admin_params)
        user_breakdown = cursor.fetchall()
        monthly_data_with_users.append((month, dl_count, pv_count, unique_users_count, user_breakdown))

    data['monthly_integrated'] = monthly_data_with_users

    # Daily statistics (last 30 days) with user breakdown
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_data_raw = list(reversed(cursor.fetchall()))

    # Process daily data to get detailed user breakdown
    daily_data_with_users = []
    for date, dl_count, pv_count, unique_users_count in daily_data_raw:
        # Get detailed breakdown for this date
        cursor.execute(f'''
            SELECT
                user_name,
                SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl_count,
                SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv_count,
                COUNT(*) as total
            FROM downloads
            WHERE DATE(download_at_jst) = ? AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY total DESC
        ''', (date,) + admin_params)
        user_breakdown = cursor.fetchall()
        daily_data_with_users.append((date, dl_count, pv_count, unique_users_count, user_breakdown))

    data['daily_integrated'] = daily_data_with_users

    # Hourly statistics with user breakdown
    hourly_data_with_users = []
    for hour, dl_count, pv_count in cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour
        ORDER BY hour
    ''', admin_params).fetchall():
        # Get user breakdown for this hour (both DL and PV)
        cursor.execute(f'''
            SELECT
                user_name,
                SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl_count,
                SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv_count,
                COUNT(*) as total
            FROM downloads
            WHERE CAST(strftime('%H', download_at_jst) AS INTEGER) = ? AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY total DESC
        ''', (hour,) + admin_params)
        user_breakdown = cursor.fetchall()
        hourly_data_with_users.append((hour, dl_count, pv_count, user_breakdown))

    data['hourly_integrated'] = hourly_data_with_users

    # Top users
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(*) as total_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login
        ORDER BY total_count DESC
    ''', admin_params)
    data['top_users_integrated'] = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT
            file_id,
            file_name,
            raw_json,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(*) as total_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id
        ORDER BY total_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_raw = cursor.fetchall()

    top_files_integrated = []
    for file_id, file_name, raw_json, dl_count, pv_count, total, unique_users_count in top_files_raw:
        folder = ''
        if raw_json:
            try:
                data_json = json.loads(raw_json)
                folder = data_json.get('parent_folder', '')
            except:
                pass

        # Get users who accessed this file
        file_clause = f'''
            SELECT DISTINCT user_name, user_login
            FROM downloads
            WHERE file_id = ?
              AND user_login NOT IN ({placeholders})
              {period_clause if period_clause else ''}
            ORDER BY user_name
        '''
        cursor.execute(file_clause, (file_id,) + admin_params)
        users = cursor.fetchall()
        user_names = [f"{name} ({email})" for name, email in users]

        top_files_integrated.append((file_name, folder, dl_count, pv_count, total, unique_users_count, user_names))

    data['top_files_integrated'] = top_files_integrated

    # === DOWNLOAD ONLY DATA ===
    # Monthly statistics with user breakdown
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            COUNT(*) as download_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_dl_raw = cursor.fetchall()

    # Process monthly data to get detailed user breakdown
    monthly_dl_with_users = []
    for month, dl_count, unique_users_count in monthly_dl_raw:
        cursor.execute(f'''
            SELECT
                user_name,
                COUNT(*) as dl_count
            FROM downloads
            WHERE strftime('%Y-%m', download_at_jst) = ? AND event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY dl_count DESC
        ''', (month,) + admin_params)
        user_breakdown = cursor.fetchall()
        monthly_dl_with_users.append((month, dl_count, unique_users_count, user_breakdown))

    data['monthly_download'] = monthly_dl_with_users

    # Daily statistics with user breakdown
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            COUNT(*) as download_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_dl_raw = list(reversed(cursor.fetchall()))

    # Process daily data to get detailed user breakdown
    daily_dl_with_users = []
    for date, dl_count, unique_users_count in daily_dl_raw:
        cursor.execute(f'''
            SELECT
                user_name,
                COUNT(*) as dl_count
            FROM downloads
            WHERE DATE(download_at_jst) = ? AND event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY dl_count DESC
        ''', (date,) + admin_params)
        user_breakdown = cursor.fetchall()
        daily_dl_with_users.append((date, dl_count, unique_users_count, user_breakdown))

    data['daily_download'] = daily_dl_with_users

    # Hourly statistics with user breakdown
    hourly_dl_with_users = []
    for hour, dl_count in cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            COUNT(*) as download_count
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour
        ORDER BY hour
    ''', admin_params).fetchall():
        cursor.execute(f'''
            SELECT
                user_name,
                COUNT(*) as dl_count
            FROM downloads
            WHERE CAST(strftime('%H', download_at_jst) AS INTEGER) = ? AND event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY dl_count DESC
        ''', (hour,) + admin_params)
        user_breakdown = cursor.fetchall()
        hourly_dl_with_users.append((hour, dl_count, user_breakdown))

    data['hourly_download'] = hourly_dl_with_users

    # Top users
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            COUNT(*) as download_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login
        ORDER BY download_count DESC
    ''', admin_params)
    data['top_users_download'] = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT
            file_id,
            file_name,
            raw_json,
            COUNT(*) as download_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id
        ORDER BY download_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_dl_raw = cursor.fetchall()

    top_files_download = []
    for file_id, file_name, raw_json, count, unique_users_count in top_files_dl_raw:
        folder = ''
        if raw_json:
            try:
                data_json = json.loads(raw_json)
                folder = data_json.get('parent_folder', '')
            except:
                pass

        # Get users who downloaded this file
        file_clause = f'''
            SELECT DISTINCT user_name, user_login
            FROM downloads
            WHERE file_id = ?
              AND event_type = "DOWNLOAD"
              AND user_login NOT IN ({placeholders})
              {period_clause if period_clause else ''}
            ORDER BY user_name
        '''
        cursor.execute(file_clause, (file_id,) + admin_params)
        users = cursor.fetchall()
        user_names = [f"{name} ({email})" for name, email in users]

        top_files_download.append((file_name, folder, count, unique_users_count, user_names))

    data['top_files_download'] = top_files_download

    # === PREVIEW ONLY DATA ===
    # Monthly statistics with user breakdown
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            COUNT(*) as preview_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_pv_raw = cursor.fetchall()

    # Process monthly data to get detailed user breakdown
    monthly_pv_with_users = []
    for month, pv_count, unique_users_count in monthly_pv_raw:
        cursor.execute(f'''
            SELECT
                user_name,
                COUNT(*) as pv_count
            FROM downloads
            WHERE strftime('%Y-%m', download_at_jst) = ? AND event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY pv_count DESC
        ''', (month,) + admin_params)
        user_breakdown = cursor.fetchall()
        monthly_pv_with_users.append((month, pv_count, unique_users_count, user_breakdown))

    data['monthly_preview'] = monthly_pv_with_users

    # Daily statistics with user breakdown
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            COUNT(*) as preview_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_pv_raw = list(reversed(cursor.fetchall()))

    # Process daily data to get detailed user breakdown
    daily_pv_with_users = []
    for date, pv_count, unique_users_count in daily_pv_raw:
        cursor.execute(f'''
            SELECT
                user_name,
                COUNT(*) as pv_count
            FROM downloads
            WHERE DATE(download_at_jst) = ? AND event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY pv_count DESC
        ''', (date,) + admin_params)
        user_breakdown = cursor.fetchall()
        daily_pv_with_users.append((date, pv_count, unique_users_count, user_breakdown))

    data['daily_preview'] = daily_pv_with_users

    # Hourly statistics with user breakdown
    hourly_pv_with_users = []
    for hour, pv_count in cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            COUNT(*) as preview_count
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour
        ORDER BY hour
    ''', admin_params).fetchall():
        cursor.execute(f'''
            SELECT
                user_name,
                COUNT(*) as pv_count
            FROM downloads
            WHERE CAST(strftime('%H', download_at_jst) AS INTEGER) = ? AND event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
            GROUP BY user_name
            ORDER BY pv_count DESC
        ''', (hour,) + admin_params)
        user_breakdown = cursor.fetchall()
        hourly_pv_with_users.append((hour, pv_count, user_breakdown))

    data['hourly_preview'] = hourly_pv_with_users

    # Top users
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            COUNT(*) as preview_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login
        ORDER BY preview_count DESC
    ''', admin_params)
    data['top_users_preview'] = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT
            file_id,
            file_name,
            raw_json,
            COUNT(*) as preview_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id
        ORDER BY preview_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_pv_raw = cursor.fetchall()

    top_files_preview = []
    for file_id, file_name, raw_json, count, unique_users_count in top_files_pv_raw:
        folder = ''
        if raw_json:
            try:
                data_json = json.loads(raw_json)
                folder = data_json.get('parent_folder', '')
            except:
                pass

        # Get users who previewed this file
        file_clause = f'''
            SELECT DISTINCT user_name, user_login
            FROM downloads
            WHERE file_id = ?
              AND event_type = "PREVIEW"
              AND user_login NOT IN ({placeholders})
              {period_clause if period_clause else ''}
            ORDER BY user_name
        '''
        cursor.execute(file_clause, (file_id,) + admin_params)
        users = cursor.fetchall()
        user_names = [f"{name} ({email})" for name, email in users]

        top_files_preview.append((file_name, folder, count, unique_users_count, user_names))

    data['top_files_preview'] = top_files_preview

    return data


def generate_period_content(period_id, period_name, stats):
    """Generate HTML content for a specific period with tabs."""

    total_downloads = stats['total_downloads']
    total_previews = stats['total_previews']
    unique_users_download = stats['unique_users_download']
    unique_users_preview = stats['unique_users_preview']
    unique_files = stats['unique_files']

    total_access = total_downloads + total_previews
    download_ratio = (total_downloads / total_access * 100) if total_access > 0 else 0
    preview_ratio = (total_previews / total_access * 100) if total_access > 0 else 0

    # Prepare chart data and build tooltip data for monthly integrated chart
    monthly_integrated_labels = [row[0] for row in stats['monthly_integrated']]
    monthly_integrated_downloads = [row[1] for row in stats['monthly_integrated']]
    monthly_integrated_previews = [row[2] for row in stats['monthly_integrated']]

    monthly_integrated_tooltips = []
    for month, dl_count, pv_count, unique_users_count, user_breakdown in stats['monthly_integrated']:
        tooltip_data = {
            'month': month,
            'dl_count': dl_count,
            'pv_count': pv_count,
            'unique_users': unique_users_count,
            'users': []
        }
        for user_name, user_dl, user_pv, user_total in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'dl': user_dl,
                'pv': user_pv,
                'total': user_total
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        monthly_integrated_tooltips.append(tooltip_data)

    # Build tooltip data for monthly download chart
    monthly_download_labels = [row[0] for row in stats['monthly_download']]
    monthly_download_values = [row[1] for row in stats['monthly_download']]

    monthly_download_tooltips = []
    for month, dl_count, unique_users_count, user_breakdown in stats['monthly_download']:
        tooltip_data = {
            'month': month,
            'dl_count': dl_count,
            'unique_users': unique_users_count,
            'users': []
        }
        for user_name, user_dl in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'dl': user_dl
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        monthly_download_tooltips.append(tooltip_data)

    # Build tooltip data for monthly preview chart
    monthly_preview_labels = [row[0] for row in stats['monthly_preview']]
    monthly_preview_values = [row[1] for row in stats['monthly_preview']]

    monthly_preview_tooltips = []
    for month, pv_count, unique_users_count, user_breakdown in stats['monthly_preview']:
        tooltip_data = {
            'month': month,
            'pv_count': pv_count,
            'unique_users': unique_users_count,
            'users': []
        }
        for user_name, user_pv in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'pv': user_pv
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        monthly_preview_tooltips.append(tooltip_data)

    # Build tooltip data for daily integrated chart
    daily_integrated_labels = [row[0] for row in stats['daily_integrated']]
    daily_integrated_downloads = [row[1] for row in stats['daily_integrated']]
    daily_integrated_previews = [row[2] for row in stats['daily_integrated']]

    daily_integrated_tooltips = []
    for date, dl_count, pv_count, unique_users_count, user_breakdown in stats['daily_integrated']:
        tooltip_data = {
            'date': date,
            'dl_count': dl_count,
            'pv_count': pv_count,
            'unique_users': unique_users_count,
            'users': []
        }
        for user_name, user_dl, user_pv, user_total in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'dl': user_dl,
                'pv': user_pv,
                'total': user_total
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        daily_integrated_tooltips.append(tooltip_data)

    # Build tooltip data for daily download chart
    daily_download_labels = [row[0] for row in stats['daily_download']]
    daily_download_values = [row[1] for row in stats['daily_download']]

    daily_download_tooltips = []
    for date, dl_count, unique_users_count, user_breakdown in stats['daily_download']:
        tooltip_data = {
            'date': date,
            'dl_count': dl_count,
            'unique_users': unique_users_count,
            'users': []
        }
        for user_name, user_dl in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'dl': user_dl
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        daily_download_tooltips.append(tooltip_data)

    # Build tooltip data for daily preview chart
    daily_preview_labels = [row[0] for row in stats['daily_preview']]
    daily_preview_values = [row[1] for row in stats['daily_preview']]

    daily_preview_tooltips = []
    for date, pv_count, unique_users_count, user_breakdown in stats['daily_preview']:
        tooltip_data = {
            'date': date,
            'pv_count': pv_count,
            'unique_users': unique_users_count,
            'users': []
        }
        for user_name, user_pv in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'pv': user_pv
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        daily_preview_tooltips.append(tooltip_data)

    # Build tooltip data for hourly integrated chart
    hourly_integrated_labels = [f"{row[0]:02d}:00" for row in stats['hourly_integrated']]
    hourly_integrated_downloads = [row[1] for row in stats['hourly_integrated']]
    hourly_integrated_previews = [row[2] for row in stats['hourly_integrated']]

    hourly_integrated_tooltips = []
    for hour, dl_count, pv_count, user_breakdown in stats['hourly_integrated']:
        tooltip_data = {
            'hour': f"{hour:02d}:00",
            'dl_count': dl_count,
            'pv_count': pv_count,
            'users': []
        }
        for user_name, user_dl, user_pv, user_total in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'dl': user_dl,
                'pv': user_pv,
                'total': user_total
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        hourly_integrated_tooltips.append(tooltip_data)

    # Build tooltip data for hourly download chart
    hourly_download_labels = [f"{row[0]:02d}:00" for row in stats['hourly_download']]
    hourly_download_values = [row[1] for row in stats['hourly_download']]

    hourly_download_tooltips = []
    for hour, dl_count, user_breakdown in stats['hourly_download']:
        tooltip_data = {
            'hour': f"{hour:02d}:00",
            'dl_count': dl_count,
            'users': []
        }
        for user_name, user_dl in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'dl': user_dl
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        hourly_download_tooltips.append(tooltip_data)

    # Build tooltip data for hourly preview chart
    hourly_preview_labels = [f"{row[0]:02d}:00" for row in stats['hourly_preview']]
    hourly_preview_values = [row[1] for row in stats['hourly_preview']]

    hourly_preview_tooltips = []
    for hour, pv_count, user_breakdown in stats['hourly_preview']:
        tooltip_data = {
            'hour': f"{hour:02d}:00",
            'pv_count': pv_count,
            'users': []
        }
        for user_name, user_pv in user_breakdown[:5]:  # Top 5 users
            tooltip_data['users'].append({
                'name': user_name,
                'pv': user_pv
            })
        if len(user_breakdown) > 5:
            tooltip_data['more'] = len(user_breakdown) - 5
        hourly_preview_tooltips.append(tooltip_data)

    # Set initial display style - show 'all' period by default
    display_style = "display: block;" if period_id == 'all' else "display: none;"

    html = f'''
        <!-- Period: {period_name} -->
        <div id="period-{period_id}" class="period-content" style="{display_style}">

            <!-- Integrated Tab for {period_name} -->
            <div id="{period_id}-integrated-tab" class="tab-content active">
                <div class="stats-grid">
                    <div class="stat-card download">
                        <h3>総ダウンロード数</h3>
                        <div class="value">{total_downloads:,}</div>
                    </div>
                    <div class="stat-card preview">
                        <h3>総プレビュー数</h3>
                        <div class="value">{total_previews:,}</div>
                    </div>
                    <div class="stat-card">
                        <h3>総アクセス数</h3>
                        <div class="value">{total_access:,}</div>
                    </div>
                    <div class="stat-card download">
                        <h3>DLユニーク数</h3>
                        <div class="value">{unique_users_download}</div>
                    </div>
                    <div class="stat-card preview">
                        <h3>PVユニーク数</h3>
                        <div class="value">{unique_users_preview}</div>
                    </div>
                    <div class="stat-card">
                        <h3>ファイル数</h3>
                        <div class="value">{unique_files:,}</div>
                    </div>
                    <div class="stat-card">
                        <h3>DL比率 / PV比率</h3>
                        <div class="value" style="font-size: 1.3em;">{download_ratio:.0f}% / {preview_ratio:.0f}%</div>
                    </div>
                </div>

                <div class="chart-grid">
                    <div class="chart-card">
                        <h2>📈 月別推移</h2>
                        <div class="chart-container">
                            <canvas id="{period_id}-monthlyIntegratedChart"></canvas>
                        </div>
                    </div>

                    <div class="chart-card">
                        <h2>📅 日別推移（直近30日）</h2>
                        <div class="chart-container">
                            <canvas id="{period_id}-dailyIntegratedChart"></canvas>
                        </div>
                    </div>
                </div>

                <div class="chart-card" style="margin-bottom: 30px;">
                    <h2>🕐 時間帯別アクセス数</h2>
                    <div class="chart-container" style="height: 250px;">
                        <canvas id="{period_id}-hourlyIntegratedChart"></canvas>
                    </div>
                </div>

                <div class="table-card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h2>👥 トップユーザー（総アクセス数）</h2>
                        <div class="toggle-buttons">
                            <button class="toggle-btn active" onclick="showTopUsersIntegrated_{period_id}(10)">トップ10</button>
                            <button class="toggle-btn" onclick="showTopUsersIntegrated_{period_id}({len(stats['top_users_integrated'])})">すべて ({len(stats['top_users_integrated'])}人)</button>
                        </div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 50px;">順位</th>
                                <th>ユーザー名</th>
                                <th>メールアドレス</th>
                                <th style="text-align: right;">ダウンロード</th>
                                <th style="text-align: right;">プレビュー</th>
                                <th style="text-align: right;">合計</th>
                                <th style="text-align: right;">ファイル数</th>
                                <th style="text-align: right;">重複率</th>
                            </tr>
                        </thead>
                        <tbody id="topUsersIntegratedTable_{period_id}">
'''

    for i, (name, email, dl_count, pv_count, total, files) in enumerate(stats['top_users_integrated'], 1):
        duplication_rate = ((total - files) / total * 100) if total > 0 else 0
        show_class = 'show' if i <= 10 else ''

        html += f'''                            <tr class="user-row {show_class}" data-rank="{i}">
                                <td><span class="rank">{i}</span></td>
                                <td>{name}</td>
                                <td>{email}</td>
                                <td style="text-align: right;"><span class="badge download">{dl_count:,}</span></td>
                                <td style="text-align: right;"><span class="badge preview">{pv_count:,}</span></td>
                                <td style="text-align: right; font-weight: bold;">{total:,}</td>
                                <td style="text-align: right;">{files:,}</td>
                                <td style="text-align: right; color: {"#e74c3c" if duplication_rate > 30 else "#27ae60"};">{duplication_rate:.1f}%</td>
                            </tr>
'''

    html += '''                        </tbody>
                    </table>
                </div>

                <div class="table-card">
                    <h2>📁 トップ10ファイル（総アクセス数）</h2>
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 50px;">順位</th>
                                <th>ファイル名</th>
                                <th>フォルダ</th>
                                <th style="text-align: right;">ダウンロード</th>
                                <th style="text-align: right;">プレビュー</th>
                                <th style="text-align: right;">合計</th>
                                <th style="text-align: right;">ユーザー数</th>
                            </tr>
                        </thead>
                        <tbody>
'''

    for i, (file_name, folder, dl_count, pv_count, total, users, user_names) in enumerate(stats['top_files_integrated'], 1):
        users_json = json.dumps(user_names, ensure_ascii=False)
        html += f'''                            <tr>
                                <td><span class="rank">{i}</span></td>
                                <td>{file_name}</td>
                                <td style="font-size: 0.9em; color: #666;">{folder}</td>
                                <td style="text-align: right;"><span class="badge download">{dl_count:,}</span></td>
                                <td style="text-align: right;"><span class="badge preview">{pv_count:,}</span></td>
                                <td style="text-align: right; font-weight: bold;">{total:,}</td>
                                <td style="text-align: right;">
                                    <span class="user-count" data-users='{users_json}'>{users}</span>
                                </td>
                            </tr>
'''

    html += '''                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Download Tab -->
            <div id="''' + period_id + '''-download-tab" class="tab-content">
                <div class="stats-grid">
                    <div class="stat-card download">
                        <h3>総ダウンロード数</h3>
                        <div class="value">''' + f"{total_downloads:,}" + '''</div>
                    </div>
                    <div class="stat-card download">
                        <h3>ユニークユーザー</h3>
                        <div class="value">''' + f"{unique_users_download}" + '''</div>
                    </div>
                    <div class="stat-card">
                        <h3>ファイル数</h3>
                        <div class="value">''' + f"{unique_files:,}" + '''</div>
                    </div>
                </div>

                <div class="chart-grid">
                    <div class="chart-card">
                        <h2>📈 月別ダウンロード推移</h2>
                        <div class="chart-container">
                            <canvas id="''' + period_id + '''-monthlyDownloadChart"></canvas>
                        </div>
                    </div>

                    <div class="chart-card">
                        <h2>📅 日別ダウンロード推移（直近30日）</h2>
                        <div class="chart-container">
                            <canvas id="''' + period_id + '''-dailyDownloadChart"></canvas>
                        </div>
                    </div>
                </div>

                <div class="chart-card" style="margin-bottom: 30px;">
                    <h2>🕐 時間帯別ダウンロード数</h2>
                    <div class="chart-container" style="height: 250px;">
                        <canvas id="''' + period_id + '''-hourlyDownloadChart"></canvas>
                    </div>
                </div>

                <div class="table-card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h2>👥 トップユーザー</h2>
                        <div class="toggle-buttons">
                            <button class="toggle-btn active" onclick="showTopUsersDownload_''' + period_id + '''(10)">トップ10</button>
                            <button class="toggle-btn" onclick="showTopUsersDownload_''' + period_id + f'''({len(stats['top_users_download'])})">すべて ({len(stats['top_users_download'])}人)</button>
                        </div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 50px;">順位</th>
                                <th>ユーザー名</th>
                                <th>メールアドレス</th>
                                <th style="text-align: right;">ダウンロード数</th>
                                <th style="text-align: right;">ユニークファイル</th>
                                <th style="text-align: right;">重複率</th>
                            </tr>
                        </thead>
                        <tbody id="topUsersDownloadTable_''' + period_id + '''">
'''

    for i, (name, email, count, files) in enumerate(stats['top_users_download'], 1):
        duplication_rate = ((count - files) / count * 100) if count > 0 else 0
        show_class = 'show' if i <= 10 else ''

        html += f'''                            <tr class="user-row {show_class}" data-rank="{i}">
                                <td><span class="rank">{i}</span></td>
                                <td>{name}</td>
                                <td>{email}</td>
                                <td style="text-align: right; font-weight: bold;">{count:,}</td>
                                <td style="text-align: right;">{files:,}</td>
                                <td style="text-align: right; color: {"#e74c3c" if duplication_rate > 30 else "#27ae60"};">{duplication_rate:.1f}%</td>
                            </tr>
'''

    html += '''                        </tbody>
                    </table>
                </div>

                <div class="table-card">
                    <h2>📁 トップ10ファイル</h2>
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 50px;">順位</th>
                                <th>ファイル名</th>
                                <th>フォルダ</th>
                                <th style="text-align: right;">ダウンロード数</th>
                                <th style="text-align: right;">ユーザー数</th>
                            </tr>
                        </thead>
                        <tbody>
'''

    for i, (file_name, folder, count, users, user_names) in enumerate(stats['top_files_download'], 1):
        users_json = json.dumps(user_names, ensure_ascii=False)
        html += f'''                            <tr>
                                <td><span class="rank">{i}</span></td>
                                <td>{file_name}</td>
                                <td style="font-size: 0.9em; color: #666;">{folder}</td>
                                <td style="text-align: right; font-weight: bold;">{count:,}</td>
                                <td style="text-align: right;">
                                    <span class="user-count" data-users='{users_json}'>{users}</span>
                                </td>
                            </tr>
'''

    html += '''                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Preview Tab -->
            <div id="''' + period_id + '''-preview-tab" class="tab-content">
                <div class="stats-grid">
                    <div class="stat-card preview">
                        <h3>総プレビュー数</h3>
                        <div class="value">''' + f"{total_previews:,}" + '''</div>
                    </div>
                    <div class="stat-card preview">
                        <h3>ユニークユーザー</h3>
                        <div class="value">''' + f"{unique_users_preview}" + '''</div>
                    </div>
                    <div class="stat-card">
                        <h3>プレビューファイル数</h3>
                        <div class="value">''' + f"{unique_files:,}" + '''</div>
                    </div>
                </div>

                <div class="chart-grid">
                    <div class="chart-card">
                        <h2>📈 月別プレビュー推移</h2>
                        <div class="chart-container">
                            <canvas id="''' + period_id + '''-monthlyPreviewChart"></canvas>
                        </div>
                    </div>

                    <div class="chart-card">
                        <h2>📅 日別プレビュー推移（直近30日）</h2>
                        <div class="chart-container">
                            <canvas id="''' + period_id + '''-dailyPreviewChart"></canvas>
                        </div>
                    </div>
                </div>

                <div class="chart-card" style="margin-bottom: 30px;">
                    <h2>🕐 時間帯別プレビュー数</h2>
                    <div class="chart-container" style="height: 250px;">
                        <canvas id="''' + period_id + '''-hourlyPreviewChart"></canvas>
                    </div>
                </div>

                <div class="table-card">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                        <h2>👥 トップユーザー</h2>
                        <div class="toggle-buttons">
                            <button class="toggle-btn active" onclick="showTopUsersPreview_''' + period_id + '''(10)">トップ10</button>
                            <button class="toggle-btn" onclick="showTopUsersPreview_''' + period_id + f'''({len(stats['top_users_preview'])})">すべて ({len(stats['top_users_preview'])}人)</button>
                        </div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 50px;">順位</th>
                                <th>ユーザー名</th>
                                <th>メールアドレス</th>
                                <th style="text-align: right;">プレビュー数</th>
                                <th style="text-align: right;">ユニークファイル</th>
                                <th style="text-align: right;">重複率</th>
                            </tr>
                        </thead>
                        <tbody id="topUsersPreviewTable_''' + period_id + '''">
'''

    for i, (name, email, count, files) in enumerate(stats['top_users_preview'], 1):
        duplication_rate = ((count - files) / count * 100) if count > 0 else 0
        show_class = 'show' if i <= 10 else ''

        html += f'''                            <tr class="user-row {show_class}" data-rank="{i}">
                                <td><span class="rank">{i}</span></td>
                                <td>{name}</td>
                                <td>{email}</td>
                                <td style="text-align: right; font-weight: bold;">{count:,}</td>
                                <td style="text-align: right;">{files:,}</td>
                                <td style="text-align: right; color: {"#e74c3c" if duplication_rate > 30 else "#27ae60"};">{duplication_rate:.1f}%</td>
                            </tr>
'''

    html += '''                        </tbody>
                    </table>
                </div>

                <div class="table-card">
                    <h2>📁 トップ10ファイル</h2>
                    <table>
                        <thead>
                            <tr>
                                <th style="width: 50px;">順位</th>
                                <th>ファイル名</th>
                                <th>フォルダ</th>
                                <th style="text-align: right;">プレビュー数</th>
                                <th style="text-align: right;">ユーザー数</th>
                            </tr>
                        </thead>
                        <tbody>
'''

    for i, (file_name, folder, count, users, user_names) in enumerate(stats['top_files_preview'], 1):
        users_json = json.dumps(user_names, ensure_ascii=False)
        html += f'''                            <tr>
                                <td><span class="rank">{i}</span></td>
                                <td>{file_name}</td>
                                <td style="font-size: 0.9em; color: #666;">{folder}</td>
                                <td style="text-align: right; font-weight: bold;">{count:,}</td>
                                <td style="text-align: right;">
                                    <span class="user-count" data-users='{users_json}'>{users}</span>
                                </td>
                            </tr>
'''

    html += '''                        </tbody>
                    </table>
                </div>
            </div>
        </div>
'''

    # Generate JavaScript for charts
    js_code = f'''
        // Charts for {period_name} - Integrated
        const monthlyIntegratedTooltips_{period_id} = {json.dumps(monthly_integrated_tooltips)};

        new Chart(document.getElementById('{period_id}-monthlyIntegratedChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(monthly_integrated_labels)},
                datasets: [
                    {{
                        label: 'ダウンロード',
                        data: {json.dumps(monthly_integrated_downloads)},
                        backgroundColor: 'rgba(76, 175, 80, 0.8)',
                        borderColor: 'rgba(76, 175, 80, 1)',
                        borderWidth: 2
                    }},
                    {{
                        label: 'プレビュー',
                        data: {json.dumps(monthly_integrated_previews)},
                        backgroundColor: 'rgba(255, 152, 0, 0.8)',
                        borderColor: 'rgba(255, 152, 0, 1)',
                        borderWidth: 2
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = monthlyIntegratedTooltips_{period_id}[context[0].dataIndex];
                                return data.month;
                            }},
                            beforeBody: function(context) {{
                                const data = monthlyIntegratedTooltips_{period_id}[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 / PV: ${{data.pv_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = monthlyIntegratedTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: DL ${{user.dl}}件 / PV ${{user.pv}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{
                    x: {{ stacked: true }},
                    y: {{ stacked: true, beginAtZero: true }}
                }}
            }}
        }});

        const dailyIntegratedTooltips_{period_id} = {json.dumps(daily_integrated_tooltips)};

        new Chart(document.getElementById('{period_id}-dailyIntegratedChart').getContext('2d'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(daily_integrated_labels)},
                datasets: [
                    {{
                        label: 'ダウンロード',
                        data: {json.dumps(daily_integrated_downloads)},
                        borderColor: 'rgba(76, 175, 80, 1)',
                        backgroundColor: 'rgba(76, 175, 80, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4
                    }},
                    {{
                        label: 'プレビュー',
                        data: {json.dumps(daily_integrated_previews)},
                        borderColor: 'rgba(255, 152, 0, 1)',
                        backgroundColor: 'rgba(255, 152, 0, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = dailyIntegratedTooltips_{period_id}[context[0].dataIndex];
                                return data.date;
                            }},
                            beforeBody: function(context) {{
                                const data = dailyIntegratedTooltips_{period_id}[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 / PV: ${{data.pv_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = dailyIntegratedTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: DL ${{user.dl}}件 / PV ${{user.pv}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{
                    y: {{ beginAtZero: true }}
                }},
                interaction: {{
                    mode: 'nearest',
                    intersect: false
                }}
            }}
        }});

        const hourlyIntegratedTooltips_{period_id} = {json.dumps(hourly_integrated_tooltips)};

        new Chart(document.getElementById('{period_id}-hourlyIntegratedChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(hourly_integrated_labels)},
                datasets: [
                    {{
                        label: 'ダウンロード',
                        data: {json.dumps(hourly_integrated_downloads)},
                        backgroundColor: 'rgba(76, 175, 80, 0.8)',
                        borderColor: 'rgba(76, 175, 80, 1)',
                        borderWidth: 2
                    }},
                    {{
                        label: 'プレビュー',
                        data: {json.dumps(hourly_integrated_previews)},
                        backgroundColor: 'rgba(255, 152, 0, 0.8)',
                        borderColor: 'rgba(255, 152, 0, 1)',
                        borderWidth: 2
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = hourlyIntegratedTooltips_{period_id}[context[0].dataIndex];
                                return data.hour;
                            }},
                            beforeBody: function(context) {{
                                const data = hourlyIntegratedTooltips_{period_id}[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 / PV: ${{data.pv_count}}件`;
                            }},
                            label: function(context) {{
                                const data = hourlyIntegratedTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: DL ${{user.dl}}件 / PV ${{user.pv}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{
                    x: {{ stacked: true }},
                    y: {{ stacked: true, beginAtZero: true }}
                }}
            }}
        }});

        // Charts for {period_name} - Download
        const monthlyDownloadTooltips_{period_id} = {json.dumps(monthly_download_tooltips)};

        new Chart(document.getElementById('{period_id}-monthlyDownloadChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(monthly_download_labels)},
                datasets: [{{
                    label: 'ダウンロード数',
                    data: {json.dumps(monthly_download_values)},
                    backgroundColor: 'rgba(76, 175, 80, 0.8)',
                    borderColor: 'rgba(76, 175, 80, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = monthlyDownloadTooltips_{period_id}[context[0].dataIndex];
                                return data.month;
                            }},
                            beforeBody: function(context) {{
                                const data = monthlyDownloadTooltips_{period_id}[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = monthlyDownloadTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: DL ${{user.dl}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        const dailyDownloadTooltips_{period_id} = {json.dumps(daily_download_tooltips)};

        new Chart(document.getElementById('{period_id}-dailyDownloadChart').getContext('2d'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(daily_download_labels)},
                datasets: [{{
                    label: 'ダウンロード数',
                    data: {json.dumps(daily_download_values)},
                    borderColor: 'rgba(76, 175, 80, 1)',
                    backgroundColor: 'rgba(76, 175, 80, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = dailyDownloadTooltips_{period_id}[context[0].dataIndex];
                                return data.date;
                            }},
                            beforeBody: function(context) {{
                                const data = dailyDownloadTooltips_{period_id}[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = dailyDownloadTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: DL ${{user.dl}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }},
                interaction: {{
                    mode: 'nearest',
                    intersect: false
                }}
            }}
        }});

        const hourlyDownloadTooltips_{period_id} = {json.dumps(hourly_download_tooltips)};

        new Chart(document.getElementById('{period_id}-hourlyDownloadChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(hourly_download_labels)},
                datasets: [{{
                    label: 'ダウンロード数',
                    data: {json.dumps(hourly_download_values)},
                    backgroundColor: 'rgba(76, 175, 80, 0.8)',
                    borderColor: 'rgba(76, 175, 80, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = hourlyDownloadTooltips_{period_id}[context[0].dataIndex];
                                return data.hour;
                            }},
                            beforeBody: function(context) {{
                                const data = hourlyDownloadTooltips_{period_id}[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件`;
                            }},
                            label: function(context) {{
                                const data = hourlyDownloadTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: DL ${{user.dl}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        // Charts for {period_name} - Preview
        const monthlyPreviewTooltips_{period_id} = {json.dumps(monthly_preview_tooltips)};

        new Chart(document.getElementById('{period_id}-monthlyPreviewChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(monthly_preview_labels)},
                datasets: [{{
                    label: 'プレビュー数',
                    data: {json.dumps(monthly_preview_values)},
                    backgroundColor: 'rgba(255, 152, 0, 0.8)',
                    borderColor: 'rgba(255, 152, 0, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = monthlyPreviewTooltips_{period_id}[context[0].dataIndex];
                                return data.month;
                            }},
                            beforeBody: function(context) {{
                                const data = monthlyPreviewTooltips_{period_id}[context[0].dataIndex];
                                return `PV: ${{data.pv_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = monthlyPreviewTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: PV ${{user.pv}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        const dailyPreviewTooltips_{period_id} = {json.dumps(daily_preview_tooltips)};

        new Chart(document.getElementById('{period_id}-dailyPreviewChart').getContext('2d'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(daily_preview_labels)},
                datasets: [{{
                    label: 'プレビュー数',
                    data: {json.dumps(daily_preview_values)},
                    borderColor: 'rgba(255, 152, 0, 1)',
                    backgroundColor: 'rgba(255, 152, 0, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = dailyPreviewTooltips_{period_id}[context[0].dataIndex];
                                return data.date;
                            }},
                            beforeBody: function(context) {{
                                const data = dailyPreviewTooltips_{period_id}[context[0].dataIndex];
                                return `PV: ${{data.pv_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = dailyPreviewTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: PV ${{user.pv}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }},
                interaction: {{
                    mode: 'nearest',
                    intersect: false
                }}
            }}
        }});

        const hourlyPreviewTooltips_{period_id} = {json.dumps(hourly_preview_tooltips)};

        new Chart(document.getElementById('{period_id}-hourlyPreviewChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(hourly_preview_labels)},
                datasets: [{{
                    label: 'プレビュー数',
                    data: {json.dumps(hourly_preview_values)},
                    backgroundColor: 'rgba(255, 152, 0, 0.8)',
                    borderColor: 'rgba(255, 152, 0, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = hourlyPreviewTooltips_{period_id}[context[0].dataIndex];
                                return data.hour;
                            }},
                            beforeBody: function(context) {{
                                const data = hourlyPreviewTooltips_{period_id}[context[0].dataIndex];
                                return `PV: ${{data.pv_count}}件`;
                            }},
                            label: function(context) {{
                                const data = hourlyPreviewTooltips_{period_id}[context.dataIndex];
                                const labels = [];

                                if (data.users && data.users.length > 0) {{
                                    labels.push(''); // Empty line
                                    data.users.forEach(user => {{
                                        labels.push(`${{user.name}}: PV ${{user.pv}}件`);
                                    }});

                                    if (data.more) {{
                                        labels.push(`...他${{data.more}}人`);
                                    }}
                                }}

                                return labels;
                            }}
                        }},
                        bodyFont: {{
                            size: 12
                        }},
                        padding: 12,
                        displayColors: false,
                        backgroundColor: 'rgba(0, 0, 0, 0.9)',
                        borderColor: 'rgba(102, 126, 234, 0.8)',
                        borderWidth: 2
                    }}
                }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        // Toggle functions for {period_id}
        function showTopUsersIntegrated_{period_id}(limit) {{
            document.querySelectorAll('#{period_id}-integrated-tab .toggle-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');

            const rows = document.querySelectorAll('#topUsersIntegratedTable_{period_id} .user-row');
            rows.forEach(row => {{
                const rank = parseInt(row.getAttribute('data-rank'));
                if (rank <= limit) {{
                    row.classList.add('show');
                }} else {{
                    row.classList.remove('show');
                }}
            }});
        }}

        function showTopUsersDownload_{period_id}(limit) {{
            document.querySelectorAll('#{period_id}-download-tab .toggle-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');

            const rows = document.querySelectorAll('#topUsersDownloadTable_{period_id} .user-row');
            rows.forEach(row => {{
                const rank = parseInt(row.getAttribute('data-rank'));
                if (rank <= limit) {{
                    row.classList.add('show');
                }} else {{
                    row.classList.remove('show');
                }}
            }});
        }}

        function showTopUsersPreview_{period_id}(limit) {{
            document.querySelectorAll('#{period_id}-preview-tab .toggle-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');

            const rows = document.querySelectorAll('#topUsersPreviewTable_{period_id} .user-row');
            rows.forEach(row => {{
                const rank = parseInt(row.getAttribute('data-rank'));
                if (rank <= limit) {{
                    row.classList.add('show');
                }} else {{
                    row.classList.remove('show');
                }}
            }});
        }}
'''

    return html, js_code


def generate_dashboard():
    """Generate period-filtered all-in-one HTML dashboard from database statistics."""

    # Read Chart.js library for offline use
    chartjs_path = Path(__file__).parent / "chart.js"
    with open(chartjs_path, 'r', encoding='utf-8') as f:
        chartjs_code = f.read()

    # Connect to database
    db_path = r"data\box_audit.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Admin user IDs to exclude
    admin_ids = ['13213941207', '16623033409', '30011740170', '32504279209']

    # Get admin emails
    admin_emails = set()
    cursor.execute('SELECT DISTINCT user_login, raw_json FROM downloads')
    for email, raw_json in cursor.fetchall():
        if raw_json:
            try:
                data = json.loads(raw_json)
                user_id = data.get('user_id', '')
                if user_id in admin_ids:
                    admin_emails.add(email)
            except:
                pass

    placeholders = ','.join(['?' for _ in admin_emails])
    admin_params = tuple(admin_emails)

    # Get overall date range
    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    min_date, max_date = cursor.fetchone()

    print("Box レポート 図面活用状況 生成開始...")
    print(f"データ期間: {min_date} ～ {max_date}")

    # Define periods
    periods = {
        'all': ('全期間', ''),
        'before': ('運用開始前（～2025-10-13）', 'AND DATE(download_at_jst) <= "2025-10-13"'),
        'after': ('運用開始後（2025-10-14～）', 'AND DATE(download_at_jst) >= "2025-10-14"')
    }

    # Collect statistics for all periods
    period_stats = {}
    for period_id, (period_name, period_filter) in periods.items():
        print(f"\n{period_name} データ収集中...")
        stats = collect_all_data(cursor, admin_params, placeholders, period_filter, period_id)
        period_stats[period_id] = (period_name, stats)
        print(f"  DL: {stats['total_downloads']:,}, PV: {stats['total_previews']:,}")
        print(f"  ユーザー数: {len(stats['top_users_integrated'])}人")

    conn.close()

    print(f"\nHTML生成中...")

    # Generate HTML content for each period
    period_html_parts = []
    period_js_parts = []

    for period_id, (period_name, stats) in period_stats.items():
        html_part, js_part = generate_period_content(period_id, period_name, stats)
        period_html_parts.append(html_part)
        period_js_parts.append(js_part)

    # Combine all HTML parts
    all_period_content = '\n'.join(period_html_parts)
    all_period_js = '\n'.join(period_js_parts)

    # Generate main HTML structure
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Box レポート 図面活用状況</title>
    <script>
{chartjs_code}
    </script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        .header {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
            text-align: center;
        }}

        .header h1 {{
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .header p {{
            color: #666;
            font-size: 1.1em;
        }}

        /* Period Selector */
        .period-selector {{
            display: flex;
            gap: 15px;
            justify-content: center;
            margin: 25px 0 15px 0;
            flex-wrap: wrap;
        }}

        .period-btn {{
            padding: 15px 35px;
            border: 2px solid #667eea;
            background: white;
            color: #667eea;
            border-radius: 10px;
            font-size: 1.1em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
        }}

        .period-btn:hover {{
            background: #f0f0f0;
        }}

        .period-btn.active {{
            background: #667eea;
            color: white;
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.5);
            transform: translateY(-2px);
        }}

        /* Tab Navigation */
        .tab-nav {{
            display: flex;
            gap: 10px;
            justify-content: center;
            margin-top: 20px;
            flex-wrap: wrap;
        }}

        .tab-btn {{
            padding: 12px 30px;
            border: none;
            background: #e0e0e0;
            color: #666;
            border-radius: 8px;
            font-size: 1em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s;
        }}

        .tab-btn:hover {{
            background: #d0d0d0;
        }}

        .tab-btn.active {{
            background: #667eea;
            color: white;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }}

        .tab-btn.integrated {{
            border-left: 4px solid #667eea;
        }}

        .tab-btn.download {{
            border-left: 4px solid #4CAF50;
        }}

        .tab-btn.preview {{
            border-left: 4px solid #FF9800;
        }}

        /* Period & Tab Content */
        .period-content {{
            display: none;
        }}

        .period-content.active {{
            display: block;
            animation: fadeIn 0.3s;
        }}

        .tab-content {{
            display: none;
        }}

        .tab-content.active {{
            display: block;
            animation: fadeIn 0.3s;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; }}
            to {{ opacity: 1; }}
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            text-align: center;
            transition: transform 0.3s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.15);
        }}

        .stat-card h3 {{
            color: #888;
            font-size: 0.85em;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .stat-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}

        .stat-card.download .value {{
            color: #4CAF50;
        }}

        .stat-card.preview .value {{
            color: #FF9800;
        }}

        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}

        .chart-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
        }}

        .chart-card h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.3em;
        }}

        .chart-container {{
            position: relative;
            height: 300px;
        }}

        .table-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}

        .table-card h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.3em;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        th {{
            background: #f8f9fa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #667eea;
            border-bottom: 2px solid #667eea;
        }}

        td {{
            padding: 12px;
            border-bottom: 1px solid #e9ecef;
        }}

        tr:hover {{
            background: #f8f9fa;
        }}

        .rank {{
            display: inline-block;
            width: 30px;
            height: 30px;
            background: #667eea;
            color: white;
            border-radius: 50%;
            text-align: center;
            line-height: 30px;
            font-weight: bold;
        }}

        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: 600;
        }}

        .badge.download {{
            background: #4CAF50;
            color: white;
        }}

        .badge.preview {{
            background: #FF9800;
            color: white;
        }}

        .user-count {{
            cursor: help;
            color: #667eea;
            font-weight: bold;
            position: relative;
        }}

        .user-count:hover {{
            text-decoration: underline;
        }}

        .tooltip {{
            position: absolute;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 10px 15px;
            border-radius: 8px;
            font-size: 0.85em;
            line-height: 1.6;
            z-index: 1000;
            white-space: nowrap;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s;
            max-width: 400px;
            white-space: normal;
        }}

        .tooltip.show {{
            opacity: 1;
        }}

        .toggle-buttons {{
            display: flex;
            gap: 10px;
            margin-bottom: 15px;
            justify-content: flex-end;
        }}

        .toggle-btn {{
            padding: 8px 16px;
            border: 2px solid #667eea;
            background: white;
            color: #667eea;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.9em;
            font-weight: 600;
            transition: all 0.3s;
        }}

        .toggle-btn:hover {{
            background: #f0f0ff;
        }}

        .toggle-btn.active {{
            background: #667eea;
            color: white;
        }}

        .user-row {{
            display: none;
        }}

        .user-row.show {{
            display: table-row;
        }}

        .footer {{
            text-align: center;
            color: white;
            margin-top: 30px;
            padding: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Box レポート 図面活用状況</h1>
            <p>図面001フォルダ アクセス分析ダッシュボード</p>
            <p style="font-size: 0.9em; color: #999; margin-top: 10px;">
                全期間: {min_date} ～ {max_date}
            </p>

            <div class="period-selector">
                <button class="period-btn active" onclick="switchPeriod('all')">
                    📅 全期間
                </button>
                <button class="period-btn" onclick="switchPeriod('before')">
                    ⏪ 運用開始前（～2025-10-13）
                </button>
                <button class="period-btn" onclick="switchPeriod('after')">
                    ⏩ 運用開始後（2025-10-14～）
                </button>
            </div>

            <div class="tab-nav">
                <button class="tab-btn integrated active" onclick="switchTab('integrated')">
                    📊 統合レポート
                </button>
                <button class="tab-btn download" onclick="switchTab('download')">
                    📥 ダウンロードのみ集計
                </button>
                <button class="tab-btn preview" onclick="switchTab('preview')">
                    👁️ プレビューのみ集計
                </button>
            </div>
        </div>

{all_period_content}

        <div class="footer">
            <p>🤖 Generated with Claude Code</p>
            <p style="font-size: 0.9em; margin-top: 5px;">
                {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')} に生成
            </p>
            <p style="font-size: 0.85em; margin-top: 10px; opacity: 0.8;">
                期間とタブを選択して図面の活用状況を分析
            </p>
            <p style="font-size: 0.8em; margin-top: 5px; opacity: 0.7;">
                機能: 期間フィルタ / マウスオーバー詳細表示 / ユーザー別分析 / DL/PV比較
            </p>
        </div>
    </div>

    <div id="tooltip" class="tooltip"></div>

    <script>
        // Current selection state
        let currentPeriod = 'all';
        let currentTab = 'integrated';

        function switchPeriod(periodId) {{
            // Update current period
            currentPeriod = periodId;

            // Hide all period contents
            document.querySelectorAll('.period-content').forEach(content => {{
                content.classList.remove('active');
                content.style.display = 'none';
            }});

            // Deactivate all period buttons
            document.querySelectorAll('.period-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});

            // Show selected period content
            const periodContent = document.getElementById('period-' + periodId);
            if (periodContent) {{
                periodContent.classList.add('active');
                periodContent.style.display = 'block';
            }}

            // Activate selected period button
            document.querySelectorAll('.period-btn').forEach(btn => {{
                if (btn.onclick && btn.onclick.toString().includes("'" + periodId + "'")) {{
                    btn.classList.add('active');
                }}
            }});

            // Re-apply current tab selection for the new period
            switchTabContent(currentTab);
        }}

        function switchTab(tabName) {{
            // Update current tab
            currentTab = tabName;

            // Deactivate all tab buttons
            document.querySelectorAll('.tab-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});

            // Activate selected tab button
            document.querySelectorAll('.tab-btn').forEach(btn => {{
                if (btn.classList.contains(tabName)) {{
                    btn.classList.add('active');
                }}
            }});

            // Apply tab selection to current period
            switchTabContent(tabName);
        }}

        function switchTabContent(tabName) {{
            // Get the active period container
            const activePeriod = document.querySelector('.period-content.active');
            if (!activePeriod) return;

            // Hide all tabs within the active period
            activePeriod.querySelectorAll('.tab-content').forEach(tab => {{
                tab.classList.remove('active');
            }});

            // Show the selected tab within the active period
            const selectedTab = activePeriod.querySelector('#' + currentPeriod + '-' + tabName + '-tab');
            if (selectedTab) {{
                selectedTab.classList.add('active');
            }}
        }}

        // Initialize tooltips
        function initializeTooltips() {{
            const tooltip = document.getElementById('tooltip');
            document.querySelectorAll('.user-count').forEach(element => {{
                element.addEventListener('mouseenter', (e) => {{
                    const users = JSON.parse(element.getAttribute('data-users'));
                    tooltip.innerHTML = '<strong>アクセスユーザー:</strong><br>' + users.join('<br>');
                    tooltip.classList.add('show');
                }});

                element.addEventListener('mousemove', (e) => {{
                    tooltip.style.left = (e.pageX + 15) + 'px';
                    tooltip.style.top = (e.pageY + 15) + 'px';
                }});

                element.addEventListener('mouseleave', () => {{
                    tooltip.classList.remove('show');
                }});
            }});
        }}

        // Initialize: Show first period
        window.addEventListener('DOMContentLoaded', function() {{
            switchPeriod('all');
            initializeTooltips();
        }});

{all_period_js}
    </script>
</body>
</html>'''

    # Write HTML file
    output_path = r"data\dashboard_period_allinone_full.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    file_size = len(html.encode('utf-8'))

    print(f"\n{'='*80}")
    print(f"[OK] 完成: {output_path}")
    print(f"ファイルサイズ: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")

    # Print summary
    print("\n=== 各期間の統計 ===")
    for period_id, (period_name, stats) in period_stats.items():
        print(f"\n{period_name}:")
        print(f"  総ダウンロード数: {stats['total_downloads']:,}")
        print(f"  総プレビュー数: {stats['total_previews']:,}")
        print(f"  総アクセス数: {stats['total_downloads'] + stats['total_previews']:,}")
        print(f"  ユーザー数: {len(stats['top_users_integrated'])}人")

    print(f"\n{'='*80}")

    return output_path


if __name__ == '__main__':
    output_path = generate_dashboard()
    print(f"\n[SUCCESS] Dashboard successfully created!")
    print(f"Path: {output_path}")
