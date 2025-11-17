"""
Generate Integrated Dashboard v2
ダウンロードとプレビューを統合したダッシュボードを生成
改善点:
- 月別グラフクリックで詳細モーダル表示
- 日別/時間帯別グラフにユーザー内訳ツールチップ
- トップユーザーテーブルにトグル機能
- トップファイルにユーザー詳細ツールチップ
- 重複率の表示
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path

def generate_dashboard():
    """Generate integrated HTML dashboard from database statistics."""

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

    # Get summary statistics for both download and preview
    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})', admin_params)
    total_downloads = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})', admin_params)
    total_previews = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})', admin_params)
    unique_users_download = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})', admin_params)
    unique_users_preview = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT file_id) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    unique_files = cursor.fetchone()[0]

    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    min_date, max_date = cursor.fetchone()

    # Get monthly statistics for both types
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_data = cursor.fetchall()

    # Get detailed monthly breakdown for drill-down
    monthly_details = {}
    for month, _, _ in monthly_data:
        cursor.execute(f'''
            SELECT
                user_name,
                user_login,
                file_name,
                download_at_jst,
                event_type,
                raw_json
            FROM downloads
            WHERE user_login NOT IN ({placeholders})
              AND strftime('%Y-%m', download_at_jst) = ?
            ORDER BY download_at_jst DESC
        ''', admin_params + (month,))

        details = []
        for user_name, user_login, file_name, download_at, event_type, raw_json in cursor.fetchall():
            parent_folder = ''
            if raw_json:
                try:
                    data = json.loads(raw_json)
                    parent_folder = data.get('parent_folder', '')
                except:
                    pass

            details.append({
                'user_name': user_name,
                'user_login': user_login,
                'file_name': file_name,
                'parent_folder': parent_folder,
                'download_at': download_at,
                'event_type': event_type
            })

        monthly_details[month] = details

    # Get hourly statistics with user breakdown
    hourly_data_with_users = []
    for hour, dl_count, pv_count in cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
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
            WHERE CAST(strftime('%H', download_at_jst) AS INTEGER) = ? AND user_login NOT IN ({placeholders})
            GROUP BY user_name
            ORDER BY total DESC
        ''', (hour,) + admin_params)
        user_breakdown = cursor.fetchall()
        hourly_data_with_users.append((hour, dl_count, pv_count, user_breakdown))

    # Get daily statistics with user breakdown (last 30 days)
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
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
            WHERE DATE(download_at_jst) = ? AND user_login NOT IN ({placeholders})
            GROUP BY user_name
            ORDER BY total DESC
        ''', (date,) + admin_params)
        user_breakdown = cursor.fetchall()
        daily_data_with_users.append((date, dl_count, pv_count, unique_users_count, user_breakdown))

    # Get all users by total activity (to support top 10 / all switching)
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(*) as total_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY user_login
        ORDER BY total_count DESC
    ''', admin_params)
    top_users = cursor.fetchall()
    total_user_count = len(top_users)

    # Get top files by total activity with user details
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
        WHERE user_login NOT IN ({placeholders})
        GROUP BY file_id
        ORDER BY total_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_raw = cursor.fetchall()

    # Get user names for each top file
    top_files_with_users = []
    for file_id, file_name, raw_json, dl_count, pv_count, total, unique_users_count in top_files_raw:
        # Get users who accessed this file
        cursor.execute(f'''
            SELECT DISTINCT user_name, user_login
            FROM downloads
            WHERE file_id = ? AND user_login NOT IN ({placeholders})
            ORDER BY user_name
        ''', (file_id,) + admin_params)
        users = cursor.fetchall()
        user_names = [f"{name} ({email})" for name, email in users]

        folder = ''
        if raw_json:
            try:
                data = json.loads(raw_json)
                folder = data.get('parent_folder', '')
            except:
                pass

        top_files_with_users.append((file_name, folder, dl_count, pv_count, total, unique_users_count, user_names))

    conn.close()

    # Prepare data for charts
    monthly_labels = [row[0] for row in monthly_data]
    monthly_downloads = [row[1] for row in monthly_data]
    monthly_previews = [row[2] for row in monthly_data]

    # Build tooltip data for hourly chart
    hourly_labels = [f"{row[0]:02d}:00" for row in hourly_data_with_users]
    hourly_downloads = [row[1] for row in hourly_data_with_users]
    hourly_previews = [row[2] for row in hourly_data_with_users]

    hourly_tooltips = []
    for hour, dl_count, pv_count, user_breakdown in hourly_data_with_users:
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
        hourly_tooltips.append(tooltip_data)

    # Build tooltip data for daily chart
    daily_labels = [row[0] for row in daily_data_with_users]
    daily_downloads = [row[1] for row in daily_data_with_users]
    daily_previews = [row[2] for row in daily_data_with_users]

    daily_tooltips = []
    for date, dl_count, pv_count, unique_users_count, user_breakdown in daily_data_with_users:
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
        daily_tooltips.append(tooltip_data)

    # Calculate download/preview ratio
    download_ratio = (total_downloads / (total_downloads + total_previews) * 100) if (total_downloads + total_previews) > 0 else 0
    preview_ratio = (total_previews / (total_downloads + total_previews) * 100) if (total_downloads + total_previews) > 0 else 0

    # Generate HTML
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Box 統合レポート ダッシュボード</title>
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
            font-size: 0.9em;
            font-weight: normal;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .stat-card .value {{
            font-size: 2.5em;
            font-weight: bold;
            color: #667eea;
        }}

        .stat-card.download {{
            border-left: 4px solid #4CAF50;
        }}

        .stat-card.download .value {{
            color: #4CAF50;
        }}

        .stat-card.preview {{
            border-left: 4px solid #FF9800;
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

        .chart-card canvas {{
            cursor: pointer;
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

        .footer {{
            text-align: center;
            color: white;
            margin-top: 30px;
            padding: 20px;
        }}

        .legend {{
            display: flex;
            justify-content: center;
            gap: 20px;
            margin-top: 15px;
            font-size: 0.9em;
        }}

        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .legend-color {{
            width: 20px;
            height: 20px;
            border-radius: 4px;
        }}

        .legend-color.download {{
            background: #4CAF50;
        }}

        .legend-color.preview {{
            background: #FF9800;
        }}

        /* Modal styles */
        .modal {{
            display: none;
            position: fixed;
            z-index: 2000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.7);
            overflow: auto;
        }}

        .modal-content {{
            background-color: white;
            margin: 50px auto;
            padding: 0;
            border-radius: 15px;
            width: 90%;
            max-width: 1200px;
            max-height: 85vh;
            box-shadow: 0 10px 50px rgba(0,0,0,0.3);
            display: flex;
            flex-direction: column;
        }}

        .modal-header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px 30px;
            border-radius: 15px 15px 0 0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .modal-header h2 {{
            margin: 0;
            font-size: 1.8em;
        }}

        .close-btn {{
            color: white;
            font-size: 32px;
            font-weight: bold;
            cursor: pointer;
            background: none;
            border: none;
            padding: 0;
            width: 40px;
            height: 40px;
            line-height: 32px;
            text-align: center;
            border-radius: 50%;
            transition: background 0.3s;
        }}

        .close-btn:hover {{
            background: rgba(255, 255, 255, 0.2);
        }}

        .modal-body {{
            padding: 30px;
            overflow-y: auto;
            flex: 1;
        }}

        .detail-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}

        .detail-table th {{
            background: #f8f9fa;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            color: #667eea;
            border-bottom: 2px solid #667eea;
            position: sticky;
            top: 0;
            z-index: 10;
        }}

        .detail-table td {{
            padding: 12px;
            border-bottom: 1px solid #e9ecef;
        }}

        .detail-table tr:hover {{
            background: #f8f9fa;
        }}

        /* Toggle buttons */
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Box 統合レポート</h1>
            <p>図面001フォルダ ダウンロード・プレビュー統合分析ダッシュボード</p>
            <p style="font-size: 0.9em; color: #999; margin-top: 10px;">
                期間: {min_date} ～ {max_date}
            </p>
            <div style="margin-top: 15px; display: flex; gap: 10px; justify-content: center;">
                <a href="dashboard.html" style="padding: 8px 20px; background: #4CAF50; color: white; text-decoration: none; border-radius: 5px; font-size: 0.9em; transition: background 0.3s;">📥 ダウンロードのみ集計</a>
                <a href="dashboard_preview.html" style="padding: 8px 20px; background: #FF9800; color: white; text-decoration: none; border-radius: 5px; font-size: 0.9em; transition: background 0.3s;">👁️ プレビューのみ集計</a>
            </div>
        </div>

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
                <div class="value">{total_downloads + total_previews:,}</div>
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
                <div class="value" style="font-size: 1.5em;">{download_ratio:.0f}% / {preview_ratio:.0f}%</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-card">
                <h2>📈 月別推移</h2>
                <div class="chart-container">
                    <canvas id="monthlyChart"></canvas>
                </div>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-color download"></div>
                        <span>ダウンロード</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color preview"></div>
                        <span>プレビュー</span>
                    </div>
                </div>
                <p style="text-align: center; color: #999; font-size: 0.85em; margin-top: 10px;">
                    クリックで詳細表示
                </p>
            </div>

            <div class="chart-card">
                <h2>📅 日別推移（直近30日）</h2>
                <div class="chart-container">
                    <canvas id="dailyChart"></canvas>
                </div>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-color download"></div>
                        <span>ダウンロード</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-color preview"></div>
                        <span>プレビュー</span>
                    </div>
                </div>
            </div>
        </div>

        <div class="chart-card" style="margin-bottom: 30px;">
            <h2>🕐 時間帯別アクセス数</h2>
            <div class="chart-container" style="height: 250px;">
                <canvas id="hourlyChart"></canvas>
            </div>
            <div class="legend">
                <div class="legend-item">
                    <div class="legend-color download"></div>
                    <span>ダウンロード</span>
                </div>
                <div class="legend-item">
                    <div class="legend-color preview"></div>
                    <span>プレビュー</span>
                </div>
            </div>
        </div>

        <div class="table-card">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                <h2>👥 トップユーザー（総アクセス数）</h2>
                <div class="toggle-buttons">
                    <button class="toggle-btn active" onclick="showTopUsers(10)">トップ10</button>
                    <button class="toggle-btn" onclick="showTopUsers({total_user_count})">すべて ({total_user_count}人)</button>
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
                <tbody id="topUsersTable">
'''

    for i, (name, email, dl_count, pv_count, total, files) in enumerate(top_users, 1):
        duplication_rate = ((total - files) / total * 100) if total > 0 else 0
        show_class = 'show' if i <= 10 else ''

        html += f'''                    <tr class="user-row {show_class}" data-rank="{i}">
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

    html += '''                </tbody>
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

    for i, (file_name, folder, dl_count, pv_count, total, users, user_names) in enumerate(top_files_with_users, 1):
        users_json = json.dumps(user_names, ensure_ascii=False)
        html += f'''                    <tr>
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

    html += f'''                </tbody>
            </table>
        </div>

        <div class="footer">
            <p>🤖 Generated with Claude Code</p>
            <p style="font-size: 0.9em; margin-top: 5px;">
                {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')} に生成
            </p>
        </div>
    </div>

    <div id="tooltip" class="tooltip"></div>

    <!-- Monthly Details Modal -->
    <div id="monthModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2 id="modalTitle">月別アクセス詳細</h2>
                <button class="close-btn" onclick="closeModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div id="modalContent"></div>
            </div>
        </div>
    </div>

    <script>
        // Monthly details data
        const monthlyDetails = {json.dumps(monthly_details, ensure_ascii=False, indent=2)};

        // Modal functions
        function showMonthDetails(month) {{
            const modal = document.getElementById('monthModal');
            const modalTitle = document.getElementById('modalTitle');
            const modalContent = document.getElementById('modalContent');

            const details = monthlyDetails[month];
            if (!details || details.length === 0) {{
                return;
            }}

            modalTitle.textContent = `${{month}} アクセス詳細 (${{details.length}}件)`;

            let tableHTML = `
                <table class="detail-table">
                    <thead>
                        <tr>
                            <th style="width: 50px;">#</th>
                            <th style="width: 100px;">操作</th>
                            <th style="width: 150px;">ユーザー名</th>
                            <th style="width: 200px;">メールアドレス</th>
                            <th>ファイル名</th>
                            <th style="width: 250px;">フォルダ</th>
                            <th style="width: 150px;">日時</th>
                        </tr>
                    </thead>
                    <tbody>
            `;

            details.forEach((item, index) => {{
                const eventBadge = item.event_type === 'DOWNLOAD'
                    ? '<span class="badge download">DL</span>'
                    : '<span class="badge preview">PV</span>';

                tableHTML += `
                    <tr>
                        <td>${{index + 1}}</td>
                        <td>${{eventBadge}}</td>
                        <td>${{item.user_name}}</td>
                        <td style="font-size: 0.85em;">${{item.user_login}}</td>
                        <td>${{item.file_name}}</td>
                        <td style="font-size: 0.85em; color: #666;">${{item.parent_folder || '-'}}</td>
                        <td style="font-size: 0.85em;">${{item.download_at.split('T').join(' ')}}</td>
                    </tr>
                `;
            }});

            tableHTML += `
                    </tbody>
                </table>
            `;

            modalContent.innerHTML = tableHTML;
            modal.style.display = 'block';
        }}

        function closeModal() {{
            const modal = document.getElementById('monthModal');
            modal.style.display = 'none';
        }}

        // Close modal when clicking outside
        window.onclick = function(event) {{
            const modal = document.getElementById('monthModal');
            if (event.target === modal) {{
                closeModal();
            }}
        }};

        // Close modal with ESC key
        document.addEventListener('keydown', function(event) {{
            if (event.key === 'Escape') {{
                closeModal();
            }}
        }});

        // Toggle top users display
        function showTopUsers(limit) {{
            // Update button states
            document.querySelectorAll('.toggle-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});
            event.target.classList.add('active');

            // Show/hide rows based on limit
            const rows = document.querySelectorAll('.user-row');
            rows.forEach(row => {{
                const rank = parseInt(row.getAttribute('data-rank'));
                if (rank <= limit) {{
                    row.classList.add('show');
                }} else {{
                    row.classList.remove('show');
                }}
            }});
        }}

        // Tooltip for user count
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

        // Monthly Chart with click event
        const monthlyCtx = document.getElementById('monthlyChart').getContext('2d');
        new Chart(monthlyCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(monthly_labels)},
                datasets: [
                    {{
                        label: 'ダウンロード',
                        data: {json.dumps(monthly_downloads)},
                        backgroundColor: 'rgba(76, 175, 80, 0.8)',
                        borderColor: 'rgba(76, 175, 80, 1)',
                        borderWidth: 2
                    }},
                    {{
                        label: 'プレビュー',
                        data: {json.dumps(monthly_previews)},
                        backgroundColor: 'rgba(255, 152, 0, 0.8)',
                        borderColor: 'rgba(255, 152, 0, 1)',
                        borderWidth: 2
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                onClick: (event, activeElements) => {{
                    if (activeElements.length > 0) {{
                        const index = activeElements[0].index;
                        const month = {json.dumps(monthly_labels)}[index];
                        showMonthDetails(month);
                    }}
                }},
                plugins: {{
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            footer: function() {{
                                return 'クリックで詳細表示';
                            }}
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        stacked: true,
                        ticks: {{
                            font: {{
                                size: 11
                            }}
                        }}
                    }},
                    y: {{
                        stacked: true,
                        beginAtZero: true,
                        ticks: {{
                            font: {{
                                size: 12
                            }}
                        }}
                    }}
                }}
            }}
        }});

        // Daily Chart with custom tooltips
        const dailyCtx = document.getElementById('dailyChart').getContext('2d');
        const dailyTooltips = {json.dumps(daily_tooltips)};

        // Register custom positioner for adaptive tooltip placement
        Chart.Tooltip.positioners.adaptive = function(elements, eventPosition) {{
            if (!elements.length) {{
                return false;
            }}

            const element = elements[0];
            const chart = this.chart;
            const chartArea = chart.chartArea;
            const pointY = element.element.y;
            const chartHeight = chartArea.bottom - chartArea.top;
            const midPoint = chartArea.top + (chartHeight / 2);

            // Calculate tooltip position
            const x = element.element.x;
            let y = element.element.y;

            // Adjust y position based on point location
            if (pointY > midPoint) {{
                // Point is in lower half, show tooltip above
                y = pointY - 10;
            }} else {{
                // Point is in upper half, show tooltip below
                y = pointY + 10;
            }}

            return {{
                x: x,
                y: y,
                xAlign: 'center',
                yAlign: pointY > midPoint ? 'bottom' : 'top'
            }};
        }};

        new Chart(dailyCtx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(daily_labels)},
                datasets: [
                    {{
                        label: 'ダウンロード',
                        data: {json.dumps(daily_downloads)},
                        borderColor: 'rgba(76, 175, 80, 1)',
                        backgroundColor: 'rgba(76, 175, 80, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4
                    }},
                    {{
                        label: 'プレビュー',
                        data: {json.dumps(daily_previews)},
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
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        position: 'adaptive',
                        callbacks: {{
                            title: function(context) {{
                                const data = dailyTooltips[context[0].dataIndex];
                                return data.date;
                            }},
                            beforeBody: function(context) {{
                                const data = dailyTooltips[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 / PV: ${{data.pv_count}}件 (${{data.unique_users}}人)`;
                            }},
                            label: function(context) {{
                                const data = dailyTooltips[context.dataIndex];
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
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            font: {{
                                size: 12
                            }}
                        }}
                    }},
                    x: {{
                        ticks: {{
                            font: {{
                                size: 10
                            }},
                            maxRotation: 45,
                            minRotation: 45
                        }}
                    }}
                }},
                interaction: {{
                    mode: 'nearest',
                    intersect: false
                }}
            }}
        }});

        // Hourly Chart with custom tooltips
        const hourlyCtx = document.getElementById('hourlyChart').getContext('2d');
        const hourlyTooltips = {json.dumps(hourly_tooltips)};

        new Chart(hourlyCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(hourly_labels)},
                datasets: [
                    {{
                        label: 'ダウンロード',
                        data: {json.dumps(hourly_downloads)},
                        backgroundColor: 'rgba(76, 175, 80, 0.8)',
                        borderColor: 'rgba(76, 175, 80, 1)',
                        borderWidth: 2
                    }},
                    {{
                        label: 'プレビュー',
                        data: {json.dumps(hourly_previews)},
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
                    legend: {{
                        display: false
                    }},
                    tooltip: {{
                        callbacks: {{
                            title: function(context) {{
                                const data = hourlyTooltips[context[0].dataIndex];
                                return data.hour;
                            }},
                            beforeBody: function(context) {{
                                const data = hourlyTooltips[context[0].dataIndex];
                                return `DL: ${{data.dl_count}}件 / PV: ${{data.pv_count}}件`;
                            }},
                            label: function(context) {{
                                const data = hourlyTooltips[context.dataIndex];
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
                    x: {{
                        stacked: true,
                        ticks: {{
                            font: {{
                                size: 11
                            }}
                        }}
                    }},
                    y: {{
                        stacked: true,
                        beginAtZero: true,
                        ticks: {{
                            font: {{
                                size: 12
                            }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>'''

    # Write HTML file
    output_path = r"data\dashboard_integrated.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Dashboard generated: {output_path}")
    return output_path


if __name__ == '__main__':
    output_path = generate_dashboard()
    print(f"\n[OK] 統合ダッシュボードを生成しました: {output_path}")
    print(f"\nブラウザで開いてください:")
    print(f"  file:///{output_path.replace(chr(92), '/')}")
