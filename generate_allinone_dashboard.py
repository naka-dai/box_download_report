"""
Generate All-in-One Dashboard
統合・ダウンロード・プレビューの3つのダッシュボードを1つのHTMLに統合
タブ切り替えで各ビューを表示（フル機能版）
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path

def generate_dashboard():
    """Generate all-in-one HTML dashboard from database statistics."""

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

    print("Collecting statistics...")

    # Get summary statistics for all types
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

    # Calculate ratios
    total_access = total_downloads + total_previews
    download_ratio = (total_downloads / total_access * 100) if total_access > 0 else 0
    preview_ratio = (total_previews / total_access * 100) if total_access > 0 else 0

    # Get monthly statistics for integrated view
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
    monthly_integrated = cursor.fetchall()

    # Get monthly statistics for download only
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            COUNT(*) as download_count
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_download = cursor.fetchall()

    # Get monthly statistics for preview only
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            COUNT(*) as preview_count
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_preview = cursor.fetchall()

    # Get daily statistics (last 30 days) for integrated view
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_integrated = list(reversed(cursor.fetchall()))

    # Get daily statistics for download only
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            COUNT(*) as download_count
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_download = list(reversed(cursor.fetchall()))

    # Get daily statistics for preview only
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            COUNT(*) as preview_count
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_preview = list(reversed(cursor.fetchall()))

    # Get hourly statistics for integrated view
    cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY hour
        ORDER BY hour
    ''', admin_params)
    hourly_integrated = cursor.fetchall()

    # Get hourly statistics for download only
    cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            COUNT(*) as download_count
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})
        GROUP BY hour
        ORDER BY hour
    ''', admin_params)
    hourly_download = cursor.fetchall()

    # Get hourly statistics for preview only
    cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            COUNT(*) as preview_count
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})
        GROUP BY hour
        ORDER BY hour
    ''', admin_params)
    hourly_preview = cursor.fetchall()

    # Get top users for integrated view
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(*) as total_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY user_login
        ORDER BY total_count DESC
        LIMIT 10
    ''', admin_params)
    top_users_integrated = cursor.fetchall()

    # Get top users for download only
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            COUNT(*) as download_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})
        GROUP BY user_login
        ORDER BY download_count DESC
        LIMIT 10
    ''', admin_params)
    top_users_download = cursor.fetchall()

    # Get top users for preview only
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            COUNT(*) as preview_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})
        GROUP BY user_login
        ORDER BY preview_count DESC
        LIMIT 10
    ''', admin_params)
    top_users_preview = cursor.fetchall()

    # Get top files for integrated view
    cursor.execute(f'''
        SELECT
            file_name,
            raw_json,
            SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as download_count,
            SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as preview_count,
            COUNT(*) as total_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY file_id
        ORDER BY total_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_integrated_raw = cursor.fetchall()
    top_files_integrated = []
    for file_name, raw_json, dl_count, pv_count, total in top_files_integrated_raw:
        folder = ''
        if raw_json:
            try:
                data = json.loads(raw_json)
                folder = data.get('parent_folder', '')
            except:
                pass
        top_files_integrated.append((file_name, folder, dl_count, pv_count, total))

    # Get top files for download only
    cursor.execute(f'''
        SELECT
            file_name,
            raw_json,
            COUNT(*) as download_count
        FROM downloads
        WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders})
        GROUP BY file_id
        ORDER BY download_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_download_raw = cursor.fetchall()
    top_files_download = []
    for file_name, raw_json, count in top_files_download_raw:
        folder = ''
        if raw_json:
            try:
                data = json.loads(raw_json)
                folder = data.get('parent_folder', '')
            except:
                pass
        top_files_download.append((file_name, folder, count))

    # Get top files for preview only
    cursor.execute(f'''
        SELECT
            file_name,
            raw_json,
            COUNT(*) as preview_count
        FROM downloads
        WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders})
        GROUP BY file_id
        ORDER BY preview_count DESC
        LIMIT 10
    ''', admin_params)
    top_files_preview_raw = cursor.fetchall()
    top_files_preview = []
    for file_name, raw_json, count in top_files_preview_raw:
        folder = ''
        if raw_json:
            try:
                data = json.loads(raw_json)
                folder = data.get('parent_folder', '')
            except:
                pass
        top_files_preview.append((file_name, folder, count))

    conn.close()

    print(f"Total Downloads: {total_downloads:,}")
    print(f"Total Previews: {total_previews:,}")
    print(f"Generating all-in-one dashboard...")

    # Prepare chart data
    monthly_integrated_labels = [row[0] for row in monthly_integrated]
    monthly_integrated_downloads = [row[1] for row in monthly_integrated]
    monthly_integrated_previews = [row[2] for row in monthly_integrated]

    monthly_download_labels = [row[0] for row in monthly_download]
    monthly_download_values = [row[1] for row in monthly_download]

    monthly_preview_labels = [row[0] for row in monthly_preview]
    monthly_preview_values = [row[1] for row in monthly_preview]

    daily_integrated_labels = [row[0] for row in daily_integrated]
    daily_integrated_downloads = [row[1] for row in daily_integrated]
    daily_integrated_previews = [row[2] for row in daily_integrated]

    daily_download_labels = [row[0] for row in daily_download]
    daily_download_values = [row[1] for row in daily_download]

    daily_preview_labels = [row[0] for row in daily_preview]
    daily_preview_values = [row[1] for row in daily_preview]

    hourly_integrated_labels = [f"{row[0]:02d}:00" for row in hourly_integrated]
    hourly_integrated_downloads = [row[1] for row in hourly_integrated]
    hourly_integrated_previews = [row[2] for row in hourly_integrated]

    hourly_download_labels = [f"{row[0]:02d}:00" for row in hourly_download]
    hourly_download_values = [row[1] for row in hourly_download]

    hourly_preview_labels = [f"{row[0]:02d}:00" for row in hourly_preview]
    hourly_preview_values = [row[1] for row in hourly_preview]

    # Generate compact HTML with embedded data
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Box レポート オールインワン ダッシュボード</title>
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

        /* Tab Content */
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
            <h1>📊 Box レポート オールインワン</h1>
            <p>図面001フォルダ アクセス分析ダッシュボード</p>
            <p style="font-size: 0.9em; color: #999; margin-top: 10px;">
                期間: {min_date} ～ {max_date}
            </p>

            <div class="tab-nav">
                <button class="tab-btn integrated active" onclick="switchTab('integrated')">
                    📊 統合レポート
                </button>
                <button class="tab-btn download" onclick="switchTab('download')">
                    📥 ダウンロード専用
                </button>
                <button class="tab-btn preview" onclick="switchTab('preview')">
                    👁️ プレビュー専用
                </button>
            </div>
        </div>

        <!-- Integrated Tab -->
        <div id="integrated-tab" class="tab-content active">
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
                        <canvas id="monthlyIntegratedChart"></canvas>
                    </div>
                </div>

                <div class="chart-card">
                    <h2>📅 日別推移（直近30日）</h2>
                    <div class="chart-container">
                        <canvas id="dailyIntegratedChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="chart-card" style="margin-bottom: 30px;">
                <h2>🕐 時間帯別アクセス数</h2>
                <div class="chart-container" style="height: 250px;">
                    <canvas id="hourlyIntegratedChart"></canvas>
                </div>
            </div>

            <div class="table-card">
                <h2>👥 トップユーザー（総アクセス数）</h2>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">順位</th>
                            <th>ユーザー名</th>
                            <th>メールアドレス</th>
                            <th style="text-align: right;">ダウンロード</th>
                            <th style="text-align: right;">プレビュー</th>
                            <th style="text-align: right;">合計</th>
                        </tr>
                    </thead>
                    <tbody>
'''

    for i, (name, email, dl_count, pv_count, total) in enumerate(top_users_integrated, 1):
        html += f'''                        <tr>
                            <td><span class="rank">{i}</span></td>
                            <td>{name}</td>
                            <td>{email}</td>
                            <td style="text-align: right; color: #4CAF50; font-weight: bold;">{dl_count:,}</td>
                            <td style="text-align: right; color: #FF9800; font-weight: bold;">{pv_count:,}</td>
                            <td style="text-align: right; font-weight: bold;">{total:,}</td>
                        </tr>
'''

    html += '''                    </tbody>
                </table>
            </div>

            <div class="table-card">
                <h2>📁 トップファイル（総アクセス数）</h2>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">順位</th>
                            <th>ファイル名</th>
                            <th>フォルダ</th>
                            <th style="text-align: right;">ダウンロード</th>
                            <th style="text-align: right;">プレビュー</th>
                            <th style="text-align: right;">合計</th>
                        </tr>
                    </thead>
                    <tbody>
'''

    for i, (file_name, folder, dl_count, pv_count, total) in enumerate(top_files_integrated, 1):
        html += f'''                        <tr>
                            <td><span class="rank">{i}</span></td>
                            <td>{file_name}</td>
                            <td style="font-size: 0.9em; color: #666;">{folder}</td>
                            <td style="text-align: right; color: #4CAF50; font-weight: bold;">{dl_count:,}</td>
                            <td style="text-align: right; color: #FF9800; font-weight: bold;">{pv_count:,}</td>
                            <td style="text-align: right; font-weight: bold;">{total:,}</td>
                        </tr>
'''

    html += '''                    </tbody>
                </table>
            </div>
        </div>

        <!-- Download Tab -->
        <div id="download-tab" class="tab-content">
            <div class="stats-grid">
                <div class="stat-card download">
                    <h3>総ダウンロード数</h3>
                    <div class="value">{total_downloads:,}</div>
                </div>
                <div class="stat-card download">
                    <h3>ユニークユーザー</h3>
                    <div class="value">{unique_users_download}</div>
                </div>
                <div class="stat-card">
                    <h3>ファイル数</h3>
                    <div class="value">{unique_files:,}</div>
                </div>
            </div>

            <div class="chart-grid">
                <div class="chart-card">
                    <h2>📈 月別ダウンロード推移</h2>
                    <div class="chart-container">
                        <canvas id="monthlyDownloadChart"></canvas>
                    </div>
                </div>

                <div class="chart-card">
                    <h2>📅 日別ダウンロード推移（直近30日）</h2>
                    <div class="chart-container">
                        <canvas id="dailyDownloadChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="chart-card" style="margin-bottom: 30px;">
                <h2>🕐 時間帯別ダウンロード数</h2>
                <div class="chart-container" style="height: 250px;">
                    <canvas id="hourlyDownloadChart"></canvas>
                </div>
            </div>

            <div class="table-card">
                <h2>👥 トップユーザー</h2>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">順位</th>
                            <th>ユーザー名</th>
                            <th>メールアドレス</th>
                            <th style="text-align: right;">ダウンロード数</th>
                            <th style="text-align: right;">ユニークファイル</th>
                        </tr>
                    </thead>
                    <tbody>
'''.format(total_downloads=total_downloads, unique_users_download=unique_users_download, unique_files=unique_files)

    for i, (name, email, count, files) in enumerate(top_users_download, 1):
        html += f'''                        <tr>
                            <td><span class="rank">{i}</span></td>
                            <td>{name}</td>
                            <td>{email}</td>
                            <td style="text-align: right; font-weight: bold;">{count:,}</td>
                            <td style="text-align: right;">{files:,}</td>
                        </tr>
'''

    html += '''                    </tbody>
                </table>
            </div>

            <div class="table-card">
                <h2>📁 トップファイル</h2>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">順位</th>
                            <th>ファイル名</th>
                            <th>フォルダ</th>
                            <th style="text-align: right;">ダウンロード数</th>
                        </tr>
                    </thead>
                    <tbody>
'''

    for i, (file_name, folder, count) in enumerate(top_files_download, 1):
        html += f'''                        <tr>
                            <td><span class="rank">{i}</span></td>
                            <td>{file_name}</td>
                            <td style="font-size: 0.9em; color: #666;">{folder}</td>
                            <td style="text-align: right; font-weight: bold;">{count:,}</td>
                        </tr>
'''

    html += '''                    </tbody>
                </table>
            </div>
        </div>

        <!-- Preview Tab -->
        <div id="preview-tab" class="tab-content">
            <div class="stats-grid">
                <div class="stat-card preview">
                    <h3>総プレビュー数</h3>
                    <div class="value">{total_previews:,}</div>
                </div>
                <div class="stat-card preview">
                    <h3>ユニークユーザー</h3>
                    <div class="value">{unique_users_preview}</div>
                </div>
                <div class="stat-card">
                    <h3>プレビューファイル数</h3>
                    <div class="value">{unique_files:,}</div>
                </div>
            </div>

            <div class="chart-grid">
                <div class="chart-card">
                    <h2>📈 月別プレビュー推移</h2>
                    <div class="chart-container">
                        <canvas id="monthlyPreviewChart"></canvas>
                    </div>
                </div>

                <div class="chart-card">
                    <h2>📅 日別プレビュー推移（直近30日）</h2>
                    <div class="chart-container">
                        <canvas id="dailyPreviewChart"></canvas>
                    </div>
                </div>
            </div>

            <div class="chart-card" style="margin-bottom: 30px;">
                <h2>🕐 時間帯別プレビュー数</h2>
                <div class="chart-container" style="height: 250px;">
                    <canvas id="hourlyPreviewChart"></canvas>
                </div>
            </div>

            <div class="table-card">
                <h2>👥 トップユーザー</h2>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">順位</th>
                            <th>ユーザー名</th>
                            <th>メールアドレス</th>
                            <th style="text-align: right;">プレビュー数</th>
                            <th style="text-align: right;">ユニークファイル</th>
                        </tr>
                    </thead>
                    <tbody>
'''.format(total_previews=total_previews, unique_users_preview=unique_users_preview, unique_files=unique_files)

    for i, (name, email, count, files) in enumerate(top_users_preview, 1):
        html += f'''                        <tr>
                            <td><span class="rank">{i}</span></td>
                            <td>{name}</td>
                            <td>{email}</td>
                            <td style="text-align: right; font-weight: bold;">{count:,}</td>
                            <td style="text-align: right;">{files:,}</td>
                        </tr>
'''

    html += '''                    </tbody>
                </table>
            </div>

            <div class="table-card">
                <h2>📁 トップファイル</h2>
                <table>
                    <thead>
                        <tr>
                            <th style="width: 50px;">順位</th>
                            <th>ファイル名</th>
                            <th>フォルダ</th>
                            <th style="text-align: right;">プレビュー数</th>
                        </tr>
                    </thead>
                    <tbody>
'''

    for i, (file_name, folder, count) in enumerate(top_files_preview, 1):
        html += f'''                        <tr>
                            <td><span class="rank">{i}</span></td>
                            <td>{file_name}</td>
                            <td style="font-size: 0.9em; color: #666;">{folder}</td>
                            <td style="text-align: right; font-weight: bold;">{count:,}</td>
                        </tr>
'''

    html += f'''                    </tbody>
                </table>
            </div>
        </div>

        <div class="footer">
            <p>🤖 Generated with Claude Code</p>
            <p style="font-size: 0.9em; margin-top: 5px;">
                {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')} に生成
            </p>
            <p style="font-size: 0.85em; margin-top: 10px; opacity: 0.8;">
                オールインワン版 - タブ切り替えで各レポートを表示
            </p>
        </div>
    </div>

    <script>
        function switchTab(tabName) {{
            // Hide all tabs
            document.querySelectorAll('.tab-content').forEach(tab => {{
                tab.classList.remove('active');
            }});

            // Deactivate all buttons
            document.querySelectorAll('.tab-btn').forEach(btn => {{
                btn.classList.remove('active');
            }});

            // Show selected tab
            document.getElementById(tabName + '-tab').classList.add('active');

            // Activate selected button
            event.target.classList.add('active');
        }}

        // Integrated Charts
        new Chart(document.getElementById('monthlyIntegratedChart').getContext('2d'), {{
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
                scales: {{
                    x: {{ stacked: true }},
                    y: {{ stacked: true, beginAtZero: true }}
                }}
            }}
        }});

        new Chart(document.getElementById('dailyIntegratedChart').getContext('2d'), {{
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
                scales: {{
                    y: {{ beginAtZero: true }}
                }}
            }}
        }});

        new Chart(document.getElementById('hourlyIntegratedChart').getContext('2d'), {{
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
                scales: {{
                    x: {{ stacked: true }},
                    y: {{ stacked: true, beginAtZero: true }}
                }}
            }}
        }});

        // Download Charts
        new Chart(document.getElementById('monthlyDownloadChart').getContext('2d'), {{
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
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        new Chart(document.getElementById('dailyDownloadChart').getContext('2d'), {{
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
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        new Chart(document.getElementById('hourlyDownloadChart').getContext('2d'), {{
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
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        // Preview Charts
        new Chart(document.getElementById('monthlyPreviewChart').getContext('2d'), {{
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
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        new Chart(document.getElementById('dailyPreviewChart').getContext('2d'), {{
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
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});

        new Chart(document.getElementById('hourlyPreviewChart').getContext('2d'), {{
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
                plugins: {{ legend: {{ display: false }} }},
                scales: {{ y: {{ beginAtZero: true }} }}
            }}
        }});
    </script>
</body>
</html>'''

    # Write HTML file
    output_path = r"data\dashboard_allinone.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"[OK] All-in-one dashboard generated: {output_path}")
    print(f"File size: {len(html):,} bytes")
    return output_path


if __name__ == '__main__':
    output_path = generate_dashboard()
    print(f"\n[OK] Dashboard successfully created: {output_path}")
