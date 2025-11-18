"""
Generate Complete Period-Filtered Dashboard
期間フィルタリング機能を持つ完全版ダッシュボード生成
Based on dashboard_allinone_full.html structure with period filtering
"""

import sqlite3
import json
from pathlib import Path


def generate_dashboard():
    """Generate period-filtered dashboard with full features."""

    print("期間フィルタ付き完全版ダッシュボード生成開始...")

    # Read Chart.js library
    chartjs_path = Path(__file__).parent / "chart.js"
    with open(chartjs_path, 'r', encoding='utf-8') as f:
        chartjs_code = f.read()

    # Connect to database
    db_path = r"data\box_audit.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get admin emails to exclude
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

    placeholders = ','.join(['?' for _ in admin_emails])
    admin_params = tuple(admin_emails)

    print(f"  管理者除外: {len(admin_emails)}件")

    # Collect data for all three periods × three tabs = 9 datasets
    print("  データ収集中...")

    periods = {
        'all': ('全期間', ''),
        'before': ('運用開始前（2024年10月13日まで）', "AND DATE(download_at_jst) <= '2024-10-13'"),
        'after': ('運用開始後（2024年10月14日以降）', "AND DATE(download_at_jst) >= '2024-10-14'")
    }

    all_data = {}

    for period_key, (period_label, period_clause) in periods.items():
        print(f"    {period_label}...")

        # Integrated tab data
        all_data[f'{period_key}_int'] = collect_integrated_data(
            cursor, admin_params, placeholders, period_clause, period_label
        )

        # Download tab data
        all_data[f'{period_key}_dl'] = collect_download_data(
            cursor, admin_params, placeholders, period_clause, period_label
        )

        # Preview tab data
        all_data[f'{period_key}_pv'] = collect_preview_data(
            cursor, admin_params, placeholders, period_clause, period_label
        )

    conn.close()

    print("  HTML生成中...")

    # Generate HTML
    output_path = r"data\dashboard_period_full.html"
    html = generate_html(all_data, chartjs_code)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n[OK] 完成: {output_path}")
    print(f"     file:///{output_path.replace(chr(92), '/')}")

    return output_path


def collect_integrated_data(cursor, admin_params, placeholders, period_clause, period_label):
    """Collect integrated (DL+PV) data."""

    data = {'period_label': period_label}

    # Basic stats
    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['total_dl'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['total_pv'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_users_dl'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_users_pv'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT file_id) FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_files'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    min_date, max_date = cursor.fetchone()
    data['date_range'] = f'{min_date or "N/A"} ～ {max_date or "N/A"}'

    # Ratios
    total = data['total_dl'] + data['total_pv']
    data['dl_ratio'] = f"{(data['total_dl'] / total * 100) if total > 0 else 0:.0f}%"
    data['pv_ratio'] = f"{(data['total_pv'] / total * 100) if total > 0 else 0:.0f}%"

    # Monthly data
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv
        FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly = cursor.fetchall()
    data['monthly_labels'] = json.dumps([row[0] for row in monthly])
    data['monthly_dl'] = json.dumps([row[1] for row in monthly])
    data['monthly_pv'] = json.dumps([row[2] for row in monthly])

    # Daily data (last 30 days)
    cursor.execute(f'''
        SELECT DATE(download_at_jst) as day,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv
        FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', admin_params)
    daily = list(reversed(cursor.fetchall()))
    data['daily_labels'] = json.dumps([row[0] for row in daily])
    data['daily_dl'] = json.dumps([row[1] for row in daily])
    data['daily_pv'] = json.dumps([row[2] for row in daily])

    # Hourly data
    cursor.execute(f'''
        SELECT CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv
        FROM downloads WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour ORDER BY hour
    ''', admin_params)
    hourly = cursor.fetchall()
    # Fill missing hours with 0
    hourly_dict_dl = {row[0]: row[1] for row in hourly}
    hourly_dict_pv = {row[0]: row[2] for row in hourly}
    data['hourly_labels'] = json.dumps([f'{h:02d}:00' for h in range(24)])
    data['hourly_dl'] = json.dumps([hourly_dict_dl.get(h, 0) for h in range(24)])
    data['hourly_pv'] = json.dumps([hourly_dict_pv.get(h, 0) for h in range(24)])

    # Top users (DL)
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    data['top_users_dl'] = cursor.fetchall()

    # Top users (PV)
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    data['top_users_pv'] = cursor.fetchall()

    # Top files (DL)
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt, COUNT(DISTINCT user_login) as users
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    data['top_files_dl'] = cursor.fetchall()

    # Top files (PV)
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt, COUNT(DISTINCT user_login) as users
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    data['top_files_pv'] = cursor.fetchall()

    return data


def collect_download_data(cursor, admin_params, placeholders, period_clause, period_label):
    """Collect download-only data."""

    data = {'period_label': period_label}

    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['total'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_users'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT file_id) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_files'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    min_date, max_date = cursor.fetchone()
    data['date_range'] = f'{min_date or "N/A"} ～ {max_date or "N/A"}'

    # Monthly
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month, COUNT(*) as cnt
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly = cursor.fetchall()
    data['monthly_labels'] = json.dumps([row[0] for row in monthly])
    data['monthly_counts'] = json.dumps([row[1] for row in monthly])

    # Daily
    cursor.execute(f'''
        SELECT DATE(download_at_jst) as day, COUNT(*) as cnt
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', admin_params)
    daily = list(reversed(cursor.fetchall()))
    data['daily_labels'] = json.dumps([row[0] for row in daily])
    data['daily_counts'] = json.dumps([row[1] for row in daily])

    # Hourly
    cursor.execute(f'''
        SELECT CAST(strftime('%H', download_at_jst) AS INTEGER) as hour, COUNT(*) as cnt
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour ORDER BY hour
    ''', admin_params)
    hourly = cursor.fetchall()
    hourly_dict = {row[0]: row[1] for row in hourly}
    data['hourly_labels'] = json.dumps([f'{h:02d}:00' for h in range(24)])
    data['hourly_counts'] = json.dumps([hourly_dict.get(h, 0) for h in range(24)])

    # Top users
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    data['top_users'] = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt, COUNT(DISTINCT user_login) as users
        FROM downloads WHERE event_type = "DOWNLOAD" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    data['top_files'] = cursor.fetchall()

    return data


def collect_preview_data(cursor, admin_params, placeholders, period_clause, period_label):
    """Collect preview-only data."""

    data = {'period_label': period_label}

    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['total'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_users'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT file_id) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    data['unique_files'] = cursor.fetchone()[0]

    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}', admin_params)
    min_date, max_date = cursor.fetchone()
    data['date_range'] = f'{min_date or "N/A"} ～ {max_date or "N/A"}'

    # Monthly
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month, COUNT(*) as cnt
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly = cursor.fetchall()
    data['monthly_labels'] = json.dumps([row[0] for row in monthly])
    data['monthly_counts'] = json.dumps([row[1] for row in monthly])

    # Daily
    cursor.execute(f'''
        SELECT DATE(download_at_jst) as day, COUNT(*) as cnt
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY day ORDER BY day DESC LIMIT 30
    ''', admin_params)
    daily = list(reversed(cursor.fetchall()))
    data['daily_labels'] = json.dumps([row[0] for row in daily])
    data['daily_counts'] = json.dumps([row[1] for row in daily])

    # Hourly
    cursor.execute(f'''
        SELECT CAST(strftime('%H', download_at_jst) AS INTEGER) as hour, COUNT(*) as cnt
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY hour ORDER BY hour
    ''', admin_params)
    hourly = cursor.fetchall()
    hourly_dict = {row[0]: row[1] for row in hourly}
    data['hourly_labels'] = json.dumps([f'{h:02d}:00' for h in range(24)])
    data['hourly_counts'] = json.dumps([hourly_dict.get(h, 0) for h in range(24)])

    # Top users
    cursor.execute(f'''
        SELECT user_login, COUNT(*) as cnt, COUNT(DISTINCT file_id) as files
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY user_login ORDER BY cnt DESC
    ''', admin_params)
    data['top_users'] = cursor.fetchall()

    # Top files
    cursor.execute(f'''
        SELECT file_id, file_name, COUNT(*) as cnt, COUNT(DISTINCT user_login) as users
        FROM downloads WHERE event_type = "PREVIEW" AND user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY file_id ORDER BY cnt DESC LIMIT 50
    ''', admin_params)
    data['top_files'] = cursor.fetchall()

    return data


def generate_html(all_data, chartjs_code):
    """Generate complete HTML dashboard."""

    # Extract data for easier access
    d_all_int = all_data['all_int']
    d_all_dl = all_data['all_dl']
    d_all_pv = all_data['all_pv']
    d_before_int = all_data['before_int']
    d_before_dl = all_data['before_dl']
    d_before_pv = all_data['before_pv']
    d_after_int = all_data['after_int']
    d_after_dl = all_data['after_dl']
    d_after_pv = all_data['after_pv']

    # Generate HTML content
    # This will be a large HTML file with embedded JavaScript
    # Structure: Period selector → Tab selector → Content for each period×tab combination

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Box オールインワンダッシュボード - 期間フィルタ付き</title>

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
            margin-bottom: 20px;
            text-align: center;
        }}

        .header h1 {{
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}

        .period-selector {{
            background: white;
            padding: 20px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            text-align: center;
        }}

        .period-selector label {{
            font-size: 1.1em;
            font-weight: bold;
            margin-right: 15px;
            color: #667eea;
        }}

        .period-selector select {{
            padding: 12px 20px;
            font-size: 1em;
            border-radius: 8px;
            border: 2px solid #667eea;
            background: white;
            color: #333;
            cursor: pointer;
            min-width: 300px;
        }}

        .period-selector select:hover {{
            border-color: #764ba2;
        }}

        .tab-navigation {{
            background: white;
            padding: 15px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 20px;
            display: flex;
            justify-content: center;
            gap: 10px;
            flex-wrap: wrap;
        }}

        .tab-btn {{
            padding: 15px 30px;
            border: none;
            border-radius: 10px;
            font-size: 1.1em;
            cursor: pointer;
            transition: all 0.3s ease;
            background: #f0f0f0;
            color: #666;
            font-weight: 500;
        }}

        .tab-btn:hover {{
            background: #e0e0e0;
            transform: translateY(-2px);
        }}

        .tab-btn.active {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: bold;
        }}

        .tab-btn.integrated.active {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }}

        .tab-btn.download.active {{
            background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
        }}

        .tab-btn.preview.active {{
            background: linear-gradient(135deg, #FF9800 0%, #F57C00 100%);
        }}

        .content-wrapper {{
            display: none;
        }}

        .content-wrapper.active {{
            display: block;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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
            font-size: 2.2em;
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

        .chart-section {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}

        .chart-section h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.5em;
        }}

        .chart-container {{
            position: relative;
            height: 400px;
            margin-bottom: 20px;
        }}

        .table-section {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}

        .table-section h2 {{
            color: #667eea;
            margin-bottom: 20px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}

        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}

        th {{
            background: #f8f9fa;
            font-weight: 600;
            color: #667eea;
        }}

        tr:hover {{
            background: #f8f9fa;
        }}

        .info-box {{
            background: #e3f2fd;
            border-left: 4px solid #2196f3;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 8px;
        }}

        .toggle-btn {{
            padding: 8px 15px;
            border: none;
            border-radius: 5px;
            background: #667eea;
            color: white;
            cursor: pointer;
            margin-bottom: 15px;
        }}

        .toggle-btn:hover {{
            background: #764ba2;
        }}

        .badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.85em;
            font-weight: bold;
            margin-left: 8px;
        }}

        .badge.download {{
            background: #4CAF50;
            color: white;
        }}

        .badge.preview {{
            background: #FF9800;
            color: white;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Box オールインワンダッシュボード</h1>
            <p>期間フィルタリング機能付き完全版</p>
        </div>

        <div class="period-selector">
            <label for="period-select">📅 集計期間:</label>
            <select id="period-select" onchange="switchPeriod(this.value)">
                <option value="all">全期間</option>
                <option value="before">運用開始前（2024年10月13日まで）</option>
                <option value="after">運用開始後（2024年10月14日以降）</option>
            </select>
        </div>

        <div class="tab-navigation">
            <button class="tab-btn integrated active" onclick="switchTab('integrated')">
                📊 統合
            </button>
            <button class="tab-btn download" onclick="switchTab('download')">
                📥 ダウンロードのみ集計
            </button>
            <button class="tab-btn preview" onclick="switchTab('preview')">
                👁️ プレビューのみ集計
            </button>
        </div>
'''

    # Generate content for all 9 combinations (3 periods × 3 tabs)
    html += generate_all_period_content(d_all_int, d_all_dl, d_all_pv,
                                        d_before_int, d_before_dl, d_before_pv,
                                        d_after_int, d_after_dl, d_after_pv)

    # JavaScript for period/tab switching and charts
    html += generate_javascript(d_all_int, d_all_dl, d_all_pv,
                                 d_before_int, d_before_dl, d_before_pv,
                                 d_after_int, d_after_dl, d_after_pv)

    html += '''
    </div>
</body>
</html>'''

    return html


def generate_all_period_content(d_all_int, d_all_dl, d_all_pv,
                                 d_before_int, d_before_dl, d_before_pv,
                                 d_after_int, d_after_dl, d_after_pv):
    """Generate HTML content for all period/tab combinations."""

    html = ''

    # All period content
    html += f'''
        <!-- All Period -->
        <div id="period-all" class="content-wrapper active">
            {generate_tab_content('all', 'integrated', d_all_int, 'integrated', True)}
            {generate_tab_content('all', 'download', d_all_dl, 'download')}
            {generate_tab_content('all', 'preview', d_all_pv, 'preview')}
        </div>

        <!-- Before Period -->
        <div id="period-before" class="content-wrapper">
            {generate_tab_content('before', 'integrated', d_before_int, 'integrated', True)}
            {generate_tab_content('before', 'download', d_before_dl, 'download')}
            {generate_tab_content('before', 'preview', d_before_pv, 'preview')}
        </div>

        <!-- After Period -->
        <div id="period-after" class="content-wrapper">
            {generate_tab_content('after', 'integrated', d_after_int, 'integrated', True)}
            {generate_tab_content('after', 'download', d_after_dl, 'download')}
            {generate_tab_content('after', 'preview', d_after_pv, 'preview')}
        </div>
    '''

    return html


def generate_tab_content(period_key, tab_key, data, tab_type, is_active=False):
    """Generate content for a specific period/tab combination."""

    active_class = ' active' if is_active else ''

    if tab_type == 'integrated':
        return f'''
        <div id="tab-{period_key}-integrated" class="tab-content{active_class}">
            <div class="info-box">
                <strong>集計期間:</strong> {data['date_range']}
            </div>

            <div class="stats-grid">
                <div class="stat-card download">
                    <h3>総ダウンロード数</h3>
                    <div class="value">{data['total_dl']:,}</div>
                </div>
                <div class="stat-card preview">
                    <h3>総プレビュー数</h3>
                    <div class="value">{data['total_pv']:,}</div>
                </div>
                <div class="stat-card download">
                    <h3>DL ユニークユーザー</h3>
                    <div class="value">{data['unique_users_dl']}</div>
                </div>
                <div class="stat-card preview">
                    <h3>PV ユニークユーザー</h3>
                    <div class="value">{data['unique_users_pv']}</div>
                </div>
                <div class="stat-card">
                    <h3>ユニークファイル数</h3>
                    <div class="value">{data['unique_files']}</div>
                </div>
                <div class="stat-card">
                    <h3>DL/PV 比率</h3>
                    <div class="value" style="font-size: 1.5em;">{data['dl_ratio']} / {data['pv_ratio']}</div>
                </div>
            </div>

            <div class="chart-section">
                <h2>📈 月別推移</h2>
                <div class="chart-container">
                    <canvas id="chart-monthly-{period_key}"></canvas>
                </div>
            </div>

            <div class="chart-section">
                <h2>📅 日別推移（直近30日）</h2>
                <div class="chart-container">
                    <canvas id="chart-daily-{period_key}"></canvas>
                </div>
            </div>

            <div class="chart-section">
                <h2>🕐 時間帯別分布</h2>
                <div class="chart-container">
                    <canvas id="chart-hourly-{period_key}"></canvas>
                </div>
            </div>

            <div class="table-section">
                <h2>👥 トップユーザー（ダウンロード）</h2>
                <button class="toggle-btn" onclick="toggleTable('{period_key}-dl-users')">
                    全員表示/トップ10
                </button>
                <table id="table-{period_key}-dl-users">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ユーザー</th>
                            <th>ダウンロード数</th>
                            <th>ファイル数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_user_table_rows(data['top_users_dl'])}
                    </tbody>
                </table>
            </div>

            <div class="table-section">
                <h2>👥 トップユーザー（プレビュー）</h2>
                <button class="toggle-btn" onclick="toggleTable('{period_key}-pv-users')">
                    全員表示/トップ10
                </button>
                <table id="table-{period_key}-pv-users">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ユーザー</th>
                            <th>プレビュー数</th>
                            <th>ファイル数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_user_table_rows(data['top_users_pv'])}
                    </tbody>
                </table>
            </div>

            <div class="table-section">
                <h2>📄 トップファイル（ダウンロード）</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ファイル名</th>
                            <th>ダウンロード数</th>
                            <th>ユーザー数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_file_table_rows(data['top_files_dl'], 'DL')}
                    </tbody>
                </table>
            </div>

            <div class="table-section">
                <h2>📄 トップファイル（プレビュー）</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ファイル名</th>
                            <th>プレビュー数</th>
                            <th>ユーザー数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_file_table_rows(data['top_files_pv'], 'PV')}
                    </tbody>
                </table>
            </div>
        </div>
        '''

    elif tab_type == 'download':
        return f'''
        <div id="tab-{period_key}-download" class="tab-content{active_class}">
            <div class="info-box">
                <strong>集計期間:</strong> {data['date_range']}
            </div>

            <div class="stats-grid">
                <div class="stat-card download">
                    <h3>総ダウンロード数</h3>
                    <div class="value">{data['total']:,}</div>
                </div>
                <div class="stat-card download">
                    <h3>ユニークユーザー</h3>
                    <div class="value">{data['unique_users']}</div>
                </div>
                <div class="stat-card download">
                    <h3>ユニークファイル数</h3>
                    <div class="value">{data['unique_files']}</div>
                </div>
            </div>

            <div class="chart-section">
                <h2>📈 月別推移</h2>
                <div class="chart-container">
                    <canvas id="chart-monthly-dl-{period_key}"></canvas>
                </div>
            </div>

            <div class="chart-section">
                <h2>📅 日別推移（直近30日）</h2>
                <div class="chart-container">
                    <canvas id="chart-daily-dl-{period_key}"></canvas>
                </div>
            </div>

            <div class="chart-section">
                <h2>🕐 時間帯別分布</h2>
                <div class="chart-container">
                    <canvas id="chart-hourly-dl-{period_key}"></canvas>
                </div>
            </div>

            <div class="table-section">
                <h2>👥 トップユーザー</h2>
                <button class="toggle-btn" onclick="toggleTable('{period_key}-dl-only-users')">
                    全員表示/トップ10
                </button>
                <table id="table-{period_key}-dl-only-users">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ユーザー</th>
                            <th>ダウンロード数</th>
                            <th>ファイル数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_user_table_rows(data['top_users'])}
                    </tbody>
                </table>
            </div>

            <div class="table-section">
                <h2>📄 トップファイル</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ファイル名</th>
                            <th>ダウンロード数</th>
                            <th>ユーザー数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_file_table_rows(data['top_files'], 'DL')}
                    </tbody>
                </table>
            </div>
        </div>
        '''

    else:  # preview
        return f'''
        <div id="tab-{period_key}-preview" class="tab-content{active_class}">
            <div class="info-box">
                <strong>集計期間:</strong> {data['date_range']}
            </div>

            <div class="stats-grid">
                <div class="stat-card preview">
                    <h3>総プレビュー数</h3>
                    <div class="value">{data['total']:,}</div>
                </div>
                <div class="stat-card preview">
                    <h3>ユニークユーザー</h3>
                    <div class="value">{data['unique_users']}</div>
                </div>
                <div class="stat-card preview">
                    <h3>ユニークファイル数</h3>
                    <div class="value">{data['unique_files']}</div>
                </div>
            </div>

            <div class="chart-section">
                <h2>📈 月別推移</h2>
                <div class="chart-container">
                    <canvas id="chart-monthly-pv-{period_key}"></canvas>
                </div>
            </div>

            <div class="chart-section">
                <h2>📅 日別推移（直近30日）</h2>
                <div class="chart-container">
                    <canvas id="chart-daily-pv-{period_key}"></canvas>
                </div>
            </div>

            <div class="chart-section">
                <h2>🕐 時間帯別分布</h2>
                <div class="chart-container">
                    <canvas id="chart-hourly-pv-{period_key}"></canvas>
                </div>
            </div>

            <div class="table-section">
                <h2>👥 トップユーザー</h2>
                <button class="toggle-btn" onclick="toggleTable('{period_key}-pv-only-users')">
                    全員表示/トップ10
                </button>
                <table id="table-{period_key}-pv-only-users">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ユーザー</th>
                            <th>プレビュー数</th>
                            <th>ファイル数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_user_table_rows(data['top_users'])}
                    </tbody>
                </table>
            </div>

            <div class="table-section">
                <h2>📄 トップファイル</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>ファイル名</th>
                            <th>プレビュー数</th>
                            <th>ユーザー数</th>
                        </tr>
                    </thead>
                    <tbody>
                        {generate_file_table_rows(data['top_files'], 'PV')}
                    </tbody>
                </table>
            </div>
        </div>
        '''


def generate_user_table_rows(users):
    """Generate table rows for user data."""
    if not users:
        return '<tr><td colspan="4">データがありません</td></tr>'

    rows = ''
    for i, (user, count, files) in enumerate(users, 1):
        row_class = ' class="top-10"' if i <= 10 else ' class="rest"'
        rows += f'<tr{row_class}><td>{i}</td><td>{user}</td><td>{count:,}</td><td>{files}</td></tr>'

    return rows


def generate_file_table_rows(files, badge_type):
    """Generate table rows for file data."""
    if not files:
        return '<tr><td colspan="4">データがありません</td></tr>'

    rows = ''
    for i, (file_id, file_name, count, users) in enumerate(files, 1):
        badge = f'<span class="badge {badge_type.lower()}">{badge_type}</span>'
        rows += f'<tr><td>{i}</td><td>{file_name}{badge}</td><td>{count:,}</td><td>{users}</td></tr>'

    return rows


def generate_javascript(d_all_int, d_all_dl, d_all_pv,
                        d_before_int, d_before_dl, d_before_pv,
                        d_after_int, d_after_dl, d_after_pv):
    """Generate JavaScript for switching and charts."""

    # Simplified JavaScript for now - will expand with Chart.js initialization
    js = '''
    <script>
        let currentPeriod = 'all';
        let currentTab = 'integrated';
        let charts = {};

        function switchPeriod(period) {
            // Hide all period content
            document.querySelectorAll('.content-wrapper').forEach(el => {
                el.classList.remove('active');
            });

            // Show selected period
            document.getElementById('period-' + period).classList.add('active');

            // Update current period and initialize charts
            currentPeriod = period;
            initializeCharts();
        }

        function switchTab(tab) {
            // Update tab buttons
            document.querySelectorAll('.tab-btn').forEach(btn => {
                btn.classList.remove('active');
            });
            event.target.classList.add('active');

            // Hide all tab contents in current period
            document.querySelectorAll('#period-' + currentPeriod + ' .tab-content').forEach(el => {
                el.classList.remove('active');
            });

            // Show selected tab
            document.getElementById('tab-' + currentPeriod + '-' + tab).classList.add('active');

            currentTab = tab;
        }

        function toggleTable(tableId) {
            const table = document.getElementById('table-' + tableId);
            const rows = table.querySelectorAll('tbody tr.rest');

            rows.forEach(row => {
                row.style.display = row.style.display === 'none' ? '' : 'none';
            });
        }

        function initializeCharts() {
            // Destroy existing charts
            Object.values(charts).forEach(chart => {
                if (chart) chart.destroy();
            });
            charts = {};

            // Initialize charts based on current period
            if (currentPeriod === 'all') {
                createIntegratedCharts('all', ''' + d_all_int['monthly_labels'] + ''',
                    ''' + d_all_int['monthly_dl'] + ''', ''' + d_all_int['monthly_pv'] + ''',
                    ''' + d_all_int['daily_labels'] + ''', ''' + d_all_int['daily_dl'] + ''', ''' + d_all_int['daily_pv'] + ''',
                    ''' + d_all_int['hourly_labels'] + ''', ''' + d_all_int['hourly_dl'] + ''', ''' + d_all_int['hourly_pv'] + ''');
                createDownloadCharts('all', ''' + d_all_dl['monthly_labels'] + ''', ''' + d_all_dl['monthly_counts'] + ''',
                    ''' + d_all_dl['daily_labels'] + ''', ''' + d_all_dl['daily_counts'] + ''',
                    ''' + d_all_dl['hourly_labels'] + ''', ''' + d_all_dl['hourly_counts'] + ''');
                createPreviewCharts('all', ''' + d_all_pv['monthly_labels'] + ''', ''' + d_all_pv['monthly_counts'] + ''',
                    ''' + d_all_pv['daily_labels'] + ''', ''' + d_all_pv['daily_counts'] + ''',
                    ''' + d_all_pv['hourly_labels'] + ''', ''' + d_all_pv['hourly_counts'] + ''');
            } else if (currentPeriod === 'before') {
                createIntegratedCharts('before', ''' + d_before_int['monthly_labels'] + ''',
                    ''' + d_before_int['monthly_dl'] + ''', ''' + d_before_int['monthly_pv'] + ''',
                    ''' + d_before_int['daily_labels'] + ''', ''' + d_before_int['daily_dl'] + ''', ''' + d_before_int['daily_pv'] + ''',
                    ''' + d_before_int['hourly_labels'] + ''', ''' + d_before_int['hourly_dl'] + ''', ''' + d_before_int['hourly_pv'] + ''');
                createDownloadCharts('before', ''' + d_before_dl['monthly_labels'] + ''', ''' + d_before_dl['monthly_counts'] + ''',
                    ''' + d_before_dl['daily_labels'] + ''', ''' + d_before_dl['daily_counts'] + ''',
                    ''' + d_before_dl['hourly_labels'] + ''', ''' + d_before_dl['hourly_counts'] + ''');
                createPreviewCharts('before', ''' + d_before_pv['monthly_labels'] + ''', ''' + d_before_pv['monthly_counts'] + ''',
                    ''' + d_before_pv['daily_labels'] + ''', ''' + d_before_pv['daily_counts'] + ''',
                    ''' + d_before_pv['hourly_labels'] + ''', ''' + d_before_pv['hourly_counts'] + ''');
            } else {
                createIntegratedCharts('after', ''' + d_after_int['monthly_labels'] + ''',
                    ''' + d_after_int['monthly_dl'] + ''', ''' + d_after_int['monthly_pv'] + ''',
                    ''' + d_after_int['daily_labels'] + ''', ''' + d_after_int['daily_dl'] + ''', ''' + d_after_int['daily_pv'] + ''',
                    ''' + d_after_int['hourly_labels'] + ''', ''' + d_after_int['hourly_dl'] + ''', ''' + d_after_int['hourly_pv'] + ''');
                createDownloadCharts('after', ''' + d_after_dl['monthly_labels'] + ''', ''' + d_after_dl['monthly_counts'] + ''',
                    ''' + d_after_dl['daily_labels'] + ''', ''' + d_after_dl['daily_counts'] + ''',
                    ''' + d_after_dl['hourly_labels'] + ''', ''' + d_after_dl['hourly_counts'] + ''');
                createPreviewCharts('after', ''' + d_after_pv['monthly_labels'] + ''', ''' + d_after_pv['monthly_counts'] + ''',
                    ''' + d_after_pv['daily_labels'] + ''', ''' + d_after_pv['daily_counts'] + ''',
                    ''' + d_after_pv['hourly_labels'] + ''', ''' + d_after_pv['hourly_counts'] + ''');
            }
        }

        function createIntegratedCharts(period, monthlyLabels, monthlyDL, monthlyPV,
                                        dailyLabels, dailyDL, dailyPV,
                                        hourlyLabels, hourlyDL, hourlyPV) {
            // Monthly chart
            const monthlyCtx = document.getElementById('chart-monthly-' + period);
            if (monthlyCtx) {
                charts['monthly-' + period] = new Chart(monthlyCtx, {
                    type: 'bar',
                    data: {
                        labels: monthlyLabels,
                        datasets: [
                            {
                                label: 'ダウンロード',
                                data: monthlyDL,
                                backgroundColor: 'rgba(76, 175, 80, 0.6)',
                                borderColor: 'rgba(76, 175, 80, 1)',
                                borderWidth: 1
                            },
                            {
                                label: 'プレビュー',
                                data: monthlyPV,
                                backgroundColor: 'rgba(255, 152, 0, 0.6)',
                                borderColor: 'rgba(255, 152, 0, 1)',
                                borderWidth: 1
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { beginAtZero: true }
                        }
                    }
                });
            }

            // Daily chart
            const dailyCtx = document.getElementById('chart-daily-' + period);
            if (dailyCtx) {
                charts['daily-' + period] = new Chart(dailyCtx, {
                    type: 'line',
                    data: {
                        labels: dailyLabels,
                        datasets: [
                            {
                                label: 'ダウンロード',
                                data: dailyDL,
                                borderColor: 'rgba(76, 175, 80, 1)',
                                backgroundColor: 'rgba(76, 175, 80, 0.1)',
                                tension: 0.4
                            },
                            {
                                label: 'プレビュー',
                                data: dailyPV,
                                borderColor: 'rgba(255, 152, 0, 1)',
                                backgroundColor: 'rgba(255, 152, 0, 0.1)',
                                tension: 0.4
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { beginAtZero: true }
                        }
                    }
                });
            }

            // Hourly chart
            const hourlyCtx = document.getElementById('chart-hourly-' + period);
            if (hourlyCtx) {
                charts['hourly-' + period] = new Chart(hourlyCtx, {
                    type: 'bar',
                    data: {
                        labels: hourlyLabels,
                        datasets: [
                            {
                                label: 'ダウンロード',
                                data: hourlyDL,
                                backgroundColor: 'rgba(76, 175, 80, 0.6)',
                                borderColor: 'rgba(76, 175, 80, 1)',
                                borderWidth: 1
                            },
                            {
                                label: 'プレビュー',
                                data: hourlyPV,
                                backgroundColor: 'rgba(255, 152, 0, 0.6)',
                                borderColor: 'rgba(255, 152, 0, 1)',
                                borderWidth: 1
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: {
                            y: { beginAtZero: true }
                        }
                    }
                });
            }
        }

        function createDownloadCharts(period, monthlyLabels, monthlyData,
                                      dailyLabels, dailyData,
                                      hourlyLabels, hourlyData) {
            // Monthly
            const monthlyCtx = document.getElementById('chart-monthly-dl-' + period);
            if (monthlyCtx) {
                charts['monthly-dl-' + period] = new Chart(monthlyCtx, {
                    type: 'bar',
                    data: {
                        labels: monthlyLabels,
                        datasets: [{
                            label: 'ダウンロード数',
                            data: monthlyData,
                            backgroundColor: 'rgba(76, 175, 80, 0.6)',
                            borderColor: 'rgba(76, 175, 80, 1)',
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }

            // Daily
            const dailyCtx = document.getElementById('chart-daily-dl-' + period);
            if (dailyCtx) {
                charts['daily-dl-' + period] = new Chart(dailyCtx, {
                    type: 'line',
                    data: {
                        labels: dailyLabels,
                        datasets: [{
                            label: 'ダウンロード数',
                            data: dailyData,
                            borderColor: 'rgba(76, 175, 80, 1)',
                            backgroundColor: 'rgba(76, 175, 80, 0.1)',
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }

            // Hourly
            const hourlyCtx = document.getElementById('chart-hourly-dl-' + period);
            if (hourlyCtx) {
                charts['hourly-dl-' + period] = new Chart(hourlyCtx, {
                    type: 'bar',
                    data: {
                        labels: hourlyLabels,
                        datasets: [{
                            label: 'ダウンロード数',
                            data: hourlyData,
                            backgroundColor: 'rgba(76, 175, 80, 0.6)',
                            borderColor: 'rgba(76, 175, 80, 1)',
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }
        }

        function createPreviewCharts(period, monthlyLabels, monthlyData,
                                     dailyLabels, dailyData,
                                     hourlyLabels, hourlyData) {
            // Monthly
            const monthlyCtx = document.getElementById('chart-monthly-pv-' + period);
            if (monthlyCtx) {
                charts['monthly-pv-' + period] = new Chart(monthlyCtx, {
                    type: 'bar',
                    data: {
                        labels: monthlyLabels,
                        datasets: [{
                            label: 'プレビュー数',
                            data: monthlyData,
                            backgroundColor: 'rgba(255, 152, 0, 0.6)',
                            borderColor: 'rgba(255, 152, 0, 1)',
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }

            // Daily
            const dailyCtx = document.getElementById('chart-daily-pv-' + period);
            if (dailyCtx) {
                charts['daily-pv-' + period] = new Chart(dailyCtx, {
                    type: 'line',
                    data: {
                        labels: dailyLabels,
                        datasets: [{
                            label: 'プレビュー数',
                            data: dailyData,
                            borderColor: 'rgba(255, 152, 0, 1)',
                            backgroundColor: 'rgba(255, 152, 0, 0.1)',
                            tension: 0.4
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }

            // Hourly
            const hourlyCtx = document.getElementById('chart-hourly-pv-' + period);
            if (hourlyCtx) {
                charts['hourly-pv-' + period] = new Chart(hourlyCtx, {
                    type: 'bar',
                    data: {
                        labels: hourlyLabels,
                        datasets: [{
                            label: 'プレビュー数',
                            data: hourlyData,
                            backgroundColor: 'rgba(255, 152, 0, 0.6)',
                            borderColor: 'rgba(255, 152, 0, 1)',
                            borderWidth: 1
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        scales: { y: { beginAtZero: true } }
                    }
                });
            }
        }

        // Initialize tables (hide extra rows by default)
        document.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('tbody tr.rest').forEach(row => {
                row.style.display = 'none';
            });

            // Initialize charts for default period
            initializeCharts();
        });
    </script>
    '''

    return js


if __name__ == '__main__':
    generate_dashboard()
