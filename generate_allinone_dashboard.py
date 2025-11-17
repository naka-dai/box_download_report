"""
Generate All-in-One Dashboard
統合・ダウンロード・プレビューの3つのダッシュボードを1つのHTMLに統合
タブ切り替えで各ビューを表示
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

    print(f"Total Downloads: {total_downloads:,}")
    print(f"Total Previews: {total_previews:,}")
    print(f"Generating all-in-one dashboard...")

    conn.close()

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

        .stats-summary {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}

        .stats-summary h2 {{
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.5em;
        }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }}

        .summary-item {{
            text-align: center;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 10px;
        }}

        .summary-item h3 {{
            color: #888;
            font-size: 0.85em;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .summary-item .value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}

        .summary-item.download .value {{
            color: #4CAF50;
        }}

        .summary-item.preview .value {{
            color: #FF9800;
        }}

        .info-message {{
            background: #e3f2fd;
            border-left: 4px solid #2196F3;
            padding: 20px;
            border-radius: 8px;
            margin: 30px auto;
            max-width: 800px;
            text-align: center;
        }}

        .info-message h3 {{
            color: #1976D2;
            margin-bottom: 10px;
        }}

        .info-message p {{
            color: #555;
            line-height: 1.6;
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
            <div class="stats-summary">
                <h2>📊 統合サマリー</h2>
                <div class="summary-grid">
                    <div class="summary-item download">
                        <h3>総ダウンロード数</h3>
                        <div class="value">{total_downloads:,}</div>
                    </div>
                    <div class="summary-item preview">
                        <h3>総プレビュー数</h3>
                        <div class="value">{total_previews:,}</div>
                    </div>
                    <div class="summary-item">
                        <h3>総アクセス数</h3>
                        <div class="value">{total_access:,}</div>
                    </div>
                    <div class="summary-item download">
                        <h3>DLユニーク数</h3>
                        <div class="value">{unique_users_download}</div>
                    </div>
                    <div class="summary-item preview">
                        <h3>PVユニーク数</h3>
                        <div class="value">{unique_users_preview}</div>
                    </div>
                    <div class="summary-item">
                        <h3>ファイル数</h3>
                        <div class="value">{unique_files:,}</div>
                    </div>
                    <div class="summary-item">
                        <h3>DL比率 / PV比率</h3>
                        <div class="value" style="font-size: 1.3em;">{download_ratio:.0f}% / {preview_ratio:.0f}%</div>
                    </div>
                </div>
            </div>

            <div class="info-message">
                <h3>📌 統合ダッシュボードについて</h3>
                <p>
                    ダウンロードとプレビューの統合分析を表示します。<br>
                    詳細なグラフや分析は、個別のダッシュボードファイル（dashboard_integrated.html）をご覧ください。
                </p>
            </div>
        </div>

        <!-- Download Tab -->
        <div id="download-tab" class="tab-content">
            <div class="stats-summary">
                <h2>📥 ダウンロードサマリー</h2>
                <div class="summary-grid">
                    <div class="summary-item download">
                        <h3>総ダウンロード数</h3>
                        <div class="value">{total_downloads:,}</div>
                    </div>
                    <div class="summary-item download">
                        <h3>ユニークユーザー</h3>
                        <div class="value">{unique_users_download}</div>
                    </div>
                    <div class="summary-item">
                        <h3>ファイル数</h3>
                        <div class="value">{unique_files:,}</div>
                    </div>
                </div>
            </div>

            <div class="info-message">
                <h3>📌 ダウンロード専用ダッシュボードについて</h3>
                <p>
                    ファイルダウンロードのみの詳細分析を表示します。<br>
                    月別推移、日別推移、時間帯別分析、トップユーザー/ファイルなどの詳細は、<br>
                    個別のダッシュボードファイル（dashboard.html）をご覧ください。
                </p>
            </div>
        </div>

        <!-- Preview Tab -->
        <div id="preview-tab" class="tab-content">
            <div class="stats-summary">
                <h2>👁️ プレビューサマリー</h2>
                <div class="summary-grid">
                    <div class="summary-item preview">
                        <h3>総プレビュー数</h3>
                        <div class="value">{total_previews:,}</div>
                    </div>
                    <div class="summary-item preview">
                        <h3>ユニークユーザー</h3>
                        <div class="value">{unique_users_preview}</div>
                    </div>
                    <div class="summary-item">
                        <h3>プレビューファイル数</h3>
                        <div class="value">{unique_files:,}</div>
                    </div>
                </div>
            </div>

            <div class="info-message">
                <h3>📌 プレビュー専用ダッシュボードについて</h3>
                <p>
                    ファイルプレビューのみの詳細分析を表示します。<br>
                    月別推移、日別推移、時間帯別分析、トップユーザー/ファイルなどの詳細は、<br>
                    個別のダッシュボードファイル（dashboard_preview.html）をご覧ください。
                </p>
            </div>
        </div>

        <div class="footer">
            <p>🤖 Generated with Claude Code</p>
            <p style="font-size: 0.9em; margin-top: 5px;">
                {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')} に生成
            </p>
            <p style="font-size: 0.85em; margin-top: 10px; opacity: 0.8;">
                オールインワン版 - 詳細分析は個別ダッシュボードをご利用ください
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
