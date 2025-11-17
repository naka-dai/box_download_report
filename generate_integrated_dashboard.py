"""
Generate Integrated Dashboard
ダウンロードとプレビューを統合したダッシュボードを生成
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

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    unique_users = cursor.fetchone()[0]

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

    # Get daily statistics for both types (last 30 days)
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
    daily_data = list(reversed(cursor.fetchall()))

    # Get hourly statistics for both types
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
    hourly_data = cursor.fetchall()

    # Get top users by total activity (download + preview)
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
        LIMIT 10
    ''', admin_params)
    top_users = cursor.fetchall()

    # Get top files by total activity
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

    # Get folder information for top files
    top_files = []
    for file_id, file_name, raw_json, dl_count, pv_count, total, users in top_files_raw:
        folder = ''
        if raw_json:
            try:
                data = json.loads(raw_json)
                folder = data.get('parent_folder', '')
            except:
                pass
        top_files.append((file_name, folder, dl_count, pv_count, total, users))

    # Get event type distribution
    cursor.execute(f'''
        SELECT
            event_type,
            COUNT(*) as count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY event_type
    ''', admin_params)
    event_distribution = cursor.fetchall()

    conn.close()

    # Prepare data for charts
    monthly_labels = [row[0] for row in monthly_data]
    monthly_downloads = [row[1] for row in monthly_data]
    monthly_previews = [row[2] for row in monthly_data]

    daily_labels = [row[0] for row in daily_data]
    daily_downloads = [row[1] for row in daily_data]
    daily_previews = [row[2] for row in daily_data]

    hourly_labels = [f"{row[0]:02d}:00" for row in hourly_data]
    hourly_downloads = [row[1] for row in hourly_data]
    hourly_previews = [row[2] for row in hourly_data]

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
            <div class="stat-card">
                <h3>ユニークユーザー</h3>
                <div class="value">{unique_users}</div>
            </div>
            <div class="stat-card">
                <h3>アクセスファイル数</h3>
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
            <h2>👥 トップ10ユーザー（総アクセス数）</h2>
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
                    </tr>
                </thead>
                <tbody>
'''

    for i, (name, email, dl_count, pv_count, total, files) in enumerate(top_users, 1):
        html += f'''                    <tr>
                        <td><span class="rank">{i}</span></td>
                        <td>{name}</td>
                        <td>{email}</td>
                        <td style="text-align: right;"><span class="badge download">{dl_count:,}</span></td>
                        <td style="text-align: right;"><span class="badge preview">{pv_count:,}</span></td>
                        <td style="text-align: right; font-weight: bold;">{total:,}</td>
                        <td style="text-align: right;">{files:,}</td>
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

    for i, (file_name, folder, dl_count, pv_count, total, users) in enumerate(top_files, 1):
        html += f'''                    <tr>
                        <td><span class="rank">{i}</span></td>
                        <td>{file_name}</td>
                        <td style="font-size: 0.9em; color: #666;">{folder}</td>
                        <td style="text-align: right;"><span class="badge download">{dl_count:,}</span></td>
                        <td style="text-align: right;"><span class="badge preview">{pv_count:,}</span></td>
                        <td style="text-align: right; font-weight: bold;">{total:,}</td>
                        <td style="text-align: right;">{users}</td>
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

    <script>
        // Monthly Chart
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
                plugins: {{
                    legend: {{
                        display: false
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

        // Daily Chart
        const dailyCtx = document.getElementById('dailyChart').getContext('2d');
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
                }}
            }}
        }});

        // Hourly Chart
        const hourlyCtx = document.getElementById('hourlyChart').getContext('2d');
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
