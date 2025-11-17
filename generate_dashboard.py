"""
Generate HTML Dashboard
SQLiteデータベースから統計情報を取得してHTMLダッシュボードを生成
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path

def generate_dashboard():
    """Generate HTML dashboard from database statistics."""

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

    # Get summary statistics
    cursor.execute(f'SELECT COUNT(*) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    total_downloads = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT user_login) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    unique_users = cursor.fetchone()[0]

    cursor.execute(f'SELECT COUNT(DISTINCT file_id) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    unique_files = cursor.fetchone()[0]

    cursor.execute(f'SELECT MIN(download_at_jst), MAX(download_at_jst) FROM downloads WHERE user_login NOT IN ({placeholders})', admin_params)
    min_date, max_date = cursor.fetchone()

    # Get monthly statistics
    cursor.execute(f'''
        SELECT
            strftime('%Y-%m', download_at_jst) as month,
            COUNT(*) as download_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY month
        ORDER BY month
    ''', admin_params)
    monthly_data = cursor.fetchall()

    # Get top 10 users
    cursor.execute(f'''
        SELECT
            user_name,
            user_login,
            COUNT(*) as download_count,
            COUNT(DISTINCT file_id) as unique_files
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY user_login
        ORDER BY download_count DESC
        LIMIT 10
    ''', admin_params)
    top_users = cursor.fetchall()

    # Get top 10 files
    cursor.execute(f'''
        SELECT
            file_name,
            raw_json,
            COUNT(*) as download_count,
            COUNT(DISTINCT user_login) as unique_users
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY file_id
        ORDER BY download_count DESC
        LIMIT 10
    ''', admin_params)
    top_files = cursor.fetchall()

    # Get hourly statistics
    cursor.execute(f'''
        SELECT
            CAST(strftime('%H', download_at_jst) AS INTEGER) as hour,
            COUNT(*) as download_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY hour
        ORDER BY hour
    ''', admin_params)
    hourly_data = cursor.fetchall()

    # Get daily statistics (last 30 days)
    cursor.execute(f'''
        SELECT
            DATE(download_at_jst) as date,
            COUNT(*) as download_count
        FROM downloads
        WHERE user_login NOT IN ({placeholders})
        GROUP BY DATE(download_at_jst)
        ORDER BY date DESC
        LIMIT 30
    ''', admin_params)
    daily_data = list(reversed(cursor.fetchall()))

    conn.close()

    # Prepare data for charts
    monthly_labels = [row[0] for row in monthly_data]
    monthly_values = [row[1] for row in monthly_data]

    hourly_labels = [f"{row[0]:02d}:00" for row in hourly_data]
    hourly_values = [row[1] for row in hourly_data]

    daily_labels = [row[0] for row in daily_data]
    daily_values = [row[1] for row in daily_data]

    # Generate HTML
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Box ダウンロードレポート ダッシュボード</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
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
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
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
            <h1>📊 Box ダウンロードレポート</h1>
            <p>図面001フォルダ ダウンロード分析ダッシュボード</p>
            <p style="font-size: 0.9em; color: #999; margin-top: 10px;">
                期間: {min_date} ～ {max_date}
            </p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>総ダウンロード数</h3>
                <div class="value">{total_downloads:,}</div>
            </div>
            <div class="stat-card">
                <h3>ユニークユーザー</h3>
                <div class="value">{unique_users}</div>
            </div>
            <div class="stat-card">
                <h3>ダウンロードファイル数</h3>
                <div class="value">{unique_files:,}</div>
            </div>
            <div class="stat-card">
                <h3>管理者除外数</h3>
                <div class="value">{len(admin_emails)}</div>
            </div>
        </div>

        <div class="chart-grid">
            <div class="chart-card">
                <h2>📈 月別ダウンロード推移</h2>
                <div class="chart-container">
                    <canvas id="monthlyChart"></canvas>
                </div>
            </div>

            <div class="chart-card">
                <h2>📅 日別ダウンロード推移（直近30日）</h2>
                <div class="chart-container">
                    <canvas id="dailyChart"></canvas>
                </div>
            </div>
        </div>

        <div class="chart-card" style="margin-bottom: 30px;">
            <h2>🕐 時間帯別ダウンロード数</h2>
            <div class="chart-container" style="height: 250px;">
                <canvas id="hourlyChart"></canvas>
            </div>
        </div>

        <div class="table-card">
            <h2>👥 トップ10ユーザー</h2>
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
'''

    for i, (name, email, count, files) in enumerate(top_users, 1):
        html += f'''                    <tr>
                        <td><span class="rank">{i}</span></td>
                        <td>{name}</td>
                        <td>{email}</td>
                        <td style="text-align: right; font-weight: bold;">{count:,}</td>
                        <td style="text-align: right;">{files:,}</td>
                    </tr>
'''

    html += '''                </tbody>
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
                        <th style="text-align: right;">ユニークユーザー</th>
                    </tr>
                </thead>
                <tbody>
'''

    for i, (file_name, raw_json, count, users) in enumerate(top_files, 1):
        folder = ''
        if raw_json:
            try:
                data = json.loads(raw_json)
                folder = data.get('parent_folder', '')
            except:
                pass

        html += f'''                    <tr>
                        <td><span class="rank">{i}</span></td>
                        <td>{file_name}</td>
                        <td style="font-size: 0.9em; color: #666;">{folder}</td>
                        <td style="text-align: right; font-weight: bold;">{count:,}</td>
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
                datasets: [{{
                    label: 'ダウンロード数',
                    data: {json.dumps(monthly_values)},
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2
                }}]
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
                                size: 11
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
                datasets: [{{
                    label: 'ダウンロード数',
                    data: {json.dumps(daily_values)},
                    borderColor: 'rgba(118, 75, 162, 1)',
                    backgroundColor: 'rgba(118, 75, 162, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4
                }}]
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
                datasets: [{{
                    label: 'ダウンロード数',
                    data: {json.dumps(hourly_values)},
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2
                }}]
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
                                size: 11
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
    output_path = r"data\dashboard.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Dashboard generated: {output_path}")
    return output_path


if __name__ == '__main__':
    output_path = generate_dashboard()
    print(f"\n✓ ダッシュボードを生成しました: {output_path}")
    print(f"\nブラウザで開いてください:")
    print(f"  file:///{output_path.replace(chr(92), '/')}")
