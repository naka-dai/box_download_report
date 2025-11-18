"""
Test Period Filter Implementation
期間フィルター機能のテスト実装
"""

import sqlite3
import json
from pathlib import Path


def collect_data_for_period(cursor, admin_params, placeholders, period_filter='all'):
    """Collect data for a specific period."""

    # Build WHERE clause for period
    if period_filter == 'old':  # ~2024/10/13
        period_clause = "AND DATE(download_at_jst) <= '2024-10-13'"
    elif period_filter == 'new':  # 2024/10/14~
        period_clause = "AND DATE(download_at_jst) >= '2024-10-14'"
    else:  # all
        period_clause = ""

    # Get summary statistics
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

    # Monthly data
    cursor.execute(f'''
        SELECT strftime('%Y-%m', download_at_jst) as month,
               SUM(CASE WHEN event_type = "DOWNLOAD" THEN 1 ELSE 0 END) as dl,
               SUM(CASE WHEN event_type = "PREVIEW" THEN 1 ELSE 0 END) as pv
        FROM downloads
        WHERE user_login NOT IN ({placeholders}) {period_clause}
        GROUP BY month ORDER BY month
    ''', admin_params)
    monthly_data = cursor.fetchall()

    total = total_downloads + total_previews
    dl_ratio = (total_downloads / total * 100) if total > 0 else 0
    pv_ratio = (total_previews / total * 100) if total > 0 else 0

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
        'monthly_labels': [row[0] for row in monthly_data],
        'monthly_downloads': [row[1] for row in monthly_data],
        'monthly_previews': [row[2] for row in monthly_data]
    }


def generate_test_dashboard():
    """Generate test dashboard with period filter."""

    print("期間フィルター機能テスト...")

    # Connect to database
    db_path = r"data\box_audit.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Admin exclusion
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

    print("  全期間のデータを収集中...")
    data_all = collect_data_for_period(cursor, admin_params, placeholders, 'all')

    print("  旧運用期間のデータを収集中...")
    data_old = collect_data_for_period(cursor, admin_params, placeholders, 'old')

    print("  新運用期間のデータを収集中...")
    data_new = collect_data_for_period(cursor, admin_params, placeholders, 'new')

    conn.close()

    print(f"  データ収集完了")
    print(f"    全期間: DL={data_all['total_downloads']:,}, PV={data_all['total_previews']:,}")
    print(f"    旧運用: DL={data_old['total_downloads']:,}, PV={data_old['total_previews']:,}")
    print(f"    新運用: DL={data_new['total_downloads']:,}, PV={data_new['total_previews']:,}")

    # Generate simple HTML with period switcher
    output_path = r"data\dashboard_period_test.html"

    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>期間フィルター機能テスト</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            padding: 20px;
            background: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #667eea;
        }}
        .period-selector {{
            margin: 20px 0;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
        }}
        select {{
            padding: 10px 15px;
            font-size: 1em;
            border-radius: 5px;
            border: 2px solid #667eea;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .stat-card {{
            padding: 20px;
            background: #f8f9fa;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card h3 {{
            margin: 0 0 10px 0;
            color: #666;
            font-size: 0.9em;
        }}
        .stat-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }}
        .period-content {{
            display: none;
        }}
        .period-content.active {{
            display: block;
        }}
        .info {{
            padding: 10px;
            background: #e3f2fd;
            border-left: 4px solid #2196f3;
            margin: 10px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 期間フィルター機能テスト</h1>

        <div class="period-selector">
            <label for="period-select"><strong>集計期間を選択:</strong></label>
            <select id="period-select" onchange="switchPeriod(this.value)">
                <option value="all">全期間</option>
                <option value="old">2024年10月13日まで（旧運用）</option>
                <option value="new">2024年10月14日以降（新運用）</option>
            </select>
        </div>

        <!-- All Period -->
        <div id="period-all" class="period-content active">
            <div class="info">
                <strong>期間:</strong> {data_all['min_date']} ～ {data_all['max_date']}
            </div>
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>総ダウンロード数</h3>
                    <div class="value">{data_all['total_downloads']:,}</div>
                </div>
                <div class="stat-card">
                    <h3>総プレビュー数</h3>
                    <div class="value">{data_all['total_previews']:,}</div>
                </div>
                <div class="stat-card">
                    <h3>DLユニーク数</h3>
                    <div class="value">{data_all['unique_users_dl']}</div>
                </div>
                <div class="stat-card">
                    <h3>PVユニーク数</h3>
                    <div class="value">{data_all['unique_users_pv']}</div>
                </div>
                <div class="stat-card">
                    <h3>ファイル数</h3>
                    <div class="value">{data_all['unique_files']}</div>
                </div>
                <div class="stat-card">
                    <h3>DL/PV比率</h3>
                    <div class="value" style="font-size: 1.2em;">{data_all['dl_ratio']:.0f}% / {data_all['pv_ratio']:.0f}%</div>
                </div>
            </div>
            <p><strong>月次データ:</strong> {len(data_all['monthly_labels'])}ヶ月分</p>
        </div>

        <!-- Old Period -->
        <div id="period-old" class="period-content">
            <div class="info">
                <strong>期間:</strong> {data_old['min_date']} ～ {data_old['max_date']}<br>
                <strong>運用:</strong> 旧運用期間
            </div>
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>総ダウンロード数</h3>
                    <div class="value">{data_old['total_downloads']:,}</div>
                </div>
                <div class="stat-card">
                    <h3>総プレビュー数</h3>
                    <div class="value">{data_old['total_previews']:,}</div>
                </div>
                <div class="stat-card">
                    <h3>DLユニーク数</h3>
                    <div class="value">{data_old['unique_users_dl']}</div>
                </div>
                <div class="stat-card">
                    <h3>PVユニーク数</h3>
                    <div class="value">{data_old['unique_users_pv']}</div>
                </div>
                <div class="stat-card">
                    <h3>ファイル数</h3>
                    <div class="value">{data_old['unique_files']}</div>
                </div>
                <div class="stat-card">
                    <h3>DL/PV比率</h3>
                    <div class="value" style="font-size: 1.2em;">{data_old['dl_ratio']:.0f}% / {data_old['pv_ratio']:.0f}%</div>
                </div>
            </div>
            <p><strong>月次データ:</strong> {len(data_old['monthly_labels'])}ヶ月分</p>
        </div>

        <!-- New Period -->
        <div id="period-new" class="period-content">
            <div class="info">
                <strong>期間:</strong> {data_new['min_date']} ～ {data_new['max_date']}<br>
                <strong>運用:</strong> 新運用期間
            </div>
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>総ダウンロード数</h3>
                    <div class="value">{data_new['total_downloads']:,}</div>
                </div>
                <div class="stat-card">
                    <h3>総プレビュー数</h3>
                    <div class="value">{data_new['total_previews']:,}</div>
                </div>
                <div class="stat-card">
                    <h3>DLユニーク数</h3>
                    <div class="value">{data_new['unique_users_dl']}</div>
                </div>
                <div class="stat-card">
                    <h3>PVユニーク数</h3>
                    <div class="value">{data_new['unique_users_pv']}</div>
                </div>
                <div class="stat-card">
                    <h3>ファイル数</h3>
                    <div class="value">{data_new['unique_files']}</div>
                </div>
                <div class="stat-card">
                    <h3>DL/PV比率</h3>
                    <div class="value" style="font-size: 1.2em;">{data_new['dl_ratio']:.0f}% / {data_new['pv_ratio']:.0f}%</div>
                </div>
            </div>
            <p><strong>月次データ:</strong> {len(data_new['monthly_labels'])}ヶ月分</p>
        </div>

        <hr style="margin: 30px 0;">
        <p style="text-align: center; color: #999;">
            期間フィルター機能のテスト版<br>
            正常に動作することを確認したら、全ダッシュボードに実装します
        </p>
    </div>

    <script>
        function switchPeriod(period) {{
            // Hide all period content
            document.querySelectorAll('.period-content').forEach(el => {{
                el.classList.remove('active');
            }});

            // Show selected period
            document.getElementById('period-' + period).classList.add('active');
        }}
    </script>
</body>
</html>'''

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n[OK] テスト版ダッシュボード生成完了: {output_path}")
    print(f"     file:///{output_path.replace(chr(92), '/')}")
    return output_path


if __name__ == '__main__':
    generate_test_dashboard()
