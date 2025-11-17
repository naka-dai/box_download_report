"""
Check Operations in Database
データベース内の操作タイプを確認
"""

import sqlite3
import json

db_path = r"data\box_audit.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all operations
cursor.execute("SELECT raw_json FROM downloads LIMIT 1000")
operations = {}

for (raw_json,) in cursor.fetchall():
    if raw_json:
        try:
            data = json.loads(raw_json)
            op = data.get('operation', 'unknown')
            operations[op] = operations.get(op, 0) + 1
        except:
            pass

print("操作タイプ別件数:")
print("=" * 60)
for op, count in sorted(operations.items(), key=lambda x: x[1], reverse=True):
    print(f"{op}: {count:,} 件")

# Get total counts for all operations
cursor.execute("SELECT COUNT(*) FROM downloads")
total = cursor.fetchone()[0]
print(f"\n総レコード数: {total:,} 件")

# Count preview operations
cursor.execute("""
    SELECT COUNT(*)
    FROM downloads
    WHERE raw_json LIKE '%プレビュー%'
""")
preview_count = cursor.fetchone()[0]
print(f"プレビュー操作: {preview_count:,} 件")

# Get sample preview records
cursor.execute("""
    SELECT user_name, file_name, raw_json
    FROM downloads
    WHERE raw_json LIKE '%プレビュー%'
    LIMIT 5
""")
print("\nプレビュー操作サンプル:")
print("-" * 60)
for user_name, file_name, raw_json in cursor.fetchall():
    data = json.loads(raw_json)
    print(f"ユーザー: {user_name}")
    print(f"ファイル: {file_name}")
    print(f"操作: {data.get('operation', 'unknown')}")
    print("-" * 60)

conn.close()
