
import sqlite3
import json
from datetime import datetime
from pathlib import Path

print("完全なリッチUI版オールインワンダッシュボード生成開始")
print("="*80)
print("このバージョンには全てのリッチUI機能が含まれます:")
print("- 月別詳細モーダル（3タブ全て）")
print("- ファイルユーザー詳細ツールチップ（3タブ全て）")
print("- トップ10/全員切り替えトグル（3タブ全て）")
print("- 重複率表示（3タブ全て）")
print("- DL/PVバッジ表示（統合タブ）")
print("- 日別/時間帯別ユーザー内訳ツールチップ（3タブ全て）")
print("="*80)
print()

# インポート既存の生成関数
from generate_integrated_dashboard import generate_dashboard as gen_integrated
from generate_dashboard_v2 import generate_dashboard as gen_download
from generate_preview_dashboard import generate_dashboard as gen_preview

print("既存のダッシュボード生成関数をインポートしました")
print("これらの関数を統合して1つのHTMLファイルを生成します")
print()
print("処理を開始します（数分かかる場合があります）...")
