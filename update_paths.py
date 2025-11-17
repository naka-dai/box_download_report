"""
Update all hardcoded paths from C:\\box_reports to project data directory
"""
import os
import re
from pathlib import Path

# Project root
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"

# Files to update
python_files = [
    "generate_dashboard.py",
    "generate_dashboard_v2.py",
    "generate_preview_dashboard.py",
    "import_preview_data.py",
    "check_operations.py",
    "csv_importer.py",
    "db.py",
    "main.py",
    "analyze_downloads.py"
]

# Replacement patterns
replacements = [
    (r'r"C:\\box_reports\\box_audit\.db"', r'r"data\\box_audit.db"'),
    (r"r'C:\\\\box_reports\\\\box_audit\\.db'", r"r'data\\box_audit.db'"),
    (r'r"C:\\box_reports"', r'r"data"'),
    (r"r'C:\\\\box_reports'", r"r'data'"),
    (r'C:\\box_reports\\dashboard\.html', r'data\\dashboard.html'),
    (r'C:\\box_reports\\dashboard_preview\.html', r'data\\dashboard_preview.html'),
    (r'"C:\\\\box_reports\\\\', r'"data\\'),
]

for py_file in python_files:
    file_path = PROJECT_ROOT / py_file
    if not file_path.exists():
        print(f"⚠ Skip: {py_file} (not found)")
        continue

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content

    # Apply replacements
    for pattern, replacement in replacements:
        content = re.sub(pattern, replacement, content)

    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[OK] Updated: {py_file}")
    else:
        print(f"  No change: {py_file}")

print("\n[OK] Path update complete")
