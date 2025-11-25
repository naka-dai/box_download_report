# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['box_daily_update.py'],
    pathex=[],
    binaries=[],
    datas=[('chart.js', '.')],
    hiddenimports=['boxsdk.object.recent_item', 'update_netlify_dashboard', 'csv_importer', 'csv_downloader'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='box_daily_update',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
