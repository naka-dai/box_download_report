# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['box_daily_update.py'],
    pathex=[],
    binaries=[],
    datas=[('chart.js', '.'), ('update_netlify_dashboard.py', '.'), ('update_cloudflare_dashboard.py', '.')],
    hiddenimports=['boxsdk.object.cloneable', 'boxsdk.object.api_json_object', 'boxsdk.object.base_api_json_object', 'boxsdk.object.base_object', 'boxsdk.object.item', 'boxsdk.object.file', 'boxsdk.object.folder', 'boxsdk.object.user', 'boxsdk.object.group', 'boxsdk.object.enterprise', 'boxsdk.object.collaboration', 'boxsdk.object.comment', 'boxsdk.object.event', 'boxsdk.object.events', 'boxsdk.object.search', 'boxsdk.object.task', 'boxsdk.object.task_assignment', 'boxsdk.object.metadata', 'boxsdk.object.metadata_template', 'boxsdk.object.web_link', 'boxsdk.object.collection', 'boxsdk.object.legal_hold_policy', 'boxsdk.object.legal_hold_policy_assignment', 'boxsdk.object.file_version', 'boxsdk.object.file_version_retention', 'boxsdk.object.retention_policy', 'boxsdk.object.retention_policy_assignment', 'boxsdk.object.invite', 'boxsdk.object.storage_policy', 'boxsdk.object.storage_policy_assignment', 'boxsdk.object.terms_of_service', 'boxsdk.object.terms_of_service_user_status', 'boxsdk.object.upload_session', 'boxsdk.object.device_pinner', 'boxsdk.object.webhook', 'boxsdk.object.watermark', 'boxsdk.object.email_alias', 'boxsdk.object.shared_link', 'boxsdk.object.recent_item', 'boxsdk.object.file_request'],
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
