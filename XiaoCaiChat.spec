# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['qt_chat_client.py'],
    pathex=[],
    binaries=[],
    datas=[('icons', 'icons'), ('themes', 'themes')],
    hiddenimports=[],
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
    [],
    exclude_binaries=True,
    name='XiaoCaiChat',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['packaging/mac/build/xiaocaichat.icns'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='XiaoCaiChat',
)
app = BUNDLE(
    coll,
    name='XiaoCaiChat.app',
    icon='packaging/mac/build/xiaocaichat.icns',
    bundle_identifier=None,
)
