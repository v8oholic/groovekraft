# -*- mode: python ; coding: utf-8 -*-

# Inject dynamic version
import sys
sys.path.insert(0, '.')
from shared.version import __version__

a = Analysis(
    ['groovekraft.py'],
    pathex=[],
    binaries=[],
    datas=[('assets', 'assets')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['debugpy', 'pydevd'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='GrooveKraft',
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
    icon=['assets/groovekraft.icns'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='GrooveKraft',
)
app = BUNDLE(
    coll,
    name='GrooveKraft.app',
    icon='assets/groovekraft.icns',
    bundle_identifier='com.groovekraft.app',
    info_plist={
        'CFBundleName': 'GrooveKraft',
        'CFBundleShortVersionString': __version__,
        'CFBundleVersion': __version__,
        'CFBundleIdentifier': 'com.groovekraft.app'
    }
)
