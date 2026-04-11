# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path

from PyInstaller.utils.hooks import collect_all


PROJECT_ROOT = Path.cwd()
TOOLS_DIR = PROJECT_ROOT / "tools"

datas = [
    (str(TOOLS_DIR / "finflow_manager_icon.png"), "."),
    (str(TOOLS_DIR / "finflow_manager_icon.ico"), "."),
]
binaries = []
hiddenimports = [
    "PIL",
    "PIL.Image",
    "PIL.ImageDraw",
    "PIL.ImageFont",
    "pystray",
    "pystray._util",
    "pystray._win32",
    "tkinter",
    "tkinter.ttk",
    "tkinter.filedialog",
    "tkinter.messagebox",
    "tkinter.scrolledtext",
    "tkinter.commondialog",
    "http.client",
    "http.server",
    "socketserver",
]

pystray_datas, pystray_binaries, pystray_hiddenimports = collect_all("pystray")
datas += pystray_datas
binaries += pystray_binaries
hiddenimports += pystray_hiddenimports


a = Analysis(
    [str(TOOLS_DIR / "finflow_manager.py")],
    pathex=[str(TOOLS_DIR)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(TOOLS_DIR / "pyi_rth_silence_warnings.py")],
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
    name="FinFlowManager",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(TOOLS_DIR / "finflow_manager_icon.ico"),
)
