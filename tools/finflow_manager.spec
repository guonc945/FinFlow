# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_all


def resolve_tools_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except Exception:
        cwd = Path(os.getcwd()).resolve()
        candidate = cwd / "tools"
        if candidate.exists():
            return candidate
        return cwd


tools_dir = resolve_tools_dir()
project_root = tools_dir.parent

# Package only the manager executable and required assets.
# Runtime project discovery is still handled by finflow_manager.py itself.
datas = [
    (str(tools_dir / "finflow_manager_icon.png"), "."),
    (str(tools_dir / "finflow_manager_icon.ico"), "."),
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
    "subprocess",
    "multiprocessing",
    "concurrent.futures",
]

pystray_datas, pystray_binaries, pystray_hiddenimports = collect_all("pystray")
datas += pystray_datas
binaries += pystray_binaries
hiddenimports += pystray_hiddenimports

a = Analysis(
    [str(tools_dir / "finflow_manager.py")],
    pathex=[str(tools_dir)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[str(tools_dir / "pyi_rth_silence_warnings.py")],
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
    icon=str(tools_dir / "finflow_manager_icon.ico"),
)
