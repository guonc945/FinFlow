# -*- mode: python ; coding: utf-8 -*-
import os
from pathlib import Path
from PyInstaller.utils.hooks import collect_all

# 获取项目根目录：spec 文件所在目录的父目录
# 在 PyInstaller 执行 spec 文件时，使用特殊方式获取路径
try:
    # PyInstaller 执行时会设置 __file__
    spec_file = eval('__file__')
    tools_dir = Path(spec_file).resolve().parent
except:
    # 回退方案：使用当前工作目录
    tools_dir = Path(os.getcwd()).resolve()

project_root = tools_dir.parent

# 只包含图标文件，不包含项目代码（项目文件在服务器上已经存在）
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
