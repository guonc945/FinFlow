# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import platform
import queue
import re
import secrets
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import webbrowser
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import http.client
import warnings
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

tk = None
ttk = None
filedialog = None
messagebox = None
scrolledtext = None
Image = None
ImageDraw = None
Icon = None
Menu = None
MenuItem = None

warnings.filterwarnings("ignore", message="pkg_resources is deprecated as an API.*", category=UserWarning)

def ensure_gui_dependencies() -> None:
    global tk, ttk, filedialog, messagebox, scrolledtext
    global Image, ImageDraw, Icon, Menu, MenuItem

    if tk is not None and Image is not None and Icon is not None:
        return

    import tkinter as _tk
    from tkinter import filedialog as _filedialog, messagebox as _messagebox, scrolledtext as _scrolledtext, ttk as _ttk

    try:
        from PIL import Image as _Image, ImageDraw as _ImageDraw
    except ModuleNotFoundError as exc:
        print(
        "缺少管理器依赖 Pillow。\n"
        "请先执行：python -m pip install -r deploy\\windows\\manager_requirements.txt\n"
        f"详细信息：{exc}"
        )
        raise SystemExit(1) from exc

    try:
        from pystray import Icon as _Icon, Menu as _Menu, MenuItem as _MenuItem
    except ModuleNotFoundError as exc:
        print(
        "缺少管理器依赖 pystray。\n"
        "请先执行：python -m pip install -r deploy\\windows\\manager_requirements.txt\n"
        f"详细信息：{exc}"
        )
        raise SystemExit(1) from exc

    tk = _tk
    ttk = _ttk
    filedialog = _filedialog
    messagebox = _messagebox
    scrolledtext = _scrolledtext
    Image = _Image
    ImageDraw = _ImageDraw
    Icon = _Icon
    Menu = _Menu
    MenuItem = _MenuItem


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FinFlow manager CLI")
    parser.add_argument("--service-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--frontend-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--daemon", action="store_true", help="Run in daemon mode with console logging")
    parser.add_argument("--start", action="store_true", help="Start backend service host")
    parser.add_argument("--stop", action="store_true", help="Stop backend service host")
    parser.add_argument("--restart", action="store_true", help="Restart backend service host")
    parser.add_argument("--backup", action="store_true", help="Run database backup")
    parser.add_argument("--restore", action="store_true", help="Restore database from .bak file")
    parser.add_argument("--status", action="store_true", help="Print runtime status")
    parser.add_argument("--health", action="store_true", help="Run health check probe")
    parser.add_argument("--sync-deps", action="store_true", help="Sync backend Python dependencies")
    parser.add_argument("--backup-dir", default="", help="Backup output directory")
    parser.add_argument("--sqlcmd-path", default="", help="sqlcmd executable path")
    parser.add_argument("--restore-file", default="", help="Path to .bak file for restore")
    return parser


def run_cli(args: argparse.Namespace) -> int:
    if args.service_run:
        append_text_log(SERVICE_HOST_LOG, "===== Service Run Entry =====")
        append_text_log(SERVICE_HOST_LOG, build_runtime_snapshot_text())
        try:
            host = BackendServiceHost()
            return host.run()
        except BaseException:
            append_text_log(SERVICE_HOST_LOG, traceback.format_exc())
            raise

    if args.frontend_run:
        append_text_log(FRONTEND_STDERR_LOG, "===== Frontend Run Entry =====")
        append_text_log(FRONTEND_STDERR_LOG, build_runtime_snapshot_text())
        try:
            return run_frontend_static_host()
        except BaseException:
            append_text_log(FRONTEND_STDERR_LOG, traceback.format_exc())
            raise

    if args.daemon:
        host = BackendServiceHost()
        return host.run_daemon()

    mode_flags = [args.start, args.stop, args.restart, args.backup, args.restore, args.status, args.health, args.sync_deps]
    if not any(mode_flags):
        try:
            app = FinFlowManagerApp()
            app.run()
            return 0
        except Exception as exc:
            print(f"启动 GUI 失败: {exc}")
            return 1

    controller = ManagedBackendController()

    if args.status:
        print(controller.poll_status())
        return 0

    if args.health:
        config = load_effective_config_from_disk()
        host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
        port = config.get("APP_PORT", "8100")
        base_url = f"http://{host}:{port}"
        ok, detail = probe_backend_health(base_url)
        print(f"健康检查: {'正常' if ok else '异常'} - {detail}")
        return 0 if ok else 1

    if args.sync_deps:
        ok, detail = sync_backend_dependencies()
        print(f"依赖同步: {'成功' if ok else '失败'} - {detail}")
        return 0 if ok else 1

    if args.start:
        ok, message = controller.start(load_effective_config_from_disk())
        print(message)
        return 0 if ok else 1

    if args.stop:
        ok, message = controller.stop()
        print(message)
        return 0 if ok else 1

    if args.restart:
        ok, message = controller.restart(load_effective_config_from_disk())
        print(message)
        return 0 if ok else 1

    if args.backup:
        state = read_state_file(STATE_PATH)
        backup_dir = Path((args.backup_dir or "").strip() or str(state.get("backup_dir") or BACKUP_DIR))
        sqlcmd_path = (args.sqlcmd_path or "").strip() or str(state.get("sqlcmd_path") or "sqlcmd")
        retention_days = int(state.get("backup_retention_days", 30))
        retention_count = int(state.get("backup_retention_count", 10))
        ok, detail = run_database_backup_with_cleanup(load_effective_config_from_disk(), backup_dir, sqlcmd_path, retention_days, retention_count)
        if ok:
            print(f"数据库备份成功: {detail}")
            return 0
        print(f"数据库备份失败: {detail}")
        return 1

    if args.restore:
        state = read_state_file(STATE_PATH)
        sqlcmd_path = (args.sqlcmd_path or "").strip() or str(state.get("sqlcmd_path") or "sqlcmd")
        restore_file = (args.restore_file or "").strip()
        if not restore_file:
            bak_files = sorted(BACKUP_DIR.glob("*.bak"), key=lambda p: p.stat().st_mtime, reverse=True)
            if bak_files:
                restore_file = str(bak_files[0])
                print(f"未指定恢复文件，使用最新备份: {restore_file}")
            else:
                print("未指定恢复文件，且备份目录中无 .bak 文件")
                return 1
        ok, detail = restore_database_from_backup(load_effective_config_from_disk(), Path(restore_file), sqlcmd_path)
        if ok:
            print(f"数据库恢复成功: {detail}")
            return 0
        print(f"数据库恢复失败: {detail}")
        return 1

    print("未识别的命令")
    return 2


def ensure_tcl_tk_environment() -> None:
    if os.environ.get("TCL_LIBRARY") and os.environ.get("TK_LIBRARY"):
        return

    base_candidates = [
        Path(sys.base_prefix),
        Path(getattr(sys, "_base_executable", sys.executable)).resolve().parent,
        Path(sys.executable).resolve().parent,
    ]

    checked: set[Path] = set()
    for base in base_candidates:
        if base in checked:
            continue
        checked.add(base)

        tcl_root = base / "tcl"
        if not tcl_root.is_dir():
            continue

        tcl_candidates = sorted([item for item in tcl_root.glob("tcl*") if item.is_dir()], reverse=True)
        tk_candidates = sorted([item for item in tcl_root.glob("tk*") if item.is_dir()], reverse=True)
        if not tcl_candidates or not tk_candidates:
            continue

        os.environ.setdefault("TCL_LIBRARY", str(tcl_candidates[0]))
        os.environ.setdefault("TK_LIBRARY", str(tk_candidates[0]))
        return


def discover_root_dir() -> Path:
    if getattr(sys, "frozen", False):
        # 打包后的 EXE：使用 sys._MEIPASS 作为临时解压目录
        if hasattr(sys, "_MEIPASS"):
            # _MEIPASS 是 PyInstaller 运行时解压的临时目录
            frozen_dir = Path(sys._MEIPASS)
        else:
            frozen_dir = Path(sys.executable).resolve().parent
        
        # 在打包环境中，项目目录被包含在 _MEIPASS 下
        # 检查 _MEIPASS 下是否有 backend 和 frontend 目录
        if (frozen_dir / "backend" / "main.py").is_file() and (frozen_dir / "frontend").is_dir():
            return frozen_dir
        
        # 如果 _MEIPASS 下没有项目目录，尝试从 EXE 所在目录查找
        start_dir = Path(sys.executable).resolve().parent
        candidates = [start_dir, *start_dir.parents]
        for candidate in candidates:
            if (candidate / "backend" / "main.py").is_file() and (candidate / "frontend").is_dir():
                return candidate
        
        # 最后使用 _MEIPASS 作为根目录（项目文件应该在这里）
        return frozen_dir
    else:
        # 开发环境：使用脚本所在目录的父目录
        start_dir = Path(__file__).resolve().parents[1]

        candidates = [start_dir, *start_dir.parents]
        for candidate in candidates:
            if (candidate / "backend" / "main.py").is_file() and (candidate / "frontend").is_dir():
                return candidate
        return start_dir


ROOT_DIR = discover_root_dir()
BACKEND_DIR = ROOT_DIR / "backend"
FRONTEND_DIR = ROOT_DIR / "frontend"
DIST_DIR = FRONTEND_DIR / "dist"
LOG_DIR = BACKEND_DIR / "logs"
BACKEND_VENV_DIR = BACKEND_DIR / ".venv_runtime"
BACKEND_LEGACY_VENV_DIR = BACKEND_DIR / ".venv"
ENV_PATH = BACKEND_DIR / ".env"
ENV_EXAMPLE_PATH = BACKEND_DIR / ".env.example"
FRONTEND_ENV_PATH = FRONTEND_DIR / ".env"
FRONTEND_ENV_EXAMPLE_PATH = FRONTEND_DIR / ".env.example"
KEY_PATH = BACKEND_DIR / ".encryption.key"
TOOLS_DIR = Path(__file__).resolve().parent
MANAGER_ICON_PNG = TOOLS_DIR / "finflow_manager_icon.png"
MANAGER_ICON_ICO = TOOLS_DIR / "finflow_manager_icon.ico"
STATE_PATH = ROOT_DIR / "deploy" / "windows" / "manager_state.json"
STDOUT_LOG = LOG_DIR / "backend.stdout.log"
STDERR_LOG = LOG_DIR / "backend.stderr.log"
FRONTEND_STDOUT_LOG = LOG_DIR / "frontend.stdout.log"
FRONTEND_STDERR_LOG = LOG_DIR / "frontend.stderr.log"
PROJECT_SYNC_LOG = BACKEND_DIR / "scripts" / "fetch_projects.log"
BACKUP_DIR = ROOT_DIR / "backups"
UPGRADE_BACKUP_DIR = ROOT_DIR / "deploy" / "windows" / "upgrade_backups"
LOG_ARCHIVE_DIR = LOG_DIR / "archive"
STARTUP_DIR = Path(os.environ.get("APPDATA") or str(ROOT_DIR)) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
STARTUP_SCRIPT_PATH = STARTUP_DIR / "FinFlowManager.cmd"
RUNTIME_DIR = ROOT_DIR / "deploy" / "windows" / "runtime"
RUNTIME_STATE_PATH = RUNTIME_DIR / "service_runtime.json"
STOP_REQUEST_PATH = RUNTIME_DIR / "service.stop"
SERVICE_HOST_LOG = LOG_DIR / "manager.service.log"
STATUS_REFRESH_INTERVAL_MS = 2000
LOG_REFRESH_INTERVAL_MS = 2500
HEALTH_CHECK_INTERVAL_MS = 5000
DB_MONITOR_INTERVAL_MS = 15000
HEALTH_CHECK_TIMEOUT = 3

PROTECTED_EXACT_PATHS = {
    Path(".encryption.key"),
    Path("backend/.env"),
    Path("backend/.encryption.key"),
    Path("frontend/.env"),
    Path("deploy/windows/manager_state.json"),
}
PROTECTED_PREFIXES = {
    Path(".git"),
    Path(".venv"),
    Path("backend/.venv"),
    Path("backend/logs"),
    Path("frontend/node_modules"),
    Path("deploy/windows/build"),
    Path("deploy/windows/dist"),
    Path("deploy/windows/upgrade_backups"),
    Path("backups"),
}
SKIP_PART_NAMES = {"__pycache__", ".git", ".venv", "node_modules"}


ENV_SECTIONS: List[Tuple[str, List[Tuple[str, str, bool]]]] = [
    (
        "应用配置",
        [
            ("APP_HOST", "监听地址", False),
            ("APP_PORT", "监听端口", False),
            ("ALLOWED_ORIGINS", "允许来源", False),
            ("ALLOW_LAN_ORIGINS", "允许局域网来源", False),
        ],
    ),
    (
        "数据库配置",
        [
            ("DATABASE_URL", "数据库连接串", False),
            ("DB_HOST", "数据库主机", False),
            ("DB_PORT", "数据库端口", False),
            ("DB_NAME", "数据库名称", False),
            ("DB_USER", "数据库用户", False),
            ("DB_PASSWORD", "数据库密码", True),
        ],
    ),
    (
        "认证配置",
        [
            ("SECRET_KEY", "JWT 密钥", True),
            ("ACCESS_TOKEN_EXPIRE_MINUTES", "Token 有效期(分钟)", False),
        ],
    ),
    (
        "外部系统配置",
        [
            ("MARKI_USER", "Marki 用户", False),
            ("MARKI_PASSWORD", "Marki 密码", True),
            ("MARKI_SYSTEM_ID", "Marki 系统 ID", False),
        ],
    ),
]


DEFAULT_ENV = {
    "DATABASE_URL": "",
    "DB_HOST": "localhost",
    "DB_PORT": "1433",
    "DB_NAME": "finflow",
    "DB_USER": "admin",
    "DB_PASSWORD": "",
    "SECRET_KEY": "",
    "ACCESS_TOKEN_EXPIRE_MINUTES": "1440",
    "ENCRYPTION_KEY": "",
    "ENCRYPTION_KEY_FILE": ".encryption.key",
    "MARKI_USER": "",
    "MARKI_PASSWORD": "",
    "MARKI_SYSTEM_ID": "",
    "APP_HOST": "127.0.0.1",
    "APP_PORT": "8100",
    "APP_RELOAD": "false",
    "ALLOWED_ORIGINS": "http://127.0.0.1:8100,http://localhost:8100",
    "ALLOW_LAN_ORIGINS": "false",
}

DEFAULT_STATE = {
    "auto_restart_backend": True,
    "auto_restart_frontend": True,
    "start_backend_on_launch": False,
    "start_frontend_on_launch": False,
    "hide_to_tray_on_close": True,
    "launch_manager_on_startup": False,
    "enable_health_check": True,
    "enable_db_monitor": True,
    "git_auto_build_frontend": True,
    "frontend_deploy_source": "",
    "release_package_path": "",
    "backup_dir": str(BACKUP_DIR),
    "sqlcmd_path": "sqlcmd",
    "git_repo_url": "",
    "git_branch": "main",
    "backup_retention_days": "30",
    "backup_retention_count": "10",
    "log_max_size_mb": "20",
    "log_archive_retention_days": "90",
    "webhook_url": "",
}

OPS_STATE_KEYS = (
    "frontend_deploy_source",
    "release_package_path",
    "backup_dir",
    "sqlcmd_path",
    "git_repo_url",
    "git_branch",
    "backup_retention_days",
    "backup_retention_count",
    "log_max_size_mb",
    "log_archive_retention_days",
    "webhook_url",
)


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def read_env_file(path: Path) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if not path.exists():
        return result
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def write_env_file(path: Path, values: Dict[str, str], extra_values: Dict[str, str]) -> None:
    ordered_keys = list(DEFAULT_ENV.keys())
    lines: List[str] = []
    for key in ordered_keys:
        value = values.get(key, "")
        lines.append(f"{key}={value}")

    for key in sorted(extra_values.keys()):
        if key not in values:
            lines.append(f"{key}={extra_values[key]}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_key_value_env_file(path: Path, ordered_keys: List[str], values: Dict[str, str]) -> None:
    seen: set[str] = set()
    lines: List[str] = []
    for key in ordered_keys:
        if key in values:
            lines.append(f"{key}={values[key]}")
            seen.add(key)
    for key in sorted(values.keys()):
        if key not in seen:
            lines.append(f"{key}={values[key]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_state_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return dict(DEFAULT_STATE)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_STATE)
    state = dict(DEFAULT_STATE)
    for key in DEFAULT_STATE:
        if key in raw:
            default_value = DEFAULT_STATE[key]
            if isinstance(default_value, bool):
                state[key] = parse_bool(raw[key])
            else:
                state[key] = str(raw[key])
    return state


def write_state_file(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


DEFAULT_RUNTIME_STATE = {
    "service_host_pid": 0,
    "backend_pid": 0,
    "frontend_pid": 0,
    "user_stopped": False,
    "last_start_attempt": 0.0,
    "last_exit_code": None,
    "last_exit_at": 0.0,
    "last_started_at": 0.0,
    "consecutive_failed_starts": 0,
    "auto_restart_suppressed": False,
    "last_session_marker": "",
    "last_session_started_label": "",
    "frontend_user_stopped": False,
    "frontend_last_start_attempt": 0.0,
    "frontend_last_exit_code": None,
    "frontend_last_exit_at": 0.0,
    "frontend_last_started_at": 0.0,
    "frontend_consecutive_failed_starts": 0,
    "frontend_auto_restart_suppressed": False,
    "frontend_last_session_marker": "",
    "frontend_last_session_started_label": "",
}


def ensure_runtime_dir() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def read_runtime_state() -> Dict[str, Any]:
    state = dict(DEFAULT_RUNTIME_STATE)
    if not RUNTIME_STATE_PATH.exists():
        return state
    try:
        raw = json.loads(RUNTIME_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return state

    for key, default_value in DEFAULT_RUNTIME_STATE.items():
        if key not in raw:
            continue
        value = raw[key]
        if isinstance(default_value, bool):
            state[key] = parse_bool(value)
        elif isinstance(default_value, int):
            state[key] = int(value or 0)
        elif isinstance(default_value, float):
            state[key] = float(value or 0)
        else:
            state[key] = value
    return state


def write_runtime_state(**updates: Any) -> Dict[str, Any]:
    ensure_runtime_dir()
    state = read_runtime_state()
    state.update(updates)
    RUNTIME_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return state


def reset_runtime_state() -> None:
    ensure_runtime_dir()
    state = dict(DEFAULT_RUNTIME_STATE)
    current = read_runtime_state()
    for key in (
        "frontend_pid",
        "frontend_user_stopped",
        "frontend_last_start_attempt",
        "frontend_last_exit_code",
        "frontend_last_exit_at",
        "frontend_last_started_at",
        "frontend_consecutive_failed_starts",
        "frontend_auto_restart_suppressed",
        "frontend_last_session_marker",
        "frontend_last_session_started_label",
    ):
        state[key] = current.get(key, DEFAULT_RUNTIME_STATE[key])
    RUNTIME_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def create_stop_request() -> None:
    ensure_runtime_dir()
    STOP_REQUEST_PATH.write_text("stop\n", encoding="utf-8")


def clear_stop_request() -> None:
    STOP_REQUEST_PATH.unlink(missing_ok=True)


def is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def load_effective_config_from_disk() -> Dict[str, str]:
    values = dict(DEFAULT_ENV)
    values.update(read_env_file(ENV_EXAMPLE_PATH))
    values.update(read_env_file(ENV_PATH))
    values["APP_RELOAD"] = "false"
    return values


def get_backend_venv_candidates() -> List[Path]:
    return [BACKEND_VENV_DIR, BACKEND_LEGACY_VENV_DIR]


def get_backend_python_executable() -> Path:
    for venv_dir in get_backend_venv_candidates():
        python_exe = venv_dir / "Scripts" / "python.exe"
        if python_exe.exists():
            return python_exe
    return BACKEND_VENV_DIR / "Scripts" / "python.exe"


def get_backend_venv_dir() -> Path:
    return get_backend_python_executable().parent.parent


def evaluate_manager_runtime_layout() -> Tuple[bool, List[str]]:
    issues: List[str] = []
    if not BACKEND_DIR.exists():
        issues.append(f"未找到 backend 目录：{BACKEND_DIR}")
    if not (BACKEND_DIR / "main.py").exists():
        issues.append(f"未找到后端入口文件：{BACKEND_DIR / 'main.py'}")
    if not FRONTEND_DIR.exists():
        issues.append(f"未找到 frontend 目录：{FRONTEND_DIR}")
    if not DIST_DIR.exists():
        issues.append(f"未找到前端构建目录：{DIST_DIR}")
    if not (DIST_DIR / "index.html").exists():
        issues.append(f"未找到前端首页文件：{DIST_DIR / 'index.html'}")
    if not STATE_PATH.parent.exists():
        issues.append(f"未找到部署状态目录：{STATE_PATH.parent}")
    return len(issues) == 0, issues


def format_manager_runtime_layout_issues(issues: List[str]) -> str:
    if not issues:
        return ""
    joined = "\n".join(f"- {item}" for item in issues)
    return (
        "当前 EXE 运行目录不满足完整服务管理条件。\n"
        "打包后的管理器不是独立业务程序，必须放在完整的 FinFlow 发布目录中运行。\n\n"
        f"{joined}"
    )


def load_manager_icon_image() -> Image.Image | None:
    ensure_gui_dependencies()
    for path in (MANAGER_ICON_PNG, MANAGER_ICON_ICO):
        if not path.exists():
            continue
        try:
            return Image.open(path).convert("RGBA")
        except Exception:
            continue
    return None


def create_tray_image() -> Image.Image:
    ensure_gui_dependencies()
    icon_image = load_manager_icon_image()
    if icon_image is not None:
        return icon_image.resize((64, 64), Image.LANCZOS)
    image = Image.new("RGBA", (64, 64), (27, 84, 157, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill=(39, 125, 161, 255))
    draw.rectangle((20, 18, 44, 24), fill=(255, 255, 255, 255))
    draw.rectangle((20, 30, 44, 36), fill=(255, 255, 255, 255))
    draw.rectangle((20, 42, 36, 48), fill=(255, 255, 255, 255))
    return image


def create_window_icon() -> Image.Image:
    ensure_gui_dependencies()
    icon_image = load_manager_icon_image()
    if icon_image is not None:
        return icon_image.resize((256, 256), Image.LANCZOS)
    image = Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    
    # 背景渐变圆形（深蓝到浅蓝）
    for y in range(256):
        ratio = y / 256.0
        r = int(20 + (52 - 20) * ratio)
        g = int(100 + (170 - 100) * ratio)
        b = int(180 + (220 - 180) * ratio)
        draw.line([(0, y), (256, y)], fill=(r, g, b, 255))
    
    # 外圆边框
    draw.ellipse([(16, 16), (240, 240)], outline=(255, 255, 255, 200), width=3)
    
    # 金融图表元素 - 上升的折线图
    # 主折线
    points = [(60, 180), (90, 160), (120, 170), (150, 130), (180, 140), (210, 90), (230, 70)]
    draw.line(points, fill=(255, 255, 255), width=5)
    
    # 折线节点
    for px, py in points:
        draw.ellipse([(px-6, py-6), (px+6, py+6)], fill=(255, 220, 100, 255), outline=(255, 255, 255), width=2)
    
    # 数据柱状图
    draw.rectangle([(50, 150), (70, 200)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(85, 130), (105, 200)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(120, 140), (140, 200)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(155, 110), (175, 200)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(190, 90), (210, 200)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    
    # 货币符号装饰
    draw.text((115, 175), "¥", fill=(255, 255, 255, 220), font_size=24)
    
    # 高光效果
    draw.ellipse([(40, 40), (100, 100)], fill=(255, 255, 255, 60))
    
    return image


def create_high_res_icon() -> Image.Image:
    ensure_gui_dependencies()
    icon_image = load_manager_icon_image()
    if icon_image is not None:
        return icon_image.resize((512, 512), Image.LANCZOS)
    image = Image.new("RGBA", (512, 512), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    
    # 背景渐变圆形（深蓝到浅蓝）
    for y in range(512):
        ratio = y / 512.0
        r = int(20 + (52 - 20) * ratio)
        g = int(100 + (170 - 100) * ratio)
        b = int(180 + (220 - 180) * ratio)
        draw.line([(0, y), (512, y)], fill=(r, g, b, 255))
    
    # 外圆边框
    draw.ellipse([(32, 32), (480, 480)], outline=(255, 255, 255, 200), width=6)
    
    # 金融图表元素 - 上升的折线图
    # 主折线
    points = [(120, 360), (180, 320), (240, 340), (300, 260), (360, 280), (420, 180), (460, 140)]
    draw.line(points, fill=(255, 255, 255), width=10)
    
    # 折线节点
    for px, py in points:
        draw.ellipse([(px-12, py-12), (px+12, py+12)], fill=(255, 220, 100, 255), outline=(255, 255, 255), width=4)
    
    # 数据柱状图
    draw.rectangle([(100, 300), (140, 400)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(170, 260), (210, 400)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(240, 280), (280, 400)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(310, 220), (350, 400)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    draw.rectangle([(380, 180), (420, 400)], fill=(100, 200, 255, 180), outline=(255, 255, 255, 150))
    
    # 货币符号装饰
    draw.text((230, 350), "¥", fill=(255, 255, 255, 220), font_size=48)
    
    # 高光效果
    draw.ellipse([(80, 80), (200, 200)], fill=(255, 255, 255, 60))
    
    return image


def ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def is_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def can_connect_http(host: str, port: int, path: str = "/") -> bool:
    try:
        conn = http.client.HTTPConnection(host, port, timeout=2)
        conn.request("GET", path)
        response = conn.getresponse()
        response.read()
        conn.close()
        return 200 <= response.status < 500
    except Exception:
        return False


def find_listening_pids(port: int) -> List[int]:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=False,
            timeout=10,
            creationflags=creationflags,
        )
    except Exception:
        return []

    pids: List[int] = []
    stdout_text = decode_console_output(result.stdout or b"")
    for raw_line in stdout_text.splitlines():
        line = raw_line.strip()
        if not line or "TCP" not in line.upper():
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        local_address = parts[1]
        pid_text = parts[-1]
        if not local_address.endswith(f":{port}"):
            continue
        if not pid_text.isdigit():
            continue
        pid = int(pid_text)
        if pid not in pids:
            pids.append(pid)
    return pids


def get_process_name(pid: int) -> str:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=False,
            timeout=10,
            creationflags=creationflags,
        )
    except Exception:
        return ""

    stdout_text = decode_console_output(result.stdout or b"")
    line = stdout_text.strip().splitlines()
    if not line:
        return ""
    first = line[0].strip().strip('"')
    if not first or first.startswith("INFO:"):
        return ""
    return first.split('","')[0].strip('"')


def get_process_commandline(pid: int) -> str:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    command = (
        f"(Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\" | "
        "Select-Object -ExpandProperty CommandLine)"
    )
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", command],
            capture_output=True,
            text=False,
            timeout=10,
            creationflags=creationflags,
        )
    except Exception:
        return ""
    return decode_console_output(result.stdout or b"").strip()


def get_port_owner_info(port: int) -> Dict[str, str]:
    pids = find_listening_pids(port)
    if not pids:
        return {}
    pid = pids[0]
    return {
        "pid": str(pid),
        "name": get_process_name(pid),
        "command_line": get_process_commandline(pid),
    }


def build_port_owner_label(owner: Dict[str, str], manager_pid: int | None = None) -> str:
    if not owner:
        return "未占用"
    pid = owner.get("pid", "") or "未知"
    if manager_pid is not None and pid == str(manager_pid):
        return f"管理器实例 PID {pid}"
    return f"外部进程 PID {pid}"


def force_release_port(port: int, timeout_seconds: int = 12) -> Tuple[bool, str]:
    owners = find_listening_pids(port)
    if not owners:
        return True, f"端口 {port} 当前未被占用"

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    attempted: List[str] = []
    for pid in owners:
        attempted.append(str(pid))
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F"],
                capture_output=True,
                text=False,
                timeout=15,
                creationflags=creationflags,
            )
        except Exception:
            pass
        try:
            subprocess.run(
                ["powershell", "-NoProfile", "-Command", f"Stop-Process -Id {pid} -Force -ErrorAction SilentlyContinue"],
                capture_output=True,
                text=False,
                timeout=15,
                creationflags=creationflags,
            )
        except Exception:
            pass

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not find_listening_pids(port):
            return True, f"端口 {port} 已释放"
        time.sleep(0.5)

    remaining = find_listening_pids(port)
    if remaining:
        joined = ", ".join(str(pid) for pid in remaining)
        return False, f"端口 {port} 仍被占用，当前监听 PID: {joined}"
    return False, f"端口 {port} 释放状态未知，请重试确认"


def decode_console_output(data: bytes) -> str:
    if not data:
        return ""
    for encoding in ("utf-8-sig", "utf-16", "gbk", "mbcs", "latin-1"):
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
PYINSTALLER_WARNING_LINE_RE = re.compile(r"^PyInstaller\\loader\\pyimod02_importers\.py:\d+: UserWarning: pkg_resources is deprecated as an API\..*$", re.MULTILINE)
PYINSTALLER_WARNING_FOLLOW_RE = re.compile(r"^as early as 2025-11-30\..*$", re.MULTILINE)


def sanitize_console_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")
    text = ANSI_ESCAPE_RE.sub("", text)
    lines = []
    skip_followup = False
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if skip_followup:
            skip_followup = False
            if line.startswith("as early as 2025-11-30."):
                continue
        if PYINSTALLER_WARNING_LINE_RE.match(line):
            skip_followup = True
            continue
        if PYINSTALLER_WARNING_FOLLOW_RE.match(line):
            continue
        lines.append(raw_line)
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def build_node_process_env(base_env: Dict[str, str] | None = None) -> Dict[str, str]:
    env = dict(base_env or os.environ.copy())
    env["FORCE_COLOR"] = "0"
    env["NO_COLOR"] = "1"
    env["npm_config_color"] = "false"
    env["npm_config_loglevel"] = env.get("npm_config_loglevel", "verbose")
    env["CI"] = "1"
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env["NODE_DISABLE_COLORS"] = "1"
    return env


def tail_text_file(path: Path, max_lines: int = 40) -> str:
    if not path.exists():
        return ""
    try:
        lines = sanitize_console_text(decode_console_output(path.read_bytes())).splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-max_lines:]).strip()


def read_log_since_marker(path: Path, marker: str, max_lines: int = 80) -> str:
    if not path.exists():
        return ""
    try:
        lines = sanitize_console_text(decode_console_output(path.read_bytes())).splitlines()
    except Exception:
        return ""
    if marker:
        for index in range(len(lines) - 1, -1, -1):
            if lines[index].strip() == marker.strip():
                lines = lines[index:]
                break
    return "\n".join(lines[-max_lines:]).strip()


def append_text_log(path: Path, text: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8", errors="ignore") as handle:
            handle.write(text.rstrip() + "\n")
    except Exception:
        pass


def build_runtime_snapshot_text() -> str:
    layout_ok, layout_issues = evaluate_manager_runtime_layout()
    lines = [
        f"time={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"argv={sys.argv}",
        f"frozen={getattr(sys, 'frozen', False)}",
        f"cwd={Path.cwd()}",
        f"sys.executable={sys.executable}",
        f"ROOT_DIR={ROOT_DIR}",
        f"BACKEND_DIR={BACKEND_DIR}",
        f"FRONTEND_DIR={FRONTEND_DIR}",
        f"DIST_DIR={DIST_DIR}",
        f"RUNTIME_STATE_PATH={RUNTIME_STATE_PATH}",
        f"STOP_REQUEST_PATH={STOP_REQUEST_PATH}",
        f"layout_ok={layout_ok}",
    ]
    if layout_issues:
        lines.extend([f"layout_issue={item}" for item in layout_issues])
    return "\n".join(lines)


def build_runtime_state_snapshot_text() -> str:
    state = read_runtime_state()
    keys = [
        "service_host_pid",
        "backend_pid",
        "frontend_pid",
        "user_stopped",
        "last_start_attempt",
        "last_exit_code",
        "last_exit_at",
        "last_started_at",
        "consecutive_failed_starts",
        "auto_restart_suppressed",
        "last_session_started_label",
    ]
    return "\n".join(f"runtime_state.{key}={state.get(key)!r}" for key in keys)


def check_backend_runtime_requirements(config: Dict[str, str]) -> Tuple[bool, str]:
    layout_ok, layout_issues = evaluate_manager_runtime_layout()
    if not layout_ok:
        return False, format_manager_runtime_layout_issues(layout_issues)

    python_exe = get_backend_python_executable()
    if not python_exe.exists():
        return False, f"未找到后端虚拟环境，请先同步后端依赖：{python_exe}"
    if not ENV_PATH.exists():
        return False, "未找到 backend/.env，请先保存后端配置"
    if not KEY_PATH.exists():
        return False, "未找到 backend/.encryption.key，请先生成或放入正确密钥"

    env = os.environ.copy()
    env.update(config)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(
            [str(python_exe), "-c", "import fastapi, uvicorn; print(fastapi.__version__); print(uvicorn.__version__)"],
            cwd=BACKEND_DIR,
            env=env,
            capture_output=True,
            text=False,
            timeout=20,
            creationflags=creationflags,
        )
    except Exception as exc:
        return False, f"检查 FastAPI/uvicorn 运行环境失败：{exc}"
    if result.returncode != 0:
        detail = sanitize_console_text(decode_console_output(result.stderr or result.stdout or b"")).strip()
        return False, f"后端运行环境不完整，请先同步后端依赖：{detail or '缺少 fastapi 或 uvicorn'}"
    return True, "后端 FastAPI/uvicorn 运行环境正常"


def _read_process_stream(stream: Any, stream_name: str, sink: queue.Queue[tuple[str, bytes | None]]) -> None:
    try:
        while True:
            chunk = stream.readline()
            if not chunk:
                break
            sink.put((stream_name, chunk))
    finally:
        try:
            stream.close()
        except Exception:
            pass
        sink.put((stream_name, None))


def safe_process_pid(process: Any) -> int:
    try:
        return int(getattr(process, "pid", 0) or 0)
    except Exception:
        return 0


def safe_popen_poll(process: subprocess.Popen | None) -> int | None:
    if process is None:
        return None
    try:
        return process.poll()
    except OSError:
        pid = safe_process_pid(process)
        if pid > 0 and is_process_alive(pid):
            return None
        return getattr(process, "returncode", None) if getattr(process, "returncode", None) is not None else 1
    except Exception:
        pid = safe_process_pid(process)
        if pid > 0 and is_process_alive(pid):
            return None
        return getattr(process, "returncode", None)


def safe_process_is_running(process: Any) -> bool:
    if process is None:
        return False
    if isinstance(process, subprocess.Popen):
        return safe_popen_poll(process) is None
    return is_process_alive(safe_process_pid(process))


def safe_terminate_process(process: Any, timeout_seconds: float = 10.0) -> None:
    pid = safe_process_pid(process)
    if process is None:
        return
    if isinstance(process, subprocess.Popen):
        try:
            process.terminate()
        except Exception:
            if pid > 0:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if not safe_process_is_running(process):
                return
            time.sleep(0.2)
        try:
            process.kill()
        except Exception:
            if pid > 0:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
        kill_deadline = time.time() + 2
        while time.time() < kill_deadline:
            if not safe_process_is_running(process):
                return
            time.sleep(0.2)
        return
    if pid > 0:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass


def run_command_with_live_output(
    cmd: List[str],
    cwd: Path,
    timeout_seconds: int,
    log_callback: Callable[[str], None] | None = None,
    heartbeat_label: str = "",
    env: Dict[str, str] | None = None,
) -> Tuple[int, str, str]:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    process = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=False,
        creationflags=creationflags,
    )

    event_queue: queue.Queue[tuple[str, bytes | None]] = queue.Queue()
    stdout_chunks: List[bytes] = []
    stderr_chunks: List[bytes] = []
    stdout_done = process.stdout is None
    stderr_done = process.stderr is None

    if process.stdout is not None:
        threading.Thread(
            target=_read_process_stream,
            args=(process.stdout, "stdout", event_queue),
            daemon=True,
        ).start()
    if process.stderr is not None:
        threading.Thread(
            target=_read_process_stream,
            args=(process.stderr, "stderr", event_queue),
            daemon=True,
        ).start()

    started_at = time.monotonic()
    last_feedback_at = started_at
    heartbeat_seconds = 15

    while True:
        if timeout_seconds and time.monotonic() - started_at > timeout_seconds:
            safe_terminate_process(process, timeout_seconds=2)
            raise TimeoutError(f"命令执行超时（>{timeout_seconds} 秒）：{' '.join(cmd)}")

        try:
            stream_name, payload = event_queue.get(timeout=1)
        except queue.Empty:
            if log_callback and heartbeat_label and time.monotonic() - last_feedback_at >= heartbeat_seconds:
                elapsed = int(time.monotonic() - started_at)
                log_callback(f"    [RUN] {heartbeat_label}，已等待 {elapsed} 秒...")
                last_feedback_at = time.monotonic()
            if safe_popen_poll(process) is not None and stdout_done and stderr_done:
                break
            continue

        if payload is None:
            if stream_name == "stdout":
                stdout_done = True
            else:
                stderr_done = True
            if safe_popen_poll(process) is not None and stdout_done and stderr_done:
                break
            continue

        if stream_name == "stdout":
            stdout_chunks.append(payload)
        else:
            stderr_chunks.append(payload)

        if log_callback:
            text = sanitize_console_text(decode_console_output(payload))
            for line in text.split("\n"):
                line = line.strip()
                if not line:
                    continue
                log_callback(f"    {line}")
            last_feedback_at = time.monotonic()

    return (
        safe_popen_poll(process) if safe_popen_poll(process) is not None else process.wait(),
        sanitize_console_text(decode_console_output(b"".join(stdout_chunks))).strip(),
        sanitize_console_text(decode_console_output(b"".join(stderr_chunks))).strip(),
    )


def run_python_json(python_exe: Path, code: str) -> Dict[str, Any]:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        result = subprocess.run(
            [str(python_exe), "-c", code],
            capture_output=True,
            text=False,
            timeout=20,
            creationflags=creationflags,
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

    stdout_text = decode_console_output(result.stdout or b"")
    stderr_text = decode_console_output(result.stderr or b"")

    if result.returncode != 0:
        detail = (stderr_text or stdout_text or f"exit code {result.returncode}").strip()
        return {"ok": False, "error": detail}

    try:
        return json.loads(stdout_text.strip() or "{}")
    except Exception as exc:
        return {"ok": False, "error": f"JSON 解析失败：{exc}"}


def resolve_browser_host(host: str) -> str:
    host = (host or "").strip()
    if host in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return host


def remove_path(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def extract_db_connection(config: Dict[str, str]) -> Dict[str, str]:
    database_url = (config.get("DATABASE_URL") or "").strip()
    if database_url:
        parsed = urlparse(database_url)
        if parsed.scheme.startswith("postgresql"):
            return {
                "host": parsed.hostname or "127.0.0.1",
                "port": str(parsed.port or 5432),
                "user": parsed.username or "",
                "password": parsed.password or "",
                "dbname": (parsed.path or "").lstrip("/"),
            }
        if parsed.scheme.startswith("mssql") or parsed.scheme.startswith("sqlserver"):
            return {
                "host": parsed.hostname or "127.0.0.1",
                "port": str(parsed.port or 1433),
                "user": parsed.username or "",
                "password": parsed.password or "",
                "dbname": (parsed.path or "").lstrip("/"),
            }

    db_type = (config.get("DB_DIALECT") or "").strip().lower()
    if db_type in {"mssql", "sqlserver", "sql_server"}:
        return {
            "host": (config.get("DB_HOST") or "127.0.0.1").strip(),
            "port": (config.get("DB_PORT") or "1433").strip(),
            "user": (config.get("DB_USER") or "").strip(),
            "password": (config.get("DB_PASSWORD") or "").strip(),
            "dbname": (config.get("DB_NAME") or "").strip(),
        }

    return {
        "host": (config.get("DB_HOST") or "127.0.0.1").strip(),
        "port": (config.get("DB_PORT") or "5432").strip(),
        "user": (config.get("DB_USER") or "").strip(),
        "password": (config.get("DB_PASSWORD") or "").strip(),
        "dbname": (config.get("DB_NAME") or "").strip(),
    }


def get_database_connection_issues(config: Dict[str, str]) -> Tuple[Dict[str, str], List[str]]:
    conn = extract_db_connection(config)
    missing = [key for key in ("host", "port", "user", "dbname") if not conn.get(key)]
    return conn, missing


def describe_database_configuration(config: Dict[str, str]) -> Tuple[str, str]:
    conn, missing = get_database_connection_issues(config)
    if missing:
        return "missing_config", f"数据库配置不完整，缺少：{', '.join(missing)}"
    return "ready", f"{conn['host']}:{conn['port']} / {conn['dbname']} / 用户 {conn['user']}"


def describe_database_configuration_from_disk() -> Tuple[str, str]:
    if not ENV_PATH.exists():
        return "missing_env", "未找到 backend/.env，请先在“配置管理”中保存数据库连接配置"
    return describe_database_configuration(load_effective_config_from_disk())


def check_database_connectivity_via_backend_runtime(config: Dict[str, str]) -> Tuple[bool, str]:
    python_exe = get_backend_python_executable()
    if not python_exe.exists():
        return False, f"未找到后端虚拟环境 Python：{python_exe}"

    env = os.environ.copy()
    env.update(config)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    script = (
        "from sqlalchemy import create_engine, text\n"
        "from database import _build_database_url, _connect_args\n"
        "engine = create_engine(_build_database_url(), pool_pre_ping=True, connect_args=_connect_args)\n"
        "with engine.connect() as conn:\n"
        "    conn.execute(text('SELECT 1'))\n"
        "print('数据库连接正常')\n"
    )
    try:
        result = subprocess.run(
            [str(python_exe), "-c", script],
            cwd=BACKEND_DIR,
            env=env,
            capture_output=True,
            text=False,
            timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        return False, f"后端数据库驱动检查异常：{exc}"

    stdout_text = decode_console_output(result.stdout or b"").strip()
    stderr_text = decode_console_output(result.stderr or b"").strip()
    if result.returncode == 0:
        conn = extract_db_connection(config)
        return True, stdout_text or f"数据库 {conn.get('dbname') or ''} 连接正常".strip()
    combined_text = stderr_text or stdout_text or "数据库连接失败"
    if "ModuleNotFoundError" in combined_text or "No module named" in combined_text:
        return False, f"后端虚拟环境缺少数据库检查依赖：{combined_text}"
    return False, combined_text


def evaluate_database_runtime_status(sqlcmd: str = "sqlcmd") -> Tuple[str, str]:
    config_state, config_detail = describe_database_configuration_from_disk()
    if config_state != "ready":
        return config_state, config_detail

    config = load_effective_config_from_disk()
    backend_host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
    backend_port = (config.get("APP_PORT") or "8100").strip() or "8100"
    if backend_port.isdigit():
        backend_ok, payload, _ = fetch_backend_health_payload(f"http://{backend_host}:{backend_port}")
        if payload:
            database_status = payload.get("database")
            if backend_ok and database_status == "ok":
                conn = extract_db_connection(config)
                return "ok", f"数据库 {conn.get('dbname') or ''} 连接正常（来自后端健康检查）".strip()
            if database_status and database_status != "ok":
                return "error", str(database_status)

    runtime_python = get_backend_python_executable()
    if runtime_python.exists():
        ok, detail = check_database_connectivity_via_backend_runtime(config)
        if ok:
            return "ok", detail
        if "缺少数据库检查依赖" in detail:
            return "missing_runtime", detail
        return "error", detail

    sqlcmd = (sqlcmd or "").strip()
    sqlcmd_available = False
    if sqlcmd:
        sqlcmd_path = Path(sqlcmd)
        sqlcmd_available = sqlcmd_path.exists() if sqlcmd_path.suffix else shutil.which(sqlcmd) is not None
    if not sqlcmd_available:
        return "missing_runtime", "未找到后端虚拟环境，且 sqlcmd 不可用，暂时无法检查数据库连接"

    ok, detail = check_database_connectivity(config, sqlcmd)
    return ("ok" if ok else "error"), detail


def format_database_config_status(state: str, detail: str) -> str:
    prefix = {
        "ready": "已配置",
        "missing_env": "待配置",
        "missing_config": "配置不完整",
    }.get(state, "未知")
    return f"{prefix} ({detail})"


def format_database_connection_status(state: str, detail: str) -> str:
    prefix = {
        "ok": "正常",
        "error": "异常",
        "missing_env": "待配置",
        "missing_config": "配置不完整",
        "missing_sqlcmd": "工具不可用",
        "missing_runtime": "环境未就绪",
    }.get(state, "未检查")
    return f"{prefix} ({detail})"


def sync_backend_dependencies(timeout_seconds: int = 900) -> Tuple[bool, str]:
    python_exe = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
    requirements_path = BACKEND_DIR / "requirements.txt"
    if not python_exe.exists():
        return False, f"未找到后端虚拟环境：{python_exe}"
    if not requirements_path.exists():
        return False, f"未找到依赖清单：{requirements_path}"

    try:
        # 先升级 pip
        upgrade_result = subprocess.run(
            [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
            cwd=BACKEND_DIR,
            capture_output=True,
            text=False,
            timeout=120,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if upgrade_result.returncode != 0:
            return False, "升级 pip 失败"
        
        # 安装依赖
        result = subprocess.run(
            [str(python_exe), "-m", "pip", "install", "-r", str(requirements_path)],
            cwd=BACKEND_DIR,
            capture_output=True,
            text=False,
            timeout=timeout_seconds,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        return False, f"安装依赖失败：{exc}"

    stdout_text = decode_console_output(result.stdout or b"").strip()
    stderr_text = decode_console_output(result.stderr or b"").strip()
    if result.returncode != 0:
        detail = stderr_text or stdout_text or "未知错误"
        return False, detail
    return True, stdout_text or "依赖已同步"


def resolve_frontend_service_settings(config: Dict[str, str] | None = None) -> Dict[str, str]:
    effective_config = dict(config or load_effective_config_from_disk())
    frontend_values = read_env_file(FRONTEND_ENV_EXAMPLE_PATH)
    frontend_values.update(read_env_file(FRONTEND_ENV_PATH))

    backend_host = resolve_browser_host(effective_config.get("APP_HOST", "127.0.0.1"))
    backend_port = (effective_config.get("APP_PORT") or "8100").strip() or "8100"
    frontend_port = (frontend_values.get("VITE_PORT") or "5273").strip() or "5273"

    api_base_url = (frontend_values.get("VITE_API_BASE_URL") or "").strip()
    if not api_base_url or api_base_url == "auto":
        api_base_url = "/api"

    return {
        "frontend_host": backend_host,
        "frontend_port": frontend_port,
        "frontend_url": f"http://{backend_host}:{frontend_port}/",
        "backend_host": backend_host,
        "backend_port": backend_port,
        "api_base_url": api_base_url,
        "api_proxy_target": f"http://{backend_host}:{backend_port}",
    }


def sync_frontend_runtime_env(config: Dict[str, str]) -> Tuple[bool, str]:
    settings = resolve_frontend_service_settings(config)
    current_values = read_env_file(FRONTEND_ENV_EXAMPLE_PATH)
    current_values.update(read_env_file(FRONTEND_ENV_PATH))
    current_values.update(
        {
            "VITE_API_BASE_URL": settings["api_base_url"],
            "VITE_PORT": settings["frontend_port"],
            "VITE_API_PORT": settings["backend_port"],
            "VITE_API_PROXY_TARGET": settings["api_proxy_target"],
        }
    )

    try:
        FRONTEND_ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
        write_key_value_env_file(
            FRONTEND_ENV_PATH,
            ["VITE_API_BASE_URL", "VITE_PORT", "VITE_API_PORT", "VITE_API_PROXY_TARGET"],
            current_values,
        )
    except Exception as exc:
        return False, f"同步前端环境失败: {exc}"

    return True, f"前端环境已同步: {settings['frontend_url']} -> {settings['api_proxy_target']}"


def sync_frontend_dependencies(timeout_seconds: int = 900) -> Tuple[bool, str]:
    package_json = FRONTEND_DIR / "package.json"
    if not package_json.exists():
        return False, f"未找到 package.json: {package_json}"

    npm_exe = "npm.cmd" if os.name == "nt" else "npm"
    node_exe = "node.exe" if os.name == "nt" else "node"

    try:
        subprocess.run(
            [node_exe, "--version"],
            capture_output=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, "Node.js 未安装，无法同步前端依赖"
    except Exception as exc:
        return False, f"检查 Node.js 失败: {exc}"

    try:
        result = subprocess.run(
            [npm_exe, "install"],
            cwd=FRONTEND_DIR,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, f"未找到 npm: {npm_exe}"
    except Exception as exc:
        return False, f"安装前端依赖失败: {exc}"

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "未知错误").strip()
        return False, detail
    return True, "前端依赖已同步"


def build_frontend(timeout_seconds: int = 600) -> Tuple[bool, str]:
    frontend_dir = ROOT_DIR / "frontend"
    package_json = frontend_dir / "package.json"
    dist_dir = frontend_dir / "dist"
    node_modules = frontend_dir / "node_modules"

    if not package_json.exists():
        return False, "frontend/package.json \u4e0d\u5b58\u5728\uff0c\u8df3\u8fc7\u524d\u7aef\u6784\u5efa"

    if dist_dir.exists() and node_modules.exists():
        return True, "\u524d\u7aef\u6784\u5efa\u4ea7\u7269\u5df2\u5b58\u5728\uff0c\u8df3\u8fc7\u6784\u5efa"

    node_exe = "node.exe" if os.name == "nt" else "node"
    npm_exe = "npm.cmd" if os.name == "nt" else "npm"

    try:
        subprocess.run(
            [node_exe, "--version"],
            capture_output=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, "Node.js \u672a\u5b89\u88c5\uff0c\u8df3\u8fc7\u524d\u7aef\u6784\u5efa"
    except Exception as exc:
        return False, f"Node.js \u68c0\u67e5\u6267\u884c\u5f02\u5e38\uff1a{exc}"

    if not node_modules.exists():
        try:
            result = subprocess.run(
                [npm_exe, "install"],
                cwd=frontend_dir,
                capture_output=True,
                text=True,
                timeout=300,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except FileNotFoundError:
            return False, f"\u672a\u627e\u5230 npm\uff1a{npm_exe}"
        except Exception as exc:
            return False, f"npm install \u6267\u884c\u5f02\u5e38\uff1a{exc}"
        if result.returncode != 0:
            return False, f"npm install \u5931\u8d25\uff1a{result.stderr or result.stdout}"

    try:
        result = subprocess.run(
            [npm_exe, "run", "build"],
            cwd=frontend_dir,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, f"\u672a\u627e\u5230 npm\uff1a{npm_exe}"
    except Exception as exc:
        return False, f"npm run build \u6267\u884c\u5f02\u5e38\uff1a{exc}"
    if result.returncode != 0:
        return False, f"npm run build \u5931\u8d25\uff1a{result.stderr or result.stdout}"

    if not dist_dir.exists():
        return False, "\u6784\u5efa\u5b8c\u6210\u4f46 dist \u76ee\u5f55\u4e0d\u5b58\u5728"

    return True, f"\u524d\u7aef\u6784\u5efa\u6210\u529f\uff1a{dist_dir}"

def sync_keys_to_frontend(config: Dict[str, str]) -> Tuple[bool, str]:
    return sync_frontend_runtime_env(config)


def run_database_backup(config: Dict[str, str], backup_dir: Path, sqlcmd: str) -> Tuple[bool, str]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    conn = extract_db_connection(config)
    missing = [key for key in ("host", "port", "user", "dbname") if not conn.get(key)]
    if missing:
        return False, f"数据库配置不完整，缺少：{', '.join(missing)}"

    filename = f"finflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
    target_file = backup_dir / filename
    query = f"BACKUP DATABASE [{conn['dbname']}] TO DISK = N'{target_file}' WITH INIT, NAME = N'FinFlow Full Backup'"
    cmd = [
        sqlcmd,
        "-S",
        f"{conn['host']},{conn['port']}",
        "-U",
        conn["user"],
        "-d",
        conn["dbname"],
        "-Q",
        query,
    ]
    if conn.get("password"):
        cmd.extend(["-P", conn["password"]])

    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            text=False,
            timeout=600,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, f"未找到 sqlcmd：{sqlcmd}"
    except Exception as exc:
        return False, f"执行 sqlcmd 失败：{exc}"

    stdout_text = decode_console_output(result.stdout or b"")
    stderr_text = decode_console_output(result.stderr or b"")
    if result.returncode != 0:
        if target_file.exists():
            remove_path(target_file)
        return False, (stderr_text or stdout_text or "未知错误").strip()

    return True, str(target_file)


def run_database_backup_with_cleanup(config: Dict[str, str], backup_dir: Path, sqlcmd: str, retention_days: int = 30, retention_count: int = 10) -> Tuple[bool, str]:
    ok, detail = run_database_backup(config, backup_dir, sqlcmd)
    if ok:
        try:
            cleanup_old_backups(backup_dir, retention_days, retention_count)
        except Exception:
            pass
    return ok, detail


def restore_database_from_backup(config: Dict[str, str], bak_file: Path, sqlcmd: str) -> Tuple[bool, str]:
    if not bak_file.exists() or not bak_file.is_file():
        return False, f"备份文件不存在: {bak_file}"
    if bak_file.suffix.lower() != ".bak":
        return False, "恢复文件必须是 .bak 格式"

    conn = extract_db_connection(config)
    missing = [key for key in ("host", "port", "user", "dbname") if not conn.get(key)]
    if missing:
        return False, f"数据库配置不完整，缺少：{', '.join(missing)}"

    db_name = conn["dbname"]
    logical_name = db_name

    try:
        filelist_cmd = f"RESTORE FILELISTONLY FROM DISK = N'{bak_file}'"
        cmd_list = [
            sqlcmd, "-S", f"{conn['host']},{conn['port']}",
            "-U", conn["user"], "-d", "master",
            "-Q", filelist_cmd, "-h", "-1", "-W",
        ]
        if conn.get("password"):
            cmd_list.extend(["-P", conn["password"]])

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(cmd_list, capture_output=True, text=False, timeout=60, creationflags=creationflags)
        stdout_text = decode_console_output(result.stdout or b"")

        for line in stdout_text.splitlines():
            parts = line.split()
            if parts and parts[0].upper() in ("PRIMARY", "LOG"):
                logical_name = parts[0]
                break
    except Exception:
        pass

    data_file = f"C:\\Program Files\\Microsoft SQL Server\\MSSQL13.MSSQLSERVER\\MSSQL\\DATA\\{db_name}.mdf"
    log_file = f"C:\\Program Files\\Microsoft SQL Server\\MSSQL13.MSSQLSERVER\\MSSQL\\DATA\\{db_name}_log.ldf"

    restore_sql = (
        f"USE master;\n"
        f"ALTER DATABASE [{db_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;\n"
        f"RESTORE DATABASE [{db_name}] FROM DISK = N'{bak_file}' "
        f"WITH REPLACE, "
        f"MOVE N'{logical_name}' TO N'{data_file}', "
        f"MOVE N'{logical_name}_log' TO N'{log_file}';\n"
        f"ALTER DATABASE [{db_name}] SET MULTI_USER;\n"
    )

    return execute_sql_script_via_sqlcmd(restore_sql, config, sqlcmd)


def resolve_manager_launcher() -> List[str]:
    if getattr(sys, "frozen", False):
        return [str(Path(sys.executable).resolve())]

    python_exe = Path(sys.executable).resolve()
    pythonw_exe = python_exe.with_name("pythonw.exe")
    if pythonw_exe.exists():
        python_exe = pythonw_exe
    return [str(python_exe), str(Path(__file__).resolve())]


def render_startup_script() -> str:
    launch_parts = " ".join(f'"{part}"' for part in resolve_manager_launcher())
    return f'@echo off\ncd /d "{ROOT_DIR}"\nstart "" {launch_parts}\n'


def detect_release_root(extract_dir: Path) -> Path:
    current = extract_dir
    for _ in range(3):
        if any((current / name).exists() for name in ("backend", "frontend", "tools", "deploy")):
            return current
        children = [item for item in current.iterdir() if item.name != "__MACOSX"]
        if len(children) == 1 and children[0].is_dir():
            current = children[0]
            continue
        break
    return extract_dir


def should_skip_release_path(relative_path: Path) -> bool:
    if not relative_path.parts:
        return True
    normalized = Path(*relative_path.parts)
    if normalized in PROTECTED_EXACT_PATHS:
        return True
    if any(part in SKIP_PART_NAMES for part in normalized.parts):
        return True
    for prefix in PROTECTED_PREFIXES:
        if normalized == prefix or normalized.is_relative_to(prefix):
            return True
    return False


def backup_existing_path(target: Path, backup_root: Path) -> None:
    if not target.exists():
        return
    backup_target = backup_root / target.relative_to(ROOT_DIR)
    if backup_target.exists():
        return
    backup_target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_dir():
        shutil.copytree(target, backup_target)
    else:
        shutil.copy2(target, backup_target)


def overlay_directory(source_dir: Path, target_dir: Path, backup_root: Path) -> int:
    copied = 0
    for source_file in source_dir.rglob("*"):
        if source_file.is_dir():
            continue
        destination = target_dir / source_file.relative_to(source_dir)
        relative_to_root = destination.relative_to(ROOT_DIR)
        if should_skip_release_path(relative_to_root):
            continue
        if destination.exists():
            backup_existing_path(destination, backup_root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, destination)
        copied += 1
    return copied


def replace_directory(source_dir: Path, target_dir: Path, backup_root: Path) -> int:
    if target_dir.exists():
        backup_existing_path(target_dir, backup_root)
        remove_path(target_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_dir, target_dir)
    return sum(1 for item in source_dir.rglob("*") if item.is_file())


def copy_release_root_files(release_root: Path, backup_root: Path) -> int:
    copied = 0
    for item in release_root.iterdir():
        if not item.is_file():
            continue
        relative_path = item.relative_to(release_root)
        if should_skip_release_path(relative_path):
            continue
        target = ROOT_DIR / relative_path
        if target.exists():
            backup_existing_path(target, backup_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(item, target)
        copied += 1
    return copied


def archive_log_file(path: Path, reason: str) -> Path | None:
    if not path.exists():
        return None
    try:
        if path.stat().st_size == 0:
            path.unlink(missing_ok=True)
            return None
    except Exception:
        pass

    LOG_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archived_name = f"{path.stem}_{reason}_{timestamp}{path.suffix}"
    archived_path = LOG_ARCHIVE_DIR / archived_name
    shutil.move(str(path), str(archived_path))
    return archived_path


def probe_http(url: str) -> Tuple[bool, str]:
    request = Request(url, headers={"User-Agent": "FinFlowManager/1.0"})
    try:
        with urlopen(request, timeout=HEALTH_CHECK_TIMEOUT) as response:
            code = int(getattr(response, "status", response.getcode()))
            if 200 <= code < 400:
                return True, f"HTTP {code}"
            return False, f"HTTP {code}"
    except HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except URLError as exc:
        reason = exc.reason.strerror if hasattr(exc.reason, "strerror") else str(exc.reason)
        return False, reason or "连接失败"
    except Exception as exc:
        return False, str(exc)


def probe_backend_health(base_url: str) -> Tuple[bool, str]:
    health_url = base_url.rstrip("/") + "/api/health"
    request = Request(health_url, headers={"User-Agent": "FinFlowManager/1.0", "Accept": "application/json"})
    try:
        with urlopen(request, timeout=HEALTH_CHECK_TIMEOUT) as response:
            code = int(getattr(response, "status", response.getcode()))
            payload = json.loads(decode_console_output(response.read() or b"{}"))
            if 200 <= code < 400 and payload.get("ok") is True:
                return True, f"HTTP {code}"
            detail = payload.get("detail") or payload.get("database") or f"HTTP {code}"
            return False, str(detail)
    except HTTPError as exc:
        try:
            payload = json.loads(decode_console_output(exc.read() or b"{}"))
            detail = payload.get("detail") or payload.get("database") or f"HTTP {exc.code}"
        except Exception:
            detail = f"HTTP {exc.code}"
        return False, str(detail)
    except URLError as exc:
        reason = exc.reason.strerror if hasattr(exc.reason, "strerror") else str(exc.reason)
        return False, reason or "连接失败"
    except Exception as exc:
        return False, str(exc)


def fetch_backend_health_payload(base_url: str) -> Tuple[bool, Dict[str, Any], str]:
    health_url = base_url.rstrip("/") + "/api/health"
    request = Request(health_url, headers={"User-Agent": "FinFlowManager/1.0", "Accept": "application/json"})
    try:
        with urlopen(request, timeout=HEALTH_CHECK_TIMEOUT) as response:
            code = int(getattr(response, "status", response.getcode()))
            payload = json.loads(decode_console_output(response.read() or b"{}"))
            return 200 <= code < 400, payload, f"HTTP {code}"
    except HTTPError as exc:
        try:
            payload = json.loads(decode_console_output(exc.read() or b"{}"))
        except Exception:
            payload = {}
        return False, payload, f"HTTP {exc.code}"
    except URLError as exc:
        reason = exc.reason.strerror if hasattr(exc.reason, "strerror") else str(exc.reason)
        return False, {}, reason or "连接失败"
    except Exception as exc:
        return False, {}, str(exc)


def execute_sql_script_via_sqlcmd(sql_content: str, config: Dict[str, str], sqlcmd: str) -> Tuple[bool, str]:
    conn = extract_db_connection(config)
    missing = [key for key in ("host", "port", "user", "dbname") if not conn.get(key)]
    if missing:
        return False, f"数据库配置不完整，缺少：{', '.join(missing)}"

    tmp_sql = Path(tempfile.gettempdir()) / f"finflow_migration_{int(time.time())}.sql"
    try:
        tmp_sql.write_text(sql_content, encoding="utf-8")
        cmd = [
            sqlcmd,
            "-S", f"{conn['host']},{conn['port']}",
            "-U", conn["user"],
            "-d", conn["dbname"],
            "-i", str(tmp_sql),
            "-b",
        ]
        if conn.get("password"):
            cmd.extend(["-P", conn["password"]])

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(cmd, capture_output=True, text=False, timeout=300, creationflags=creationflags)
        stdout_text = decode_console_output(result.stdout or b"")
        stderr_text = decode_console_output(result.stderr or b"")
        if result.returncode != 0:
            return False, (stderr_text or stdout_text or "执行失败").strip()
        return True, "SQL 脚本执行成功"
    except Exception as exc:
        return False, f"执行异常：{exc}"
    finally:
        tmp_sql.unlink(missing_ok=True)


def rotate_log_file(path: Path, max_size_mb: float) -> Path | None:
    if not path.exists():
        return None
    try:
        size_mb = path.stat().st_size / (1024 * 1024)
        if size_mb < max_size_mb:
            return None
        LOG_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_name = f"{path.stem}_rotated_{timestamp}{path.suffix}"
        archived_path = LOG_ARCHIVE_DIR / archived_name
        shutil.move(str(path), str(archived_path))
        path.touch()
        return archived_path
    except Exception:
        return None


def cleanup_old_backups(backup_dir: Path, retention_days: int, retention_count: int) -> Tuple[int, str]:
    if not backup_dir.exists():
        return 0, "备份目录不存在"
    bak_files = sorted(backup_dir.glob("*.bak"), key=lambda p: p.stat().st_mtime, reverse=True)
    cutoff_time = time.time() - (retention_days * 86400)
    removed = 0
    for idx, f in enumerate(bak_files):
        should_remove = False
        if f.stat().st_mtime < cutoff_time:
            should_remove = True
        if idx >= retention_count:
            should_remove = True
        if should_remove:
            try:
                f.unlink()
                removed += 1
            except Exception:
                pass
    return removed, f"已清理 {removed} 个过期备份"


def cleanup_old_archived_logs(retention_days: int) -> Tuple[int, str]:
    if not LOG_ARCHIVE_DIR.exists():
        return 0, "归档目录不存在"
    cutoff_time = time.time() - (retention_days * 86400)
    removed = 0
    for f in LOG_ARCHIVE_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff_time:
            try:
                f.unlink()
                removed += 1
            except Exception:
                pass
    return removed, f"已清理 {removed} 个过期归档日志"


def send_webhook_notification(webhook_url: str, title: str, message: str) -> bool:
    if not webhook_url or not webhook_url.startswith("http"):
        return False
    payload = {
        "msgtype": "text",
        "text": {"content": f"[FinFlow {title}]\n{message}"},
    }
    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = Request(webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(req, timeout=5) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def check_database_connectivity(config: Dict[str, str], sqlcmd: str = "sqlcmd") -> Tuple[bool, str]:
    conn, missing = get_database_connection_issues(config)
    if missing:
        return False, f"数据库配置不完整，缺少：{', '.join(missing)}"

    try:
        cmd = [
            sqlcmd,
            "-S", f"{conn['host']},{conn['port']}",
            "-U", conn["user"],
            "-d", conn["dbname"],
            "-Q", "SELECT 1",
            "-b",
        ]
        if conn.get("password"):
            cmd.extend(["-P", conn["password"]])

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(cmd, capture_output=True, text=False, timeout=15, creationflags=creationflags)
        if result.returncode == 0:
            return True, f"数据库 {conn['dbname']} 连接正常"
        stderr_text = decode_console_output(result.stderr or b"")
        stdout_text = decode_console_output(result.stdout or b"")
        return False, (stderr_text or stdout_text or "连接失败").strip()
    except FileNotFoundError:
        return False, f"未找到 sqlcmd：{sqlcmd}"
    except Exception as exc:
        return False, f"连接检测异常：{exc}"


class BackendProcessController:
    def __init__(self) -> None:
        self.process: subprocess.Popen | None = None
        self.stdout_handle = None
        self.stderr_handle = None
        self.user_stopped = False
        self.last_start_attempt = 0.0
        self.last_exit_code: int | None = None
        self.last_exit_at = 0.0
        self.last_started_at = 0.0
        self.consecutive_failed_starts = 0
        self.auto_restart_suppressed = False
        self.last_session_marker = ""
        self.last_session_started_label = ""
        write_runtime_state(
            backend_pid=0,
            user_stopped=False,
            last_start_attempt=self.last_start_attempt,
            last_started_at=self.last_started_at,
            last_exit_code=None,
            auto_restart_suppressed=False,
            last_session_marker=self.last_session_marker,
            last_session_started_label=self.last_session_started_label,
        )

    def _close_handles(self) -> None:
        for handle in (self.stdout_handle, self.stderr_handle):
            try:
                if handle:
                    handle.flush()
                    handle.close()
            except Exception:
                pass
        self.stdout_handle = None
        self.stderr_handle = None

    def is_running(self) -> bool:
        return safe_process_is_running(self.process)

    def capture_exit(self) -> int | None:
        if self.process is None:
            return None
        code = safe_popen_poll(self.process)
        if code is None:
            return None
        self.last_exit_code = code
        self.last_exit_at = time.time()
        self.process = None
        self._close_handles()
        write_runtime_state(
            backend_pid=0,
            last_exit_code=code,
            last_exit_at=self.last_exit_at,
            user_stopped=self.user_stopped,
            consecutive_failed_starts=self.consecutive_failed_starts,
            auto_restart_suppressed=self.auto_restart_suppressed,
            last_session_marker=self.last_session_marker,
            last_session_started_label=self.last_session_started_label,
        )
        return code

    def start(self, config: Dict[str, str]) -> Tuple[bool, str]:
        if self.is_running():
            return False, "后端已在运行"

        python_exe = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
        if not python_exe.exists():
            return False, "未找到后端虚拟环境，请先在 backend/.venv 安装 Python 依赖"
        if not ENV_PATH.exists():
            return False, "未找到 backend/.env，请先在配置页保存配置"
        if not KEY_PATH.exists():
            return False, "未找到 backend/.encryption.key，请先生成或放入正确密钥"

        ensure_log_dir()
        max_size = 20.0
        try:
            state = read_state_file(STATE_PATH)
            max_size = float(state.get("log_max_size_mb", 20))
        except Exception:
            pass
        for log_path in (STDOUT_LOG, STDERR_LOG):
            rotate_log_file(log_path, max_size)

        env = os.environ.copy()
        env.update(config)
        env["PYTHONUNBUFFERED"] = "1"
        env["APP_RELOAD"] = "false"
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        host = (config.get("APP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
        port = (config.get("APP_PORT") or "8100").strip() or "8100"
        browser_host = resolve_browser_host(host)

        if port.isdigit():
            port_num = int(port)
            if is_port_open(browser_host, port_num) or (host == "0.0.0.0" and is_port_open("127.0.0.1", port_num)):
                if can_connect_http(browser_host, port_num):
                    return False, f"端口 {port} 已被现有服务占用，且页面可访问，请先停止旧实例后再启动"
                return False, f"端口 {port} 已被其他进程占用，请先释放端口后再启动"

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        session_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.last_session_started_label = session_label
        self.last_session_marker = f"===== FinFlowManager Session Start {session_label} ====="
        for log_path in (STDOUT_LOG, STDERR_LOG):
            with open(log_path, "a", encoding="utf-8", errors="ignore") as marker_handle:
                marker_handle.write(f"\n{self.last_session_marker}\n")
        self.stdout_handle = open(STDOUT_LOG, "a", encoding="utf-8", errors="ignore")
        self.stderr_handle = open(STDERR_LOG, "a", encoding="utf-8", errors="ignore")

        try:
            self.process = subprocess.Popen(
                [str(python_exe), "-m", "uvicorn", "main:app", "--host", host, "--port", port],
                cwd=BACKEND_DIR,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=self.stdout_handle,
                stderr=self.stderr_handle,
                creationflags=creationflags,
            )
        except Exception as exc:
            self._close_handles()
            self.process = None
            return False, f"启动失败：{exc}"

        self.user_stopped = False
        self.last_start_attempt = time.time()
        self.last_started_at = self.last_start_attempt
        self.last_exit_code = None
        self.auto_restart_suppressed = False
        return True, f"已发送启动命令，PID={self.process.pid}"

    def stop(self) -> Tuple[bool, str]:
        if not self.is_running():
            self.capture_exit()
            return False, "后端未运行"

        self.user_stopped = True
        write_runtime_state(user_stopped=True)
        try:
            safe_terminate_process(self.process, timeout_seconds=10)
        except Exception:
            try:
                pid = safe_process_pid(self.process)
                if pid > 0:
                    os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        finally:
            self.capture_exit()

        return True, "后端已停止"

    def restart(self, config: Dict[str, str]) -> Tuple[bool, str]:
        self.stop()
        time.sleep(1)
        return self.start(config)

    def poll_status(self) -> str:
        return _clean_poll_status(self)

    def register_failed_start_if_needed(self, fast_fail_window_seconds: int = 15, max_failures: int = 3) -> bool:
        if self.last_exit_at <= 0 or self.last_started_at <= 0:
            return False
        runtime = self.last_exit_at - self.last_started_at
        if runtime < 0 or runtime > fast_fail_window_seconds:
            self.consecutive_failed_starts = 0
            return False

        self.consecutive_failed_starts += 1
        if self.consecutive_failed_starts >= max_failures:
            self.auto_restart_suppressed = True
        return True

    def clear_restart_failure_state(self) -> None:
        self.consecutive_failed_starts = 0
        self.auto_restart_suppressed = False
        write_runtime_state(consecutive_failed_starts=0, auto_restart_suppressed=False)


class FrontendProcessController:
    def __init__(self) -> None:
        state = read_runtime_state()
        frontend_pid = int(state.get("frontend_pid") or 0)
        self.process: subprocess.Popen | SimpleNamespace | None = (
            SimpleNamespace(pid=frontend_pid) if is_process_alive(frontend_pid) else None
        )
        self.stdout_handle = None
        self.stderr_handle = None
        self.user_stopped = bool(state.get("frontend_user_stopped", False))
        self.last_start_attempt = float(state.get("frontend_last_start_attempt", 0.0) or 0.0)
        self.last_exit_code = state.get("frontend_last_exit_code")
        self.last_exit_at = float(state.get("frontend_last_exit_at", 0.0) or 0.0)
        self.last_started_at = float(state.get("frontend_last_started_at", 0.0) or 0.0)
        self.consecutive_failed_starts = int(state.get("frontend_consecutive_failed_starts", 0) or 0)
        self.auto_restart_suppressed = bool(state.get("frontend_auto_restart_suppressed", False))
        self.last_session_marker = str(state.get("frontend_last_session_marker", "") or "")
        self.last_session_started_label = str(state.get("frontend_last_session_started_label", "") or "")
        if frontend_pid > 0 and self.process is None:
            write_runtime_state(frontend_pid=0)

    def _close_handles(self) -> None:
        for handle in (self.stdout_handle, self.stderr_handle):
            try:
                if handle:
                    handle.flush()
                    handle.close()
            except Exception:
                pass
        self.stdout_handle = None
        self.stderr_handle = None

    def _sync_runtime_state(self, **updates: Any) -> None:
        write_runtime_state(
            frontend_pid=safe_process_pid(self.process),
            frontend_user_stopped=self.user_stopped,
            frontend_last_start_attempt=self.last_start_attempt,
            frontend_last_exit_code=self.last_exit_code,
            frontend_last_exit_at=self.last_exit_at,
            frontend_last_started_at=self.last_started_at,
            frontend_consecutive_failed_starts=self.consecutive_failed_starts,
            frontend_auto_restart_suppressed=self.auto_restart_suppressed,
            frontend_last_session_marker=self.last_session_marker,
            frontend_last_session_started_label=self.last_session_started_label,
            **updates,
        )

    def is_running(self) -> bool:
        return safe_process_is_running(self.process)

    def capture_exit(self) -> int | None:
        if self.process is None:
            return None
        if isinstance(self.process, subprocess.Popen):
            code = safe_popen_poll(self.process)
            if code is None:
                return None
        else:
            pid = safe_process_pid(self.process)
            if is_process_alive(pid):
                return None
            code = self.last_exit_code

        self.last_exit_code = code
        self.last_exit_at = time.time()
        self.process = None
        self._close_handles()
        self._sync_runtime_state(frontend_pid=0)
        return code

    def start(self, config: Dict[str, str]) -> Tuple[bool, str]:
        if self.is_running():
            return False, "前端已在运行"

        package_json = FRONTEND_DIR / "package.json"
        vite_cli = FRONTEND_DIR / "node_modules" / "vite" / "bin" / "vite.js"
        node_exe = "node.exe" if os.name == "nt" else "node"
        if not package_json.exists():
            return False, "未找到 frontend/package.json，请确认前端工程目录完整"
        if not vite_cli.exists():
            return False, "未找到前端运行依赖，请先执行“同步前端依赖”"
        try:
            subprocess.run(
                [node_exe, "--version"],
                capture_output=True,
                timeout=5,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except FileNotFoundError:
            return False, "未找到 Node.js，请先安装 Node.js 后再启动前端"
        except Exception as exc:
            return False, f"检查 Node.js 失败: {exc}"

        ok, detail = sync_frontend_runtime_env(config)
        if not ok:
            return False, detail

        ensure_log_dir()
        max_size = 20.0
        try:
            state = read_state_file(STATE_PATH)
            max_size = float(state.get("log_max_size_mb", 20))
        except Exception:
            pass
        for log_path in (FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG):
            rotate_log_file(log_path, max_size)

        settings = resolve_frontend_service_settings(config)
        frontend_host = settings["frontend_host"]
        frontend_port = settings["frontend_port"]
        if frontend_port.isdigit():
            port_num = int(frontend_port)
            if is_port_open(frontend_host, port_num):
                if can_connect_http(frontend_host, port_num):
                    return False, f"前端端口 {frontend_port} 已被现有服务占用，请先停止旧实例后再启动"
                return False, f"前端端口 {frontend_port} 已被其他进程占用，请先释放端口后再启动"

        env = os.environ.copy()
        env.update(read_env_file(FRONTEND_ENV_EXAMPLE_PATH))
        env.update(read_env_file(FRONTEND_ENV_PATH))
        env["BROWSER"] = "none"
        env["FORCE_COLOR"] = "0"

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        session_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.last_session_started_label = session_label
        self.last_session_marker = f"===== FinFlowManager Frontend Session Start {session_label} ====="
        for log_path in (FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG):
            with open(log_path, "a", encoding="utf-8", errors="ignore") as marker_handle:
                marker_handle.write(f"\n{self.last_session_marker}\n")
        self.stdout_handle = open(FRONTEND_STDOUT_LOG, "a", encoding="utf-8", errors="ignore")
        self.stderr_handle = open(FRONTEND_STDERR_LOG, "a", encoding="utf-8", errors="ignore")

        try:
            self.process = subprocess.Popen(
                [node_exe, str(vite_cli), "--host", "0.0.0.0", "--port", frontend_port],
                cwd=FRONTEND_DIR,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=self.stdout_handle,
                stderr=self.stderr_handle,
                creationflags=creationflags,
            )
        except Exception as exc:
            self._close_handles()
            self.process = None
            return False, f"启动前端失败: {exc}"

        self.user_stopped = False
        self.last_start_attempt = time.time()
        self.last_started_at = self.last_start_attempt
        self.last_exit_code = None
        self.auto_restart_suppressed = False
        self._sync_runtime_state()
        return True, f"前端已启动，PID={self.process.pid}，访问地址 {settings['frontend_url']}"

    def stop(self) -> Tuple[bool, str]:
        if not self.is_running():
            self.capture_exit()
            return False, "前端未运行"

        self.user_stopped = True
        self._sync_runtime_state(frontend_user_stopped=True)
        try:
            if isinstance(self.process, subprocess.Popen):
                safe_terminate_process(self.process, timeout_seconds=10)
            else:
                pid = safe_process_pid(self.process)
                if pid > 0:
                    os.kill(pid, signal.SIGTERM)
        except Exception:
            try:
                pid = safe_process_pid(self.process)
                if pid > 0:
                    os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        finally:
            self.capture_exit()

        return True, "前端已停止"

    def restart(self, config: Dict[str, str]) -> Tuple[bool, str]:
        self.stop()
        time.sleep(1)
        return self.start(config)

    def poll_status(self) -> str:
        return _clean_poll_status(self)

    def register_failed_start_if_needed(self, fast_fail_window_seconds: int = 15, max_failures: int = 3) -> bool:
        if self.last_exit_at <= 0 or self.last_started_at <= 0:
            return False
        runtime = self.last_exit_at - self.last_started_at
        if runtime < 0 or runtime > fast_fail_window_seconds:
            self.consecutive_failed_starts = 0
            self._sync_runtime_state(frontend_consecutive_failed_starts=0)
            return False

        self.consecutive_failed_starts += 1
        if self.consecutive_failed_starts >= max_failures:
            self.auto_restart_suppressed = True
        self._sync_runtime_state()
        return True

    def clear_restart_failure_state(self) -> None:
        self.consecutive_failed_starts = 0
        self.auto_restart_suppressed = False
        self._sync_runtime_state(frontend_consecutive_failed_starts=0, frontend_auto_restart_suppressed=False)


def resolve_service_host_launcher() -> List[str]:
    if getattr(sys, "frozen", False):
        backend_python = get_backend_python_executable()
        manager_script = ROOT_DIR / "tools" / "finflow_manager.py"
        if backend_python.exists() and manager_script.exists():
            return [str(backend_python), str(manager_script), "--service-run"]
        return [str(Path(sys.executable).resolve()), "--service-run"]

    python_exe = Path(sys.executable).resolve()
    if python_exe.name.lower() == "pythonw.exe":
        candidate = python_exe.with_name("python.exe")
        if candidate.exists():
            python_exe = candidate
    return [str(python_exe), str(Path(__file__).resolve()), "--service-run"]


def resolve_frontend_host_launcher() -> List[str]:
    if getattr(sys, "frozen", False):
        backend_python = get_backend_python_executable()
        manager_script = ROOT_DIR / "tools" / "finflow_manager.py"
        if backend_python.exists() and manager_script.exists():
            return [str(backend_python), str(manager_script), "--frontend-run"]
        return [str(Path(sys.executable).resolve()), "--frontend-run"]

    python_exe = Path(sys.executable).resolve()
    if python_exe.name.lower() == "pythonw.exe":
        candidate = python_exe.with_name("python.exe")
        if candidate.exists():
            python_exe = candidate
    return [str(python_exe), str(Path(__file__).resolve()), "--frontend-run"]


class FrontendStaticRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args: Any, directory: str | None = None, **kwargs: Any) -> None:
        super().__init__(*args, directory=directory, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        try:
            message = format % args
        except Exception:
            message = format
        print(f"[frontend] {self.address_string()} - {message}")

    def do_GET(self) -> None:
        requested = self.translate_path(self.path)
        if self.path.startswith("/api/"):
            self.send_error(502, "Frontend static host cannot proxy /api directly")
            return
        if os.path.exists(requested) or self.path in {"/", "/index.html"}:
            return super().do_GET()
        original_path = self.path
        self.path = "/index.html"
        try:
            return super().do_GET()
        finally:
            self.path = original_path


def run_frontend_static_host() -> int:
    ensure_log_dir()
    layout_ok, layout_issues = evaluate_manager_runtime_layout()
    if not layout_ok:
        print(format_manager_runtime_layout_issues(layout_issues))
        return 1
    config = load_effective_config_from_disk()
    settings = resolve_frontend_service_settings(config)
    frontend_host = settings["frontend_host"]
    frontend_port = settings["frontend_port"]
    if not DIST_DIR.exists() or not (DIST_DIR / "index.html").exists():
        print(f"前端静态资源不存在：{DIST_DIR}")
        return 1
    if not frontend_port.isdigit():
        print(f"前端端口无效：{frontend_port}")
        return 1

    stop_event = threading.Event()

    def _request_stop(*_args: Any) -> None:
        stop_event.set()

    for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None), getattr(signal, "SIGBREAK", None)):
        if sig is None:
            continue
        try:
            signal.signal(sig, _request_stop)
        except Exception:
            pass

    handler = partial(FrontendStaticRequestHandler, directory=str(DIST_DIR))
    try:
        httpd = ThreadingHTTPServer((frontend_host, int(frontend_port)), handler)
    except OSError as exc:
        print(f"前端静态服务启动失败：{exc}")
        return 1
    httpd.timeout = 1.0
    print(f"前端静态服务已启动：http://{frontend_host}:{frontend_port}/")
    try:
        while not stop_event.is_set():
            httpd.handle_request()
    finally:
        try:
            httpd.server_close()
        except Exception:
            pass
    print("前端静态服务已停止")
    return 0


def read_managed_runtime_state() -> Dict[str, Any]:
    state = read_runtime_state()
    host_pid = int(state.get("service_host_pid") or 0)
    backend_pid = int(state.get("backend_pid") or 0)
    frontend_pid = int(state.get("frontend_pid") or 0)
    host_alive = is_process_alive(host_pid)
    backend_alive = is_process_alive(backend_pid)
    frontend_alive = is_process_alive(frontend_pid)

    normalized = dict(state)
    normalized["service_host_alive"] = host_alive
    normalized["backend_alive"] = backend_alive
    normalized["frontend_alive"] = frontend_alive
    if not host_alive:
        normalized["service_host_pid"] = 0
        if not backend_alive:
            normalized["backend_pid"] = 0
    if not backend_alive:
        normalized["backend_pid"] = 0
    if not frontend_alive:
        normalized["frontend_pid"] = 0
    return normalized


class ManagedBackendController:
    def __init__(self) -> None:
        self.process: SimpleNamespace | None = None

    def _state(self) -> Dict[str, Any]:
        state = read_managed_runtime_state()
        backend_pid = int(state.get("backend_pid") or 0)
        self.process = SimpleNamespace(pid=backend_pid) if backend_pid > 0 else None
        return state

    @property
    def user_stopped(self) -> bool:
        return bool(self._state().get("user_stopped", False))

    @property
    def last_start_attempt(self) -> float:
        return float(self._state().get("last_start_attempt", 0.0) or 0.0)

    @property
    def last_exit_code(self) -> int | None:
        return self._state().get("last_exit_code")

    @property
    def last_exit_at(self) -> float:
        return float(self._state().get("last_exit_at", 0.0) or 0.0)

    @property
    def consecutive_failed_starts(self) -> int:
        return int(self._state().get("consecutive_failed_starts", 0) or 0)

    @consecutive_failed_starts.setter
    def consecutive_failed_starts(self, value: int) -> None:
        write_runtime_state(consecutive_failed_starts=int(value or 0))

    @property
    def auto_restart_suppressed(self) -> bool:
        return bool(self._state().get("auto_restart_suppressed", False))

    @auto_restart_suppressed.setter
    def auto_restart_suppressed(self, value: bool) -> None:
        write_runtime_state(auto_restart_suppressed=bool(value))

    @property
    def last_session_marker(self) -> str:
        return str(self._state().get("last_session_marker", "") or "")

    @property
    def last_session_started_label(self) -> str:
        return str(self._state().get("last_session_started_label", "") or "")

    def is_running(self) -> bool:
        state = self._state()
        return bool(state.get("service_host_alive") or state.get("backend_alive"))

    def clear_restart_failure_state(self) -> None:
        write_runtime_state(consecutive_failed_starts=0, auto_restart_suppressed=False)

    def register_failed_start_if_needed(self, fast_fail_window_seconds: int = 15, max_failures: int = 3) -> bool:
        state = self._state()
        last_exit_at = float(state.get("last_exit_at", 0.0) or 0.0)
        last_started_at = float(state.get("last_started_at", 0.0) or 0.0)
        runtime = last_exit_at - last_started_at
        if last_exit_at <= 0 or last_started_at <= 0 or runtime < 0 or runtime > fast_fail_window_seconds:
            write_runtime_state(consecutive_failed_starts=0)
            return False

        failures = int(state.get("consecutive_failed_starts", 0) or 0) + 1
        suppressed = failures >= max_failures
        write_runtime_state(consecutive_failed_starts=failures, auto_restart_suppressed=suppressed)
        return True

    def start(self, _config: Dict[str, str] | None = None) -> Tuple[bool, str]:
        state = self._state()
        if state.get("service_host_alive"):
            host_pid = int(state.get("service_host_pid") or 0)
            return False, f"服务宿主已在运行，PID={host_pid}"

        ensure_runtime_dir()
        clear_stop_request()
        ensure_log_dir()
        creationflags = 0
        for flag_name in ("CREATE_NO_WINDOW", "DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP"):
            creationflags |= getattr(subprocess, flag_name, 0)

        launcher = resolve_service_host_launcher()
        with open(SERVICE_HOST_LOG, "a", encoding="utf-8", errors="ignore") as service_log:
            process = subprocess.Popen(
                launcher,
                cwd=ROOT_DIR,
                stdin=subprocess.DEVNULL,
                stdout=service_log,
                stderr=service_log,
                creationflags=creationflags,
            )

        deadline = time.time() + 8
        while time.time() < deadline:
            current = self._state()
            if current.get("service_host_alive"):
                return True, f"服务宿主已启动，PID={int(current.get('service_host_pid') or process.pid)}"
            time.sleep(0.25)
        return True, f"已发送服务宿主启动命令，PID={process.pid}"

    def stop(self) -> Tuple[bool, str]:
        state = self._state()
        host_pid = int(state.get("service_host_pid") or 0)
        backend_pid = int(state.get("backend_pid") or 0)
        if host_pid <= 0 and backend_pid <= 0:
            return False, "后端未运行"

        create_stop_request()
        deadline = time.time() + 20
        while time.time() < deadline:
            current = self._state()
            if not current.get("service_host_alive") and not current.get("backend_alive"):
                clear_stop_request()
                return True, "后端已停止"
            time.sleep(0.5)

        if host_pid > 0 and is_process_alive(host_pid):
            try:
                os.kill(host_pid, signal.SIGTERM)
            except OSError:
                pass
        if backend_pid > 0 and is_process_alive(backend_pid):
            try:
                os.kill(backend_pid, signal.SIGTERM)
            except OSError:
                pass
        clear_stop_request()
        return True, "已发送停止命令，正在结束后台宿主"

    def restart(self, config: Dict[str, str] | None = None) -> Tuple[bool, str]:
        self.stop()
        time.sleep(1)
        return self.start(config)

    def poll_status(self) -> str:
        return _managed_backend_poll_status(self)


class BackendServiceHost:
    def __init__(self) -> None:
        append_text_log(SERVICE_HOST_LOG, "===== BackendServiceHost.__init__ =====")
        self.backend = BackendProcessController()
        self.stop_event = threading.Event()
        append_text_log(SERVICE_HOST_LOG, "===== BackendServiceHost.__init__ ready =====")

    def request_stop(self, *_args: Any) -> None:
        self.stop_event.set()

    def _sync_runtime_state(self) -> None:
        backend_pid = safe_process_pid(self.backend.process) if self.backend.is_running() else 0
        write_runtime_state(
            service_host_pid=os.getpid(),
            backend_pid=backend_pid,
            user_stopped=self.backend.user_stopped,
            last_start_attempt=self.backend.last_start_attempt,
            last_exit_code=self.backend.last_exit_code,
            last_exit_at=self.backend.last_exit_at,
            last_started_at=self.backend.last_started_at,
            consecutive_failed_starts=self.backend.consecutive_failed_starts,
            auto_restart_suppressed=self.backend.auto_restart_suppressed,
            last_session_marker=self.backend.last_session_marker,
            last_session_started_label=self.backend.last_session_started_label,
        )

    def run(self) -> int:
        append_text_log(SERVICE_HOST_LOG, "===== BackendServiceHost.run begin =====")
        ensure_runtime_dir()
        ensure_log_dir()
        clear_stop_request()
        reset_runtime_state()
        write_runtime_state(service_host_pid=os.getpid(), user_stopped=False)
        append_text_log(SERVICE_HOST_LOG, build_runtime_state_snapshot_text())

        for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None), getattr(signal, "SIGBREAK", None)):
            if sig is None:
                continue
            try:
                signal.signal(sig, self.request_stop)
            except Exception:
                pass

        config = load_effective_config_from_disk()
        append_text_log(SERVICE_HOST_LOG, "===== BackendServiceHost.run loaded config =====")
        ok, message = self.backend.start(config)
        append_text_log(SERVICE_HOST_LOG, f"===== BackendServiceHost.run backend.start ok={ok} =====")
        append_text_log(SERVICE_HOST_LOG, message)
        self._sync_runtime_state()
        append_text_log(SERVICE_HOST_LOG, build_runtime_state_snapshot_text())
        if not ok:
            print(message)

        append_text_log(SERVICE_HOST_LOG, "===== BackendServiceHost.run loop enter =====")
        while not self.stop_event.is_set():
            if STOP_REQUEST_PATH.exists():
                self.backend.user_stopped = True
                self.stop_event.set()
                break

            self.backend.capture_exit()
            manager_state = read_state_file(STATE_PATH)
            if (
                manager_state.get("auto_restart_backend", True)
                and not self.backend.is_running()
                and not self.backend.user_stopped
                and not self.backend.auto_restart_suppressed
                and time.time() - self.backend.last_start_attempt > 8
                and self.backend.last_start_attempt > 0
            ):
                self.backend.register_failed_start_if_needed()
                self.backend.start(load_effective_config_from_disk())
            if self.backend.is_running():
                self.backend.consecutive_failed_starts = 0

            self._sync_runtime_state()
            time.sleep(1)

        if self.backend.is_running():
            self.backend.stop()
        self.backend.capture_exit()
        clear_stop_request()
        reset_runtime_state()
        return 0

    def run_daemon(self) -> int:
        ensure_runtime_dir()
        ensure_log_dir()
        clear_stop_request()
        reset_runtime_state()
        write_runtime_state(service_host_pid=os.getpid(), user_stopped=False)

        for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None), getattr(signal, "SIGBREAK", None)):
            if sig is None:
                continue
            try:
                signal.signal(sig, self.request_stop)
            except Exception:
                pass

        config = load_effective_config_from_disk()
        host_label = config.get("APP_HOST", "127.0.0.1")
        port_label = config.get("APP_PORT", "8100")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FinFlow 守护进程启动 (PID {os.getpid()})")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 监听地址: {host_label}:{port_label}")

        ok, message = self.backend.start(config)
        self._sync_runtime_state()
        if not ok:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 启动失败: {message}")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 后端已启动: {message}")

        last_health_time = 0.0
        while not self.stop_event.is_set():
            if STOP_REQUEST_PATH.exists():
                self.backend.user_stopped = True
                self.stop_event.set()
                break

            self.backend.capture_exit()
            manager_state = read_state_file(STATE_PATH)

            if self.backend.is_running():
                self.backend.consecutive_failed_starts = 0
                now = time.time()
                if now - last_health_time > 30:
                    last_health_time = now
                    base_url = f"http://{resolve_browser_host(host_label)}:{port_label}"
                    h_ok, h_detail = probe_backend_health(base_url)
                    status_icon = "✓" if h_ok else "✗"
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 健康检查 {status_icon}: {h_detail}")
            else:
                if (
                    manager_state.get("auto_restart_backend", True)
                    and not self.backend.user_stopped
                    and not self.backend.auto_restart_suppressed
                    and time.time() - self.backend.last_start_attempt > 8
                    and self.backend.last_start_attempt > 0
                ):
                    self.backend.register_failed_start_if_needed()
                    if self.backend.auto_restart_suppressed:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 启动失败过多，暂停自动拉起")
                    else:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 后端异常退出，正在自动拉起...")
                        ok, message = self.backend.start(load_effective_config_from_disk())
                        if ok:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 自动拉起成功: {message}")
                        else:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 自动拉起失败: {message}")

            self._sync_runtime_state()
            time.sleep(1)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 收到停止信号，正在关闭...")
        if self.backend.is_running():
            self.backend.stop()
        self.backend.capture_exit()
        clear_stop_request()
        reset_runtime_state()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FinFlow 守护进程已退出")
        return 0


class FinFlowManagerApp:
    def __init__(self) -> None:
        ensure_gui_dependencies()
        ensure_tcl_tk_environment()
        self.root = tk.Tk()
        self.root.title("FinFlow 管理器")
        self.root.geometry("980x760")
        self.root.minsize(900, 680)
        
        try:
            if MANAGER_ICON_ICO.exists():
                self.root.iconbitmap(MANAGER_ICON_ICO)
            else:
                high_res_icon = create_high_res_icon()
                window_icon_path = ROOT_DIR / "deploy" / "windows" / "finflow_icon_temp.ico"
                high_res_icon.save(window_icon_path, format="ICO", sizes=[(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)])
                self.root.iconbitmap(window_icon_path)
                window_icon_path.unlink(missing_ok=True)
        except Exception:
            pass

        self.backend = ManagedBackendController()
        self.frontend = FrontendProcessController()
        self.manager_state = read_state_file(STATE_PATH)
        self.config_values, self.extra_env_values = self.load_config_values()
        self.form_vars: Dict[str, tk.StringVar] = {}
        self.status_vars: Dict[str, tk.StringVar] = {}
        self.status_badges: Dict[str, tk.Label] = {}
        self.service_action_buttons: Dict[str, ttk.Button] = {}
        self.manager_option_vars: Dict[str, tk.BooleanVar] = {}
        self.ops_vars: Dict[str, tk.StringVar] = {}
        self.init_ops_vars()
        self.log_choice = tk.StringVar(value=FF_LOG_CHOICES[0])
        self.log_auto_refresh = tk.BooleanVar(value=True)
        self.log_status_var = tk.StringVar(value="\u5f53\u524d\u663e\u793a\uff1a\u5386\u53f2\u65e5\u5fd7")
        self.tray_icon: Icon | None = None
        self.tray_thread: threading.Thread | None = None
        self.exiting = False
        self.last_notified_exit_at = 0.0
        self.last_notified_frontend_exit_at = 0.0
        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        self.last_health_state = "unknown"
        self.last_database_status_state = "unknown"
        self.deploy_running = False
        self.deploy_thread: threading.Thread | None = None
        self.deploy_log_queue: queue.Queue[str] = queue.Queue()
        self.ui_callback_queue: queue.Queue[tuple[Any, Any, Any]] = queue.Queue()
        self.status_job: str | None = None
        self.log_job: str | None = None
        self.health_job: str | None = None
        self.db_job: str | None = None
        self.ui_queue_job: str | None = None
        self.config_nav_items: Dict[str, Dict[str, Any]] = {}
        self.ops_nav_items: Dict[str, Dict[str, Any]] = {}
        self.env_nav_items: Dict[str, Dict[str, Any]] = {}
        self.env_section_frames: Dict[str, ttk.Frame] = {}
        self.env_text_widgets: Dict[str, scrolledtext.ScrolledText] = {}

        self.build_ui()
        self.create_tray_icon()
        self.sync_startup_entry(show_message=False)

        self.root.protocol("WM_DELETE_WINDOW", self.on_close_window)
        self.schedule_status_refresh(800)
        self.schedule_log_refresh(1200)
        self.schedule_health_refresh(1800)
        self.schedule_db_refresh(2200)
        self.schedule_ui_queue_refresh(150)
        self.root.after(1500, self.maybe_start_services_on_launch)

    def init_ops_vars(self) -> None:
        for key in OPS_STATE_KEYS:
            self.ops_vars[key] = tk.StringVar(value=str(self.manager_state.get(key, DEFAULT_STATE.get(key, ""))))

    def load_config_values(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        values = dict(DEFAULT_ENV)
        values.update(read_env_file(ENV_EXAMPLE_PATH))
        current = read_env_file(ENV_PATH)
        values.update(current)
        extra = {k: v for k, v in current.items() if k not in DEFAULT_ENV}
        values["APP_RELOAD"] = "false"
        return values, extra

    def save_config_values(self) -> None:
        data = {key: var.get().strip() for key, var in self.form_vars.items()}
        data["APP_RELOAD"] = "false"

        port = data.get("APP_PORT", "")
        if not port.isdigit():
            raise ValueError("APP_PORT \u5fc5\u987b\u662f\u6570\u5b57")
        token_minutes = data.get("ACCESS_TOKEN_EXPIRE_MINUTES", "")
        if token_minutes and not token_minutes.isdigit():
            raise ValueError("ACCESS_TOKEN_EXPIRE_MINUTES \u5fc5\u987b\u662f\u6570\u5b57")

        write_env_file(ENV_PATH, data, self.extra_env_values)
        self.config_values = data

    def build_ui(self) -> None:
        _ff_build_ui(self)

    def build_status_tab(self, parent: ttk.Frame) -> None:
        _ff_build_status_tab(self, parent)

    def get_status_badge_palette(self, key: str, value: str) -> Tuple[str, str]:
        return _ff_get_status_badge_palette(self, key, value)

    def refresh_status_badges(self) -> None:
        for key, widget in self.status_badges.items():
            if key not in self.status_vars:
                continue
            bg, fg = self.get_status_badge_palette(key, self.status_vars[key].get())
            try:
                widget.configure(bg=bg, fg=fg)
            except Exception:
                pass

    def build_manager_tab(self, parent: ttk.Frame) -> None:
        _ff_build_manager_tab(self, parent)

    def create_side_nav(
        self,
        parent: tk.Widget,
        items: List[Tuple[str, str, str]],
        selected_var: tk.StringVar,
        callback,
        registry: Dict[str, Dict[str, Any]],
    ) -> None:
        for key, title, description in items:
            card = tk.Frame(parent, bd=1, relief="flat", bg="#f0f0f0", padx=8, pady=6, cursor="hand2")
            card.pack(fill="x", pady=2)

            title_label = tk.Label(
                card,
                text=title,
                bg="#f0f0f0",
                fg="#333333",
                font=("Microsoft YaHei UI", 9, "bold"),
                anchor="w",
                justify="left",
                cursor="hand2",
            )
            title_label.pack(fill="x", anchor="w")

            desc_label = tk.Label(
                card,
                text=description,
                bg="#f0f0f0",
                fg="#666666",
                font=("Microsoft YaHei UI", 8),
                anchor="w",
                justify="left",
                wraplength=180,
                cursor="hand2",
            )
            desc_label.pack(fill="x", anchor="w", pady=(2, 0))

            def on_click(_event=None, target_key=key) -> None:
                selected_var.set(target_key)
                callback()

            card.bind("<Button-1>", on_click)
            title_label.bind("<Button-1>", on_click)
            desc_label.bind("<Button-1>", on_click)
            registry[key] = {"card": card, "title": title_label, "desc": desc_label}

    def refresh_side_nav_styles(self, registry: Dict[str, Dict[str, Any]], active_key: str) -> None:
        for key, widgets in registry.items():
            selected = key == active_key
            card_bg = "#e0e0e0" if selected else "#f0f0f0"
            title_fg = "#000000" if selected else "#333333"
            desc_fg = "#444444" if selected else "#666666"
            for widget_key, widget in widgets.items():
                if widget_key == "card":
                    widget.configure(bg=card_bg, highlightbackground="#cccccc")
                elif widget_key == "title":
                    widget.configure(bg=card_bg, fg=title_fg)
                elif widget_key == "desc":
                    widget.configure(bg=card_bg, fg=desc_fg)

    def build_config_tab(self, parent: ttk.Frame) -> None:
        _ff_build_config_tab(self, parent)

    def build_ops_tab(self, parent: ttk.Frame) -> None:
        _ff_build_ops_tab(self, parent)

    def build_env_tab(self, parent: ttk.Frame) -> None:
        _ff_build_env_tab(self, parent)

    def build_logs_tab(self, parent: ttk.Frame) -> None:
        _ff_build_logs_tab(self, parent)

    def show_env_section(self) -> None:
        active_key = getattr(self, "env_section_var", tk.StringVar(value="overview")).get()
        for section_key, frame in self.env_section_frames.items():
            if section_key == active_key:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
        self.refresh_side_nav_styles(self.env_nav_items, active_key)

    def show_config_section(self) -> None:
        active_key = getattr(self, "config_section_var", tk.StringVar(value="应用配置")).get()
        for section_key, frame in self.config_section_frames.items():
            if section_key == active_key:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
        self.refresh_side_nav_styles(self.config_nav_items, active_key)

    def show_ops_section(self) -> None:
        active_key = getattr(self, "ops_section_var", tk.StringVar(value="one_click_deploy")).get()
        for section_key, frame in self.ops_section_frames.items():
            if section_key == active_key:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
        self.refresh_side_nav_styles(self.ops_nav_items, active_key)

    def set_readonly_text(self, widget: scrolledtext.ScrolledText, content: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert("1.0", content)
        widget.configure(state="disabled")
        widget.see("1.0")

    def refresh_environment_info(self) -> None:
        _ff_refresh_environment_info(self)

    def create_tray_icon(self) -> None:
        _ff_create_tray_icon(self)

    def notify_tray(self, title: str, message: str) -> None:
        if not self.tray_icon:
            return
        try:
            self.tray_icon.notify(message, title)
        except Exception:
            pass

    def update_tray_title(self) -> None:
        if not self.tray_icon:
            return
        backend_status = self.status_vars.get("backend_status")
        frontend_status = self.status_vars.get("frontend_status")
        backend_text = backend_status.get() if backend_status else "未运行"
        frontend_text = frontend_status.get() if frontend_status else "未运行"
        self.tray_icon.title = f"FinFlow 管理器 | 后端：{backend_text} | 前端：{frontend_text}"

    def get_startup_status_text(self) -> str:
        enabled = bool(self.manager_state.get("launch_manager_on_startup", False))
        exists = STARTUP_SCRIPT_PATH.exists()
        if enabled and exists:
            return "\u5df2\u542f\u7528"
        if enabled and not exists:
            return "\u5f85\u5199\u5165"
        if exists:
            return "\u5df2\u5b58\u5728\u811a\u672c"
        return "\u672a\u542f\u7528"

    def sync_startup_entry(self, show_message: bool = False) -> None:
        enabled = bool(self.manager_state.get("launch_manager_on_startup", False))
        try:
            if enabled:
                STARTUP_DIR.mkdir(parents=True, exist_ok=True)
                STARTUP_SCRIPT_PATH.write_text(render_startup_script(), encoding="utf-8")
            elif STARTUP_SCRIPT_PATH.exists():
                STARTUP_SCRIPT_PATH.unlink()
        except Exception as exc:
            self.manager_state["launch_manager_on_startup"] = False
            if "launch_manager_on_startup" in self.manager_option_vars:
                self.manager_option_vars["launch_manager_on_startup"].set(False)
            write_state_file(STATE_PATH, self.manager_state)
            if show_message:
                messagebox.showerror("\u5f00\u673a\u81ea\u542f\u8bbe\u7f6e\u5931\u8d25", str(exc))
            return
        if show_message:
            messagebox.showinfo("\u8bbe\u7f6e\u6210\u529f", "\u5df2\u66f4\u65b0\u7ba1\u7406\u5668\u5f00\u673a\u81ea\u542f\u8bbe\u7f6e")

    def schedule_status_refresh(self, delay_ms: int = STATUS_REFRESH_INTERVAL_MS) -> None:
        if self.exiting:
            return
        if self.status_job:
            try:
                self.root.after_cancel(self.status_job)
            except Exception:
                pass
        self.status_job = self.root.after(delay_ms, self.refresh_status)

    def schedule_log_refresh(self, delay_ms: int = LOG_REFRESH_INTERVAL_MS) -> None:
        if self.exiting:
            return
        if self.log_job:
            try:
                self.root.after_cancel(self.log_job)
            except Exception:
                pass
        self.log_job = self.root.after(delay_ms, self.refresh_log_view)

    def schedule_health_refresh(self, delay_ms: int = HEALTH_CHECK_INTERVAL_MS) -> None:
        if self.exiting:
            return
        if self.health_job:
            try:
                self.root.after_cancel(self.health_job)
            except Exception:
                pass
        self.health_job = self.root.after(delay_ms, self.refresh_health_status)

    def schedule_db_refresh(self, delay_ms: int = DB_MONITOR_INTERVAL_MS) -> None:
        if self.exiting:
            return
        if self.db_job:
            try:
                self.root.after_cancel(self.db_job)
            except Exception:
                pass
        self.db_job = self.root.after(delay_ms, self.refresh_database_status)

    def schedule_ui_queue_refresh(self, delay_ms: int = 150) -> None:
        if self.exiting:
            return
        if self.ui_queue_job:
            try:
                self.root.after_cancel(self.ui_queue_job)
            except Exception:
                pass
        self.ui_queue_job = self.root.after(delay_ms, self.process_ui_queues)

    def process_ui_queues(self) -> None:
        self.ui_queue_job = None

        while True:
            try:
                callback, done_event, state = self.ui_callback_queue.get_nowait()
            except queue.Empty:
                break
            try:
                result = callback()
                if state is not None:
                    state["result"] = result
            except Exception as exc:
                if state is not None:
                    state["error"] = exc
            finally:
                if done_event is not None:
                    done_event.set()

        log_lines: List[str] = []
        while True:
            try:
                log_lines.append(self.deploy_log_queue.get_nowait())
            except queue.Empty:
                break
        if log_lines:
            self.deploy_log_text.configure(state="normal")
            self.deploy_log_text.insert("end", "".join(log_lines))
            self.deploy_log_text.see("end")
            self.deploy_log_text.configure(state="disabled")

        self.schedule_ui_queue_refresh(150 if self.deploy_running else 300)

    def call_on_ui_thread(self, callback: Any, wait: bool = False) -> Any:
        if threading.current_thread() is threading.main_thread():
            return callback()
        if not wait:
            self.ui_callback_queue.put((callback, None, None))
            return None
        done_event = threading.Event()
        state: Dict[str, Any] = {}
        self.ui_callback_queue.put((callback, done_event, state))
        done_event.wait()
        if "error" in state:
            raise state["error"]
        return state.get("result")

    def save_manager_state(self) -> None:
        for key, var in self.manager_option_vars.items():
            self.manager_state[key] = bool(var.get())
        for key, var in self.ops_vars.items():
            self.manager_state[key] = var.get().strip()
        write_state_file(STATE_PATH, self.manager_state)
        self.sync_startup_entry(show_message=False)
        if "db_monitor_status" in self.status_vars:
            self.status_vars["db_monitor_status"].set("监控中" if self.manager_state.get("enable_db_monitor", True) else "已关闭")
        self.refresh_status()
        self.schedule_db_refresh(200)

    def select_path_for_var(self, key: str, mode: str) -> None:
        initial_value = self.ops_vars[key].get().strip()
        initial_dir = str(Path(initial_value).parent) if initial_value else str(ROOT_DIR)

        if mode == "directory":
            selected = filedialog.askdirectory(initialdir=initial_value or str(ROOT_DIR), title="选择目录")
        else:
            filetypes = [("ZIP 文件", "*.zip")] if mode == "zip" else [("所有文件", "*.*")]
            selected = filedialog.askopenfilename(initialdir=initial_dir, title="选择文件", filetypes=filetypes)

        if selected:
            self.ops_vars[key].set(selected)
            self.save_manager_state()

    def maybe_start_services_on_launch(self) -> None:
        start_backend = bool(self.manager_state.get("start_backend_on_launch"))
        start_frontend = bool(self.manager_state.get("start_frontend_on_launch"))
        if start_backend and start_frontend:
            self.handle_start_all(show_dialog=False)
            return
        if start_backend:
            self.handle_start_backend()
        if start_frontend:
            self.handle_start_frontend(show_dialog=False)

    def handle_save_config(self) -> None:
        try:
            self.save_config_values()
            self.save_manager_state()
        except Exception as exc:
            messagebox.showerror("保存失败", str(exc))
            return
        messagebox.showinfo("保存成功", "配置已写入 backend/.env")
        self.refresh_status()

    def reload_form_from_disk(self) -> None:
        self.config_values, self.extra_env_values = self.load_config_values()
        for key, var in self.form_vars.items():
            var.set(self.config_values.get(key, ""))
        self.refresh_status()

    def generate_secret_key(self) -> None:
        value = secrets.token_urlsafe(48)
        self.form_vars["SECRET_KEY"].set(value)

    def generate_encryption_key(self) -> None:
        try:
            from cryptography.fernet import Fernet
        except Exception as exc:
            messagebox.showerror("生成失败", f"缺少 cryptography 依赖：{exc}")
            return
        KEY_PATH.write_text(Fernet.generate_key().decode(), encoding="utf-8")
        messagebox.showinfo("生成成功", f"已生成加密密钥：{KEY_PATH}")
        self.refresh_status()

    def get_effective_config(self) -> Dict[str, str]:
        data = dict(self.config_values)
        for key, var in self.form_vars.items():
            data[key] = var.get().strip()
        data["APP_RELOAD"] = "false"
        return data

    def get_backend_url(self) -> str:
        config = self.get_effective_config()
        host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
        port = config.get("APP_PORT", "8100") or "8100"
        return f"http://{host}:{port}/"

    def get_frontend_url(self) -> str:
        return resolve_frontend_service_settings(self.get_effective_config())["frontend_url"]

    def get_app_url(self) -> str:
        if self.frontend.is_running():
            return self.get_frontend_url()
        return self.get_backend_url()

    def handle_start_backend(self) -> None:
        """启动后端服务 - 高可用性设计"""
        self.log_status_var.set("正在启动后端服务...")
        self.root.update_idletasks()
        
        try:
            self.save_config_values()
        except Exception as exc:
            error_msg = f"保存配置失败：{exc}"
            self.log_status_var.set(error_msg)
            messagebox.showerror("无法启动", error_msg)
            return
        
        self.backend.clear_restart_failure_state()
        
        if self.backend.is_running():
            info_msg = "后端服务已在运行中"
            self.log_status_var.set(info_msg)
            messagebox.showinfo("服务状态", info_msg)
            self.refresh_status()
            return
        
        ok, message = self.backend.start(self.get_effective_config())
        
        if ok:
            success_msg = f"后端服务启动成功\n{message}"
            self.log_status_var.set(f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）")
            self.notify_tray("FinFlow 后端已启动", message)
            self.refresh_status()
            messagebox.showinfo("启动成功", success_msg)
            self.log_choice.set(FF_LOG_CHOICES[1])
            self.refresh_log_view(force=True)
        else:
            error_msg = f"后端服务启动失败\n{message}"
            self.log_status_var.set(error_msg)
            self.refresh_status()
            messagebox.showerror("启动失败", error_msg)

    def handle_stop_backend(self) -> None:
        """停止后端服务 - 高可用性设计"""
        if not self.backend.is_running():
            info_msg = "后端服务未运行"
            self.log_status_var.set(info_msg)
            messagebox.showinfo("服务状态", info_msg)
            return
        
        self.log_status_var.set("正在停止后端服务...")
        self.root.update_idletasks()
        
        ok, message = self.backend.stop()
        self.backend.clear_restart_failure_state()
        self.last_health_state = "unknown"
        
        if ok:
            success_msg = f"后端服务已停止\n{message}"
            self.log_status_var.set(success_msg)
            self.notify_tray("FinFlow 后端已停止", "后端服务已正常停止")
        else:
            error_msg = f"后端服务停止失败\n{message}"
            self.log_status_var.set(error_msg)
        
        self.refresh_status()
        self.log_choice.set(FF_LOG_CHOICES[1])
        self.refresh_log_view(force=True)
        messagebox.showinfo("停止结果", ok and f"后端服务已停止\n{message}" or f"停止失败\n{message}")

    def handle_restart_backend(self) -> None:
        """重启后端服务 - 高可用性设计"""
        self.log_status_var.set("正在重启后端服务...")
        self.root.update_idletasks()
        
        try:
            self.save_config_values()
        except Exception as exc:
            error_msg = f"保存配置失败：{exc}"
            self.log_status_var.set(error_msg)
            messagebox.showerror("无法重启", error_msg)
            return
        
        self.backend.clear_restart_failure_state()
        
        ok, message = self.backend.restart(self.get_effective_config())
        self.last_health_state = "unknown"
        
        if ok:
            success_msg = f"后端服务重启成功\n{message}"
            self.log_status_var.set(f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）")
            self.notify_tray("FinFlow 后端已重启", message)
            self.refresh_status()
            self.log_choice.set(FF_LOG_CHOICES[1])
            self.refresh_log_view(force=True)
            messagebox.showinfo("重启成功", success_msg)
        else:
            error_msg = f"后端服务重启失败\n{message}"
            self.log_status_var.set(error_msg)
            self.refresh_status()
            messagebox.showerror("重启失败", error_msg)

    def handle_start_frontend(self, show_dialog: bool = True) -> Tuple[bool, str]:
        """启动前端服务 - 高可用性设计"""
        self.log_status_var.set("正在启动前端服务...")
        self.root.update_idletasks()
        
        try:
            self.save_config_values()
        except Exception as exc:
            error_msg = f"保存配置失败：{exc}"
            self.log_status_var.set(error_msg)
            if show_dialog:
                messagebox.showerror("无法启动", error_msg)
            return False, error_msg
        
        self.frontend.clear_restart_failure_state()
        
        if self.frontend.is_running():
            info_msg = "前端服务已在运行中"
            self.log_status_var.set(info_msg)
            if show_dialog:
                messagebox.showinfo("服务状态", info_msg)
            self.refresh_status()
            return True, info_msg
        
        ok, message = self.frontend.start(self.get_effective_config())
        
        if ok:
            success_msg = f"前端服务启动成功\n{message}"
            self.log_status_var.set(f"当前显示：本次启动日志（前端，自 {self.frontend.last_session_started_label} 起）")
            self.notify_tray("前端已启动", message)
            self.refresh_status()
            self.log_choice.set(FF_LOG_CHOICES[3])
            self.refresh_log_view(force=True)
            if show_dialog:
                messagebox.showinfo("启动成功", success_msg)
        else:
            error_msg = f"前端服务启动失败\n{message}"
            self.log_status_var.set(error_msg)
            self.refresh_status()
            if show_dialog:
                messagebox.showerror("启动失败", error_msg)
        
        return ok, message

    def handle_stop_frontend(self, show_dialog: bool = True) -> Tuple[bool, str]:
        """停止前端服务 - 高可用性设计"""
        if not self.frontend.is_running():
            info_msg = "前端服务未运行"
            self.log_status_var.set(info_msg)
            if show_dialog:
                messagebox.showinfo("服务状态", info_msg)
            return True, info_msg
        
        self.log_status_var.set("正在停止前端服务...")
        self.root.update_idletasks()
        
        ok, message = self.frontend.stop()
        self.frontend.clear_restart_failure_state()
        self.last_frontend_health_state = "unknown"
        
        if ok:
            success_msg = f"前端服务已停止\n{message}"
            self.log_status_var.set(success_msg)
            self.notify_tray("前端已停止", "前端服务已正常停止")
        else:
            error_msg = f"前端服务停止失败\n{message}"
            self.log_status_var.set(error_msg)
        
        self.refresh_status()
        self.log_choice.set(FF_LOG_CHOICES[3])
        self.refresh_log_view(force=True)
        result_msg = ok and f"前端服务已停止\n{message}" or f"停止失败\n{message}"
        if show_dialog:
            messagebox.showinfo("停止结果", result_msg)
        return ok, message

    def handle_restart_frontend(self, show_dialog: bool = True) -> Tuple[bool, str]:
        """重启前端服务 - 高可用性设计"""
        self.log_status_var.set("正在重启前端服务...")
        self.root.update_idletasks()
        
        try:
            self.save_config_values()
        except Exception as exc:
            error_msg = f"保存配置失败：{exc}"
            self.log_status_var.set(error_msg)
            if show_dialog:
                messagebox.showerror("无法重启", error_msg)
            return False, error_msg
        
        self.frontend.clear_restart_failure_state()
        
        ok, message = self.frontend.restart(self.get_effective_config())
        self.last_frontend_health_state = "unknown"
        
        if ok:
            success_msg = f"前端服务重启成功\n{message}"
            self.log_status_var.set(f"当前显示：本次启动日志（前端，自 {self.frontend.last_session_started_label} 起）")
            self.notify_tray("前端已重启", message)
            self.refresh_status()
            self.log_choice.set(FF_LOG_CHOICES[3])
            self.refresh_log_view(force=True)
            if show_dialog:
                messagebox.showinfo("重启成功", success_msg)
        else:
            error_msg = f"前端服务重启失败\n{message}"
            self.log_status_var.set(error_msg)
            self.refresh_status()
            if show_dialog:
                messagebox.showerror("重启失败", error_msg)
        
        return ok, message

    def handle_start_all(self, show_dialog: bool = True) -> Tuple[bool, str]:
        """启动所有服务 - 高可用性设计"""
        self.log_status_var.set("正在启动所有服务...")
        self.root.update_idletasks()
        
        backend_started = False
        backend_message = ""
        
        try:
            self.save_config_values()
            self.backend.clear_restart_failure_state()
            
            if self.backend.is_running():
                backend_started = True
                backend_message = "后端服务已在运行中"
            else:
                backend_started, backend_message = self.backend.start(self.get_effective_config())
        except Exception as exc:
            backend_message = f"启动后端异常：{exc}"
            backend_started = False
        
        frontend_ok = False
        frontend_message = "后端启动失败，未继续启动前端"
        
        if backend_started:
            frontend_ok, frontend_message = self.handle_start_frontend(show_dialog=False)
        
        self.refresh_status()
        
        message = f"后端：{backend_message}\n前端：{frontend_message}"
        self.log_status_var.set(message)
        
        all_started = backend_started and frontend_ok
        if all_started:
            self.notify_tray("所有服务已启动", "后端和前端服务均已正常启动")
            if show_dialog:
                messagebox.showinfo("启动成功", f"所有服务启动成功\n\n{message}")
        else:
            if show_dialog:
                messagebox.showwarning("启动结果", f"部分服务启动失败\n\n{message}")
        
        return all_started, message

    def handle_stop_all(self, show_dialog: bool = True) -> Tuple[bool, str]:
        """停止所有服务 - 高可用性设计"""
        self.log_status_var.set("正在停止所有服务...")
        self.root.update_idletasks()
        
        frontend_ok, frontend_message = self.handle_stop_frontend(show_dialog=False)
        
        backend_ok = False
        backend_message = ""
        try:
            if self.backend.is_running():
                backend_ok, backend_message = self.backend.stop()
            else:
                backend_ok = True
                backend_message = "后端服务未运行"
        except Exception as exc:
            backend_message = f"停止后端异常：{exc}"
            backend_ok = False
        
        self.backend.clear_restart_failure_state()
        self.refresh_status()
        
        message = f"前端：{frontend_message}\n后端：{backend_message}"
        self.log_status_var.set(message)
        
        all_stopped = frontend_ok and backend_ok
        if all_stopped:
            self.notify_tray("所有服务已停止", "后端和前端服务均已正常停止")
            if show_dialog:
                messagebox.showinfo("停止成功", f"所有服务已停止\n\n{message}")
        else:
            if show_dialog:
                messagebox.showwarning("停止结果", f"部分服务停止失败\n\n{message}")
        
        return all_stopped, message

    def handle_restart_all(self, show_dialog: bool = True) -> Tuple[bool, str]:
        """重启所有服务 - 高可用性设计"""
        self.log_status_var.set("正在重启所有服务...")
        self.root.update_idletasks()
        
        self.handle_stop_all(show_dialog=False)
        time.sleep(1)
        
        ok, message = self.handle_start_all(show_dialog=False)
        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        
        if ok:
            self.notify_tray("所有服务已重启", "后端和前端服务均已重启成功")
            if show_dialog:
                messagebox.showinfo("重启成功", f"所有服务重启成功\n\n{message}")
        else:
            if show_dialog:
                messagebox.showwarning("重启结果", f"部分服务重启失败\n\n{message}")
        
        return ok, message

    def handle_takeover_backend(self) -> None:
        config = self.get_effective_config()
        port = (config.get("APP_PORT") or "8100").strip() or "8100"
        if not port.isdigit():
            messagebox.showerror("无法接管", "APP_PORT 不是有效数字")
            return

        owner = get_port_owner_info(int(port))
        if not owner:
            messagebox.showinfo("无需接管", f"端口 {port} 当前未被占用，可以直接使用“启动后端”")
            return

        owner_pid = int(owner.get("pid", "0") or "0")
        if self.backend.is_running() and self.backend.process and owner_pid == self.backend.process.pid:
            messagebox.showinfo("已由管理器接管", "当前后端实例已经是由管理器启动的")
            return

        confirm = messagebox.askyesno(
            "确认接管现有实例",
            (
                f"检测到端口 {port} 正被以下进程占用：\n"
                f"PID: {owner_pid}\n"
                "说明: 当前实例不是由 FinFlow 管理器启动。\n\n"
                "是否结束该进程，并改由 FinFlow 管理器重新启动后端？"
            ),
        )
        if not confirm:
            return

        released, release_message = force_release_port(int(port))
        if not released:
            messagebox.showerror("接管失败", f"无法释放端口 {port}。\n{release_message}")
            return

        archived_logs = []
        for log_path in (STDOUT_LOG, STDERR_LOG):
            archived = archive_log_file(log_path, "takeover")
            if archived:
                archived_logs.append(str(archived))

        self.backend.clear_restart_failure_state()
        ok, message = self.backend.start(config)
        self.refresh_status()
        if not ok:
            messagebox.showerror("接管失败", message)
            return
        self.notify_tray("接管成功", f"已由管理器接管端口 {port}")
        detail = f"旧实例已结束，管理器已重新启动后端。\n{message}"
        if archived_logs:
            detail += f"\n已归档旧日志到：\n" + "\n".join(archived_logs)
        self.log_status_var.set(f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）")
        messagebox.showinfo("接管成功", detail)

    def handle_force_release_port(self) -> None:
        config = self.get_effective_config()
        port = (config.get("APP_PORT") or "8100").strip() or "8100"
        if not port.isdigit():
            messagebox.showerror("无法释放", "APP_PORT 不是有效数字")
            return

        owner = get_port_owner_info(int(port))
        if not owner:
            messagebox.showinfo("无需释放", f"端口 {port} 当前未被占用")
            return

        owner_pid = owner.get("pid", "") or "未知"
        confirm = messagebox.askyesno(
            "确认强制释放端口",
            (
                f"检测到端口 {port} 当前被占用。\n"
                f"占用 PID: {owner_pid}\n\n"
                "是否强制结束占用进程并释放该端口？"
            ),
        )
        if not confirm:
            return

        released, release_message = force_release_port(int(port))
        self.refresh_status()
        if released:
            self.notify_tray("端口已释放", release_message)
            messagebox.showinfo("释放成功", release_message)
            return

        messagebox.showerror(
            "释放失败",
            f"{release_message}\n\n如果端口仍无法释放，建议重启服务器或临时更换 APP_PORT。",
        )

    def open_frontend(self) -> None:
        webbrowser.open(self.get_app_url())

    def open_logs_folder(self) -> None:
        ensure_log_dir()
        os.startfile(str(LOG_DIR))

    def open_log_archive_folder(self) -> None:
        LOG_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        os.startfile(str(LOG_ARCHIVE_DIR))

    def open_backend_folder(self) -> None:
        os.startfile(str(BACKEND_DIR))

    def open_dist_folder(self) -> None:
        DIST_DIR.mkdir(parents=True, exist_ok=True)
        os.startfile(str(DIST_DIR))

    def open_backup_folder(self) -> None:
        backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
        backup_dir.mkdir(parents=True, exist_ok=True)
        os.startfile(str(backup_dir))

    def open_project_root(self) -> None:
        os.startfile(str(ROOT_DIR))

    def deploy_frontend_dist(self) -> None:
        source_dir = Path(self.ops_vars["frontend_deploy_source"].get().strip())
        if not source_dir.exists() or not source_dir.is_dir():
            messagebox.showerror("部署失败", "请选择有效的前端 dist 来源目录")
            return
        if not (source_dir / "index.html").exists():
            messagebox.showerror("部署失败", "来源目录中未找到 index.html，请确认选择的是 dist 目录")
            return
        if source_dir.resolve() == DIST_DIR.resolve():
            messagebox.showinfo("无需部署", "来源目录就是当前 frontend/dist，无需重复覆盖")
            return

        self.save_manager_state()
        DIST_DIR.parent.mkdir(parents=True, exist_ok=True)

        try:
            if DIST_DIR.exists():
                shutil.rmtree(DIST_DIR)
            shutil.copytree(source_dir, DIST_DIR)
        except Exception as exc:
            messagebox.showerror("部署失败", f"复制前端 dist 失败：{exc}")
            return

        self.refresh_status()
        messagebox.showinfo("部署成功", f"前端 dist 已部署到：{DIST_DIR}")

    def apply_release_package(self) -> None:
        self.save_manager_state()
        package_path = Path(self.ops_vars["release_package_path"].get().strip())
        if not package_path.exists() or not package_path.is_file():
            messagebox.showerror("升级失败", "请选择有效的发布包 ZIP 文件")
            return
        if package_path.suffix.lower() != ".zip":
            messagebox.showerror("升级失败", "发布包必须是 .zip 文件")
            return

        proceed = messagebox.askyesno(
            "确认升级",
            "升级过程会自动停止前后端，覆盖发布包中的代码和前端构建文件，同时保留本机配置、密钥、虚拟环境与日志。是否继续？",
        )
        if not proceed:
            return

        extract_dir = Path(tempfile.mkdtemp(prefix="finflow_release_"))
        backup_root = UPGRADE_BACKUP_DIR / datetime.now().strftime("%Y%m%d_%H%M%S")
        backend_was_running = self.backend.is_running()
        frontend_was_running = self.frontend.is_running()
        restored_services: List[str] = []

        try:
            with zipfile.ZipFile(package_path, "r") as archive:
                archive.extractall(extract_dir)
        except zipfile.BadZipFile:
            shutil.rmtree(extract_dir, ignore_errors=True)
            messagebox.showerror("升级失败", "发布包不是有效的 ZIP 文件")
            return
        except Exception as exc:
            shutil.rmtree(extract_dir, ignore_errors=True)
            messagebox.showerror("升级失败", f"解压发布包失败：{exc}")
            return

        try:
            release_root = detect_release_root(extract_dir)
            has_backend = (release_root / "backend").exists()
            has_frontend_dist = (release_root / "frontend" / "dist" / "index.html").exists()
            has_tools = (release_root / "tools").exists()
            has_deploy = (release_root / "deploy").exists()
            has_root_files = any(item.is_file() for item in release_root.iterdir())

            if not any([has_backend, has_frontend_dist, has_tools, has_deploy, has_root_files]):
                raise RuntimeError("发布包内未检测到可升级内容，至少应包含 backend 或 frontend/dist")

            if frontend_was_running:
                self.frontend.stop()
            if backend_was_running:
                self.backend.stop()

            copied_counts: Dict[str, int] = {}
            if has_backend:
                copied_counts["backend"] = overlay_directory(release_root / "backend", ROOT_DIR / "backend", backup_root)
            if has_tools:
                copied_counts["tools"] = overlay_directory(release_root / "tools", ROOT_DIR / "tools", backup_root)
            if has_deploy:
                copied_counts["deploy"] = overlay_directory(release_root / "deploy", ROOT_DIR / "deploy", backup_root)
            if has_root_files:
                copied_counts["root"] = copy_release_root_files(release_root, backup_root)
            if has_frontend_dist:
                copied_counts["frontend_dist"] = replace_directory(
                    release_root / "frontend" / "dist",
                    ROOT_DIR / "frontend" / "dist",
                    backup_root,
                )

            restart_messages: List[str] = []
            if backend_was_running:
                ok, restart_message = self.backend.start(self.get_effective_config())
                restart_messages.append(f"后端：{restart_message}")
                if not ok:
                    raise RuntimeError(f"升级完成，但后端自动重启失败：{restart_message}")
            if frontend_was_running:
                ok, restart_message = self.frontend.start(self.get_effective_config())
                restart_messages.append(f"前端：{restart_message}")
                if not ok:
                    raise RuntimeError(f"升级完成，但前端自动重启失败：{restart_message}")

            self.last_backend_health_state = "unknown"
            self.last_frontend_health_state = "unknown"
            refresh_status_async()
            self.notify_tray("发布包升级完成", f"已应用：{package_path.name}")
            summary = "，".join(f"{key}={value}" for key, value in copied_counts.items() if value)
            detail = f"升级完成。\n备份目录：{backup_root}\n覆盖内容：{summary or '无文件变更'}"
            if restart_messages:
                detail += "\n服务恢复：\n" + "\n".join(restart_messages)
            messagebox.showinfo("升级完成", detail)
        except Exception as exc:
            restart_result = []
            if backend_was_running and not self.backend.is_running():
                ok, restart_message = self.backend.start(self.get_effective_config())
                restart_result.append(f"后端：{restart_message}")
                if ok:
                    restored_services.append("backend")
            else:
                restart_result.append("后端：未重启")
            if frontend_was_running and not self.frontend.is_running():
                ok, restart_message = self.frontend.start(self.get_effective_config())
                restart_result.append(f"前端：{restart_message}")
                if ok:
                    restored_services.append("frontend")
            else:
                restart_result.append("前端：未重启")

            backup_info = str(backup_root) if backup_root.exists() else "未生成备份目录"
            detail = f"{exc}\n备份目录：{backup_info}\n恢复结果：" + "\n".join(restart_result)
            if restored_services:
                self.notify_tray("发布包升级失败", f"已尝试恢复服务：{', '.join(restored_services)}")
            messagebox.showerror("升级失败", detail)
        finally:
            shutil.rmtree(extract_dir, ignore_errors=True)
            refresh_status_async()

    def backup_database(self) -> None:
        self.save_manager_state()
        backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
        sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
        retention_days = int(self.ops_vars["backup_retention_days"].get().strip() or 30)
        retention_count = int(self.ops_vars["backup_retention_count"].get().strip() or 10)
        backup_dir.mkdir(parents=True, exist_ok=True)

        config = self.get_effective_config()
        ok, detail = run_database_backup_with_cleanup(config, backup_dir, sqlcmd, retention_days, retention_count)
        if ok:
            messagebox.showinfo("备份成功", f"数据库已备份到：{detail}")
        else:
            messagebox.showerror("备份失败", detail)

    def check_git_available(self) -> bool:
        try:
            result = subprocess.run(
                ["git", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return result.returncode == 0
        except FileNotFoundError:
            return False
        except Exception:
            return False

    def check_git_update(self) -> None:
        _ff_check_git_update(self)

    def git_pull_update(self) -> None:
        _ff_git_pull_update(self)

    def run_git_post_update_tasks(self) -> Tuple[bool, str]:
        if not self.manager_state.get("git_auto_build_frontend", True):
            return True, "\u5df2\u8df3\u8fc7\u81ea\u52a8\u4f9d\u8d56\u540c\u6b65\u4e0e\u524d\u7aef\u6784\u5efa"

        steps: List[str] = []

        backend_dep_ok, backend_dep_msg = sync_backend_dependencies()
        if not backend_dep_ok:
            return False, f"\u540e\u7aef\u4f9d\u8d56\u540c\u6b65\u5931\u8d25\uff1a{backend_dep_msg}"
        steps.append(f"\u540e\u7aef\u4f9d\u8d56\uff1a{backend_dep_msg}")

        frontend_dep_ok, frontend_dep_msg = sync_frontend_dependencies()
        if not frontend_dep_ok:
            return False, f"\u524d\u7aef\u4f9d\u8d56\u540c\u6b65\u5931\u8d25\uff1a{frontend_dep_msg}"
        steps.append(f"\u524d\u7aef\u4f9d\u8d56\uff1a{frontend_dep_msg}")

        frontend_build_ok, frontend_build_msg = build_frontend()
        if not frontend_build_ok:
            return False, f"\u524d\u7aef\u6784\u5efa\u5931\u8d25\uff1a{frontend_build_msg}"
        steps.append(f"\u524d\u7aef\u6784\u5efa\uff1a{frontend_build_msg}")

        return True, "\n".join(steps)

    def git_show_history(self) -> None:
        _ff_git_show_history(self)

    def git_rollback(self) -> None:
        _ff_git_rollback(self)

    def execute_rollback(self, listbox: tk.Listbox, window: tk.Toplevel) -> None:
        _ff_execute_rollback(self, listbox, window)

    def select_migration_file(self) -> None:
        _ff_select_migration_file(self)

    def clear_migration_script(self) -> None:
        self.migration_script_text.configure(state="normal")
        self.migration_script_text.delete("1.0", tk.END)
        self.migration_script_text.configure(state="disabled")

    def execute_migration(self) -> None:
        _ff_execute_migration(self)

    def append_deploy_log(self, message: str) -> None:
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n"
        if threading.current_thread() is threading.main_thread():
            self.deploy_log_text.configure(state="normal")
            self.deploy_log_text.insert("end", line)
            self.deploy_log_text.see("end")
            self.deploy_log_text.configure(state="disabled")
            return
        self.deploy_log_queue.put(line)

    def clear_deploy_log(self) -> None:
        while True:
            try:
                self.deploy_log_queue.get_nowait()
            except queue.Empty:
                break
        self.deploy_log_text.configure(state="normal")
        self.deploy_log_text.delete("1.0", tk.END)
        self.deploy_log_text.configure(state="disabled")

    def start_one_click_deploy(self) -> None:
        if self.deploy_running:
            messagebox.showinfo("\u63d0\u793a", "\u4e00\u952e\u90e8\u7f72\u6b63\u5728\u8fd0\u884c\uff0c\u8bf7\u7a0d\u5019")
            return
        self.save_manager_state()
        self.clear_deploy_log()

        mode = self.deploy_mode.get()
        repo_url = self.ops_vars["git_repo_url"].get().strip()
        branch = self.ops_vars["git_branch"].get().strip() or "main"
        sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
        release_package_path = self.ops_vars["release_package_path"].get().strip()
        config = self.get_effective_config()
        if mode == "git" and not repo_url:
            messagebox.showerror("\u9519\u8bef", "Git \u90e8\u7f72\u6a21\u5f0f\u9700\u8981\u5148\u914d\u7f6e\u4ed3\u5e93\u5730\u5740")
            return
        if mode == "zip":
            pkg = Path(release_package_path)
            if not pkg.exists():
                messagebox.showerror("\u9519\u8bef", "ZIP \u90e8\u7f72\u6a21\u5f0f\u9700\u8981\u5148\u9009\u62e9\u53d1\u5e03\u5305\u6587\u4ef6")
                return

        proceed = messagebox.askyesno(
            "\u786e\u8ba4\u4e00\u952e\u90e8\u7f72",
            "\u4e00\u952e\u90e8\u7f72\u5c06\u81ea\u52a8\u6267\u884c\u4ee5\u4e0b\u6d41\u7a0b\uff1a\n\n"
            "1. \u73af\u5883\u68c0\u67e5\uff08Git/Python/Node/sqlcmd\uff09\n"
            "2. \u505c\u6b62\u524d\u540e\u7aef\u670d\u52a1\n"
            "3. \u4ee3\u7801\u90e8\u7f72\uff08Git \u62c9\u53d6 \u6216 ZIP \u5347\u7ea7\uff09\n"
            "4. \u4f9d\u8d56\u540c\u6b65\uff08\u540e\u7aef + \u524d\u7aef\uff09\n"
            "5. \u8fd0\u884c\u914d\u7f6e\u5199\u5165\u3001\u524d\u7aef\u6784\u5efa\u4e0e\u8fd0\u884c\u914d\u7f6e\u540c\u6b65\n"
            "6. \u90e8\u7f72\u540e\u5065\u5eb7\u4e0e\u540e\u7f6e\u68c0\u67e5\n"
            "\n\u8bf4\u660e\uff1a\u90e8\u7f72\u5b8c\u6210\u540e\u4ec5\u4fdd\u7559\u53ef\u8fd0\u884c\u4ea7\u7269\u4e0e\u914d\u7f6e\uff0c\u4e0d\u5728\u90e8\u7f72\u9636\u6bb5\u81ea\u52a8\u542f\u52a8\u4efb\u4f55\u670d\u52a1\u3002\n\n"
            "\u662f\u5426\u7ee7\u7eed\uff1f"
        )
        if not proceed:
            return

        self.root.config(cursor="watch")
        self.deploy_running = True
        self.deploy_thread = threading.Thread(
            target=self._run_one_click_deploy_worker,
            args=(mode, repo_url, branch, sqlcmd, config, release_package_path),
            daemon=True,
        )
        self.deploy_thread.start()


    def _run_one_click_deploy_worker(
        self,
        mode: str,
        repo_url: str,
        branch: str,
        sqlcmd: str,
        config: Dict[str, str],
        release_package_path: str,
    ) -> None:
        original_showerror = messagebox.showerror
        original_showinfo = messagebox.showinfo
        original_showwarning = messagebox.showwarning
        original_refresh_status = self.refresh_status
        original_refresh_database_status = self.refresh_database_status

        def _show_modal(dialog, *args, **kwargs):
            def _invoke():
                try:
                    self.root.deiconify()
                    self.root.lift()
                    self.root.focus_force()
                except Exception:
                    pass
                kwargs.setdefault("parent", self.root)
                return dialog(*args, **kwargs)

            return self.call_on_ui_thread(_invoke, wait=True)

        def _safe_showerror(*args, **kwargs):
            return _show_modal(original_showerror, *args, **kwargs)

        def _safe_showinfo(*args, **kwargs):
            return _show_modal(original_showinfo, *args, **kwargs)

        def _safe_showwarning(*args, **kwargs):
            return _show_modal(original_showwarning, *args, **kwargs)

        def _safe_refresh_status():
            return self.call_on_ui_thread(original_refresh_status)

        def _safe_refresh_database_status(*args, **kwargs):
            return self.call_on_ui_thread(lambda: original_refresh_database_status(*args, **kwargs), wait=True)

        messagebox.showerror = _safe_showerror
        messagebox.showinfo = _safe_showinfo
        messagebox.showwarning = _safe_showwarning
        self.refresh_status = _safe_refresh_status
        self.refresh_database_status = _safe_refresh_database_status

        try:
            self._run_one_click_deploy(mode, repo_url, branch, sqlcmd, config, release_package_path)
        except RuntimeError:
            pass
        except Exception as exc:
            self.append_deploy_log(f"\u90e8\u7f72\u5f02\u5e38\uff1a{exc}")
            self.append_deploy_log(traceback.format_exc())
            _safe_showerror("\u4e00\u952e\u90e8\u7f72\u5f02\u5e38", f"{exc}\n\n{traceback.format_exc()}")
        finally:
            messagebox.showerror = original_showerror
            messagebox.showinfo = original_showinfo
            messagebox.showwarning = original_showwarning
            self.refresh_status = original_refresh_status
            self.refresh_database_status = original_refresh_database_status
            self.call_on_ui_thread(self._finish_one_click_deploy)


    def _finish_one_click_deploy(self) -> None:
        self.deploy_running = False
        self.deploy_thread = None
        self.root.config(cursor="")


    def _run_one_click_deploy(
        self,
        mode: str,
        repo_url: str,
        branch: str,
        sqlcmd: str,
        config: Dict[str, str],
        release_package_path: str,
    ) -> None:
        steps_passed = 0
        total_steps = 6
        backend_was_running = self.backend.is_running()
        frontend_was_running = self.frontend.is_running()
        services_stopped = False

        self.append_deploy_log(f"{'=' * 50}")
        self.append_deploy_log(
            f"\u5f00\u59cb\u4e00\u952e\u90e8\u7f72 (\u6a21\u5f0f: {'Git \u62c9\u53d6' if mode == 'git' else 'ZIP \u53d1\u5e03\u5305'})"
        )
        self.append_deploy_log(f"{'=' * 50}")

        def refresh_status_async() -> None:
            self.call_on_ui_thread(self.refresh_status)

        def save_runtime_config(runtime_config: Dict[str, str]) -> None:
            port = (runtime_config.get("APP_PORT") or "").strip()
            if not port.isdigit():
                raise ValueError("APP_PORT must be numeric")
            token_minutes = (runtime_config.get("ACCESS_TOKEN_EXPIRE_MINUTES") or "").strip()
            if token_minutes and not token_minutes.isdigit():
                raise ValueError("ACCESS_TOKEN_EXPIRE_MINUTES must be numeric")
            runtime_config["APP_RELOAD"] = "false"
            write_env_file(ENV_PATH, runtime_config, self.extra_env_values)
            self.config_values = dict(runtime_config)

        def abort_deploy(reason: str) -> None:
            restore_messages: List[str] = []

            if services_stopped:
                if backend_was_running and not self.backend.is_running():
                    ok, msg = self.backend.start(config)
                    restore_messages.append(f"\u6062\u590d\u540e\u7aef\uff1a{msg}")
                if frontend_was_running and not self.frontend.is_running():
                    ok, msg = self.frontend.start(config)
                    restore_messages.append(f"\u6062\u590d\u524d\u7aef\uff1a{msg}")

            self.last_backend_health_state = "unknown"
            self.last_frontend_health_state = "unknown"
            refresh_status_async()

            detail = reason
            if restore_messages:
                detail += "\n\n\u6062\u590d\u7ed3\u679c\uff1a\n" + "\n".join(restore_messages)
            self.append_deploy_log(f"[FAIL] {reason}")
            messagebox.showerror("\u4e00\u952e\u90e8\u7f72\u5931\u8d25", detail)
            raise RuntimeError(reason)

        self.append_deploy_log(f"[1/{total_steps}] \u73af\u5883\u68c0\u67e5...")

        python_exe = get_backend_python_executable()
        system_python = "python"
        try:
            result = subprocess.run(
                [system_python, "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if result.returncode == 0:
                self.append_deploy_log(f"  [OK] \u7cfb\u7edf Python \u5df2\u5b89\u88c5: {result.stdout.strip()}")
        except Exception:
            self.append_deploy_log("  [FAIL] \u7cfb\u7edf\u672a\u5b89\u88c5 Python\uff0c\u8bf7\u5148\u5b89\u88c5 Python 3.9+")
            messagebox.showerror("\u73af\u5883\u7f3a\u5931", "\u8bf7\u5148\u5b89\u88c5 Python 3.9 \u6216\u66f4\u9ad8\u7248\u672c\n\u4e0b\u8f7d\u5730\u5740\uff1ahttps://www.python.org/downloads/")
            return

        if not python_exe.exists():
            self.append_deploy_log("  \u6b63\u5728\u521b\u5efa\u540e\u7aef\u865a\u62df\u73af\u5883...")
            try:
                result = subprocess.run(
                    [system_python, "-m", "venv", str(BACKEND_VENV_DIR)],
                    cwd=BACKEND_DIR,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if result.returncode == 0:
                    self.append_deploy_log("  [OK] \u540e\u7aef\u865a\u62df\u73af\u5883\u5df2\u521b\u5efa")
                    python_exe = get_backend_python_executable()
                else:
                    error_msg = result.stderr or result.stdout or "\u521b\u5efa\u5931\u8d25"
                    self.append_deploy_log(f"  [FAIL] \u521b\u5efa\u865a\u62df\u73af\u5883\u5931\u8d25: {error_msg}")
                    return
            except Exception as exc:
                self.append_deploy_log(f"  [FAIL] \u521b\u5efa\u865a\u62df\u73af\u5883\u5f02\u5e38: {exc}")
                return
        else:
            self.append_deploy_log("  [OK] \u540e\u7aef\u865a\u62df\u73af\u5883\u5df2\u5b58\u5728")

        self.append_deploy_log("  \u6b63\u5728\u5347\u7ea7\u865a\u62df\u73af\u5883 pip...")
        try:
            upgrade_result = subprocess.run(
                [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
                cwd=BACKEND_DIR,
                capture_output=True,
                text=True,
                timeout=120,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if upgrade_result.returncode == 0:
                self.append_deploy_log("  [OK] pip \u5347\u7ea7\u6210\u529f")
            else:
                self.append_deploy_log(
                    f"  [WARN] pip \u5347\u7ea7\u5931\u8d25: {upgrade_result.stderr or upgrade_result.stdout}"
                )
        except Exception as exc:
            self.append_deploy_log(f"  [WARN] pip \u5347\u7ea7\u5f02\u5e38: {exc}")

        if not self.check_git_available():
            self.append_deploy_log("  [FAIL] Git \u672a\u5b89\u88c5")
            messagebox.showerror("\u73af\u5883\u7f3a\u5931", "\u8bf7\u5148\u5b89\u88c5 Git \u5e76\u6dfb\u52a0\u5230\u7cfb\u7edf PATH\n\u4e0b\u8f7d\u5730\u5740\uff1ahttps://git-scm.com/download/win")
            return
        self.append_deploy_log("  [OK] Git \u53ef\u7528")

        git_dir = ROOT_DIR / ".git"
        if not git_dir.exists():
            if mode == "git" and repo_url:
                self.append_deploy_log("  \u6b63\u5728\u521d\u59cb\u5316 Git \u4ed3\u5e93...")
                try:
                    result = subprocess.run(
                        ["git", "init"],
                        cwd=ROOT_DIR,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    if result.returncode == 0:
                        self.append_deploy_log("  [OK] Git \u4ed3\u5e93\u5df2\u521d\u59cb\u5316")
                        result = subprocess.run(
                            ["git", "remote", "add", "origin", repo_url],
                            cwd=ROOT_DIR,
                            capture_output=True,
                            text=True,
                            timeout=30,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                        )
                        if result.returncode == 0:
                            self.append_deploy_log("  [OK] \u8fdc\u7a0b\u4ed3\u5e93\u5df2\u914d\u7f6e")
                        else:
                            self.append_deploy_log(f"  [WARN] \u6dfb\u52a0\u8fdc\u7a0b\u4ed3\u5e93\u5931\u8d25: {result.stderr}")
                    else:
                        self.append_deploy_log(f"  [FAIL] Git \u521d\u59cb\u5316\u5931\u8d25: {result.stderr}")
                        return
                except Exception as exc:
                    self.append_deploy_log(f"  [FAIL] Git \u521d\u59cb\u5316\u5f02\u5e38: {exc}")
                    return
            else:
                self.append_deploy_log("  [WARN] Git \u4ed3\u5e93\u672a\u521d\u59cb\u5316\uff0c\u4e14\u672a\u914d\u7f6e\u8fdc\u7a0b\u4ed3\u5e93\u5730\u5740")
        else:
            self.append_deploy_log("  [OK] Git \u4ed3\u5e93\u5df2\u5b58\u5728")

        try:
            subprocess.run(
                [sqlcmd, "-?"],
                capture_output=True,
                timeout=5,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            self.append_deploy_log("  [OK] sqlcmd \u53ef\u7528")
        except Exception:
            self.append_deploy_log("  [WARN] sqlcmd \u4e0d\u53ef\u7528\uff0c\u6570\u636e\u5e93\u76f8\u5173\u64cd\u4f5c\u5c06\u8df3\u8fc7")
            sqlcmd = ""

        try:
            node_result = subprocess.run(
                ["node", "--version"],
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            if node_result.returncode == 0:
                self.append_deploy_log(f"  [OK] Node.js \u5df2\u5b89\u88c5: {node_result.stdout.strip()}")
            else:
                self.append_deploy_log("  [FAIL] Node.js \u4e0d\u53ef\u7528")
                messagebox.showerror("\u73af\u5883\u7f3a\u5931", "\u8bf7\u5148\u5b89\u88c5 Node.js 18+ \u540e\u518d\u6267\u884c\u4e00\u952e\u90e8\u7f72")
                return
        except Exception:
            self.append_deploy_log("  [FAIL] Node.js \u672a\u5b89\u88c5")
            messagebox.showerror("\u73af\u5883\u7f3a\u5931", "\u8bf7\u5148\u5b89\u88c5 Node.js 18+ \u540e\u518d\u6267\u884c\u4e00\u952e\u90e8\u7f72")
            return

        steps_passed += 1

        self.append_deploy_log(f"[2/{total_steps}] \u505c\u6b62\u524d\u540e\u7aef\u670d\u52a1...")
        if frontend_was_running:
            ok, message = self.frontend.stop()
            self.append_deploy_log(f"  [{'OK' if ok else 'WARN'}] \u524d\u7aef: {message}")
        else:
            self.append_deploy_log("  [SKIP] \u524d\u7aef\u672a\u8fd0\u884c")
        if backend_was_running:
            ok, message = self.backend.stop()
            self.append_deploy_log(f"  [{'OK' if ok else 'WARN'}] \u540e\u7aef: {message}")
        else:
            self.append_deploy_log("  [SKIP] \u540e\u7aef\u672a\u8fd0\u884c")
        services_stopped = True
        steps_passed += 1

        self.append_deploy_log(f"[3/{total_steps}] \u4ee3\u7801\u90e8\u7f72...")

        if mode == "git":
            if not (ROOT_DIR / ".git").exists():
                self.append_deploy_log("  \u521d\u59cb\u5316 Git \u4ed3\u5e93...")
                temp_clone_dir = Path(tempfile.mkdtemp(prefix="finflow_git_clone_"))
                try:
                    result = subprocess.run(
                        ["git", "clone", "--branch", branch, "--depth", "1", repo_url, str(temp_clone_dir)],
                        capture_output=True,
                        text=True,
                        timeout=300,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    if result.returncode != 0:
                        abort_deploy(f"Git \u514b\u9686\u5931\u8d25: {result.stderr.strip()}")

                    backup_root = UPGRADE_BACKUP_DIR / datetime.now().strftime("%Y%m%d_%H%M%S_oneclick")
                    for item_name in ("backend", "frontend", "tools", "deploy"):
                        src = temp_clone_dir / item_name
                        if src.exists():
                            overlay_directory(src, ROOT_DIR / item_name, backup_root)

                    self.append_deploy_log("  [OK] \u4ee3\u7801\u5df2\u514b\u9686\u5e76\u8986\u76d6")
                finally:
                    shutil.rmtree(temp_clone_dir, ignore_errors=True)
            else:
                diff_result = subprocess.run(
                    ["git", "diff", "--quiet"],
                    cwd=ROOT_DIR,
                    capture_output=True,
                    timeout=10,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if diff_result.returncode != 0:
                    subprocess.run(
                        [
                            "git",
                            "stash",
                            "push",
                            "-m",
                            f"One-click deploy auto-stash {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        ],
                        cwd=ROOT_DIR,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    self.append_deploy_log("  \u5df2\u6682\u5b58\u672c\u5730\u53d8\u66f4")

                fetch_result = subprocess.run(
                    ["git", "fetch", "origin", branch],
                    cwd=ROOT_DIR,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if fetch_result.returncode != 0:
                    abort_deploy(f"Git fetch \u5931\u8d25: {fetch_result.stderr.strip()}")

                pull_result = subprocess.run(
                    ["git", "pull", "origin", branch],
                    cwd=ROOT_DIR,
                    capture_output=True,
                    text=True,
                    timeout=300,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if pull_result.returncode != 0:
                    abort_deploy(f"Git pull \u5931\u8d25: {pull_result.stderr.strip()}")

                if "Already up to date" in pull_result.stdout:
                    self.append_deploy_log("  [OK] \u4ee3\u7801\u5df2\u662f\u6700\u65b0")
                else:
                    self.append_deploy_log("  [OK] \u4ee3\u7801\u5df2\u66f4\u65b0")
        else:
            package_path = Path(release_package_path)
            extract_dir = Path(tempfile.mkdtemp(prefix="finflow_release_"))
            backup_root = UPGRADE_BACKUP_DIR / datetime.now().strftime("%Y%m%d_%H%M%S_oneclick")
            try:
                with zipfile.ZipFile(package_path, "r") as archive:
                    archive.extractall(extract_dir)

                release_root = detect_release_root(extract_dir)
                if (release_root / "backend").exists():
                    overlay_directory(release_root / "backend", ROOT_DIR / "backend", backup_root)
                if (release_root / "frontend" / "dist").exists():
                    replace_directory(release_root / "frontend" / "dist", ROOT_DIR / "frontend" / "dist", backup_root)
                if (release_root / "tools").exists():
                    overlay_directory(release_root / "tools", ROOT_DIR / "tools", backup_root)
                if (release_root / "deploy").exists():
                    overlay_directory(release_root / "deploy", ROOT_DIR / "deploy", backup_root)

                self.append_deploy_log("  [OK] ZIP \u53d1\u5e03\u5305\u5df2\u5e94\u7528")
            finally:
                shutil.rmtree(extract_dir, ignore_errors=True)

        steps_passed += 1

        self.append_deploy_log(f"[4/{total_steps}] \u4f9d\u8d56\u540c\u6b65...")
        self.append_deploy_log("  [RUN] \u5f00\u59cb\u540c\u6b65\u540e\u7aef Python \u4f9d\u8d56...")
        backend_dep_ok, backend_dep_msg = sync_backend_dependencies(log_callback=self.append_deploy_log)
        if backend_dep_ok:
            self.append_deploy_log(f"  [OK] \u540e\u7aef\u4f9d\u8d56: {backend_dep_msg}")
        else:
            abort_deploy(f"\u540e\u7aef\u4f9d\u8d56\u540c\u6b65\u5931\u8d25: {backend_dep_msg}")

        self.append_deploy_log("  [RUN] \u5f00\u59cb\u540c\u6b65\u524d\u7aef Node.js \u4f9d\u8d56...")
        frontend_dep_ok, frontend_dep_msg = sync_frontend_dependencies(log_callback=self.append_deploy_log)
        if frontend_dep_ok:
            self.append_deploy_log(f"  [OK] \u524d\u7aef\u4f9d\u8d56: {frontend_dep_msg}")
        else:
            abort_deploy(f"\u524d\u7aef\u4f9d\u8d56\u540c\u6b65\u5931\u8d25: {frontend_dep_msg}")
        steps_passed += 1

        self.append_deploy_log(f"[5/{total_steps}] \u8fd0\u884c\u914d\u7f6e\u5199\u5165\u3001\u524d\u7aef\u6784\u5efa\u4e0e\u8fd0\u884c\u914d\u7f6e\u540c\u6b65...")
        self.append_deploy_log("  [RUN] \u51c6\u5907\u5199\u5165 backend/.env ...")
        try:
            save_runtime_config(config)
            self.append_deploy_log("  [OK] \u540e\u7aef\u8fd0\u884c\u914d\u7f6e\u5df2\u5199\u5165 backend/.env")
        except Exception as exc:
            abort_deploy(f"\u8fd0\u884c\u914d\u7f6e\u5199\u5165\u5931\u8d25: {exc}")
        self.append_deploy_log("  [RUN] \u5f00\u59cb\u6267\u884c\u524d\u7aef\u6784\u5efa...")
        fe_ok, fe_msg = build_frontend(log_callback=self.append_deploy_log)
        if fe_ok:
            self.append_deploy_log(f"  [OK] {fe_msg}")
        else:
            self.append_deploy_log(f"  [WARN] {fe_msg}")
        self.append_deploy_log("  [RUN] \u5f00\u59cb\u540c\u6b65\u524d\u7aef\u8fd0\u884c\u914d\u7f6e...")
        key_ok, key_msg = sync_keys_to_frontend(config)
        if key_ok:
            self.append_deploy_log(f"  [OK] \u524d\u7aef\u8fd0\u884c\u914d\u7f6e: {key_msg}")
        else:
            abort_deploy(f"\u524d\u7aef\u8fd0\u884c\u914d\u7f6e\u540c\u6b65\u5931\u8d25: {key_msg}")
        steps_passed += 1

        self.append_deploy_log(f"[6/{total_steps}] \u90e8\u7f72\u540e\u68c0\u67e5...")
        host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
        port = config.get("APP_PORT", "8100")
        backend_url = f"http://{host}:{port}"
        frontend_url = resolve_frontend_service_settings(config)["frontend_url"]
        self.append_deploy_log("  [SKIP] \u90e8\u7f72\u9636\u6bb5\u4e0d\u81ea\u52a8\u542f\u52a8\u6216\u6062\u590d\u524d\u540e\u7aef\u670d\u52a1")
        self.append_deploy_log("  [INFO] \u8bf7\u5728\u300c\u670d\u52a1\u72b6\u6001\u300d\u9875\u9762\u5355\u72ec\u542f\u52a8\u540e\u7aef\u3001\u524d\u7aef\uff0c\u518d\u6267\u884c\u5065\u5eb7\u9a8c\u8bc1")

        backend_health_ok = False
        backend_health_detail = "\u672a\u6267\u884c"
        frontend_health_ok = False
        frontend_health_detail = "\u672a\u6267\u884c"
        if self.backend.is_running():
            self.append_deploy_log("  [RUN] \u68c0\u67e5\u540e\u7aef\u5065\u5eb7\u72b6\u6001...")
            time.sleep(3)
            backend_health_ok, backend_health_detail = probe_backend_health(backend_url)
            if backend_health_ok:
                self.append_deploy_log(f"  [OK] \u540e\u7aef\u5065\u5eb7\u68c0\u67e5: {backend_health_detail}")
            else:
                self.append_deploy_log(f"  [WARN] \u540e\u7aef\u5065\u5eb7\u68c0\u67e5: {backend_health_detail}")
        else:
            backend_health_detail = "\u672a\u542f\u52a8\u540e\u7aef\uff0c\u5df2\u8df3\u8fc7\u5065\u5eb7\u68c0\u67e5"
            self.append_deploy_log(f"  [SKIP] {backend_health_detail}")

        if self.frontend.is_running():
            self.append_deploy_log("  [RUN] \u68c0\u67e5\u524d\u7aef\u5065\u5eb7\u72b6\u6001...")
            time.sleep(2)
            frontend_health_ok, frontend_health_detail = probe_http(frontend_url)
            if frontend_health_ok:
                self.append_deploy_log(f"  [OK] \u524d\u7aef\u5065\u5eb7\u68c0\u67e5: {frontend_health_detail}")
            else:
                self.append_deploy_log(f"  [WARN] \u524d\u7aef\u5065\u5eb7\u68c0\u67e5: {frontend_health_detail}")
        else:
            frontend_health_detail = "\u672a\u542f\u52a8\u524d\u7aef\uff0c\u5df2\u8df3\u8fc7\u5065\u5eb7\u68c0\u67e5"
            self.append_deploy_log(f"  [SKIP] {frontend_health_detail}")

        db_state, db_detail = self.refresh_database_status(
            force_check=True,
            show_dialog=False,
            sqlcmd_override=sqlcmd,
            schedule_next=False,
        )
        if db_state == "ok":
            self.append_deploy_log(f"  [OK] {db_detail}")
        elif db_state in {"missing_env", "missing_config", "missing_sqlcmd", "missing_runtime"}:
            self.append_deploy_log(f"  [SKIP] {db_detail}")
        else:
            self.append_deploy_log(f"  [WARN] {db_detail}")
        steps_passed += 1

        self.append_deploy_log(f"{'=' * 50}")
        self.append_deploy_log(f"\u4e00\u952e\u90e8\u7f72\u5b8c\u6210! \u901a\u8fc7 {steps_passed}/{total_steps} \u4e2a\u68c0\u67e5\u70b9")
        self.append_deploy_log(f"\u540e\u7aef\u5730\u5740: {backend_url}")
        self.append_deploy_log(f"\u524d\u7aef\u5730\u5740: {frontend_url}")
        self.append_deploy_log(f"{'=' * 50}")

        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        refresh_status_async()
        deploy_title = "\u4e00\u952e\u90e8\u7f72\u5b8c\u6210"
        deploy_notice = f"\u901a\u8fc7 {steps_passed}/{total_steps} \u4e2a\u68c0\u67e5\u70b9"
        deploy_lines = [
            f"\u90e8\u7f72\u6210\u529f\uff01\u901a\u8fc7 {steps_passed}/{total_steps} \u4e2a\u68c0\u67e5\u70b9\u3002",
            f"\u540e\u7aef\u5730\u5740\uff1a{backend_url}",
            f"\u524d\u7aef\u5730\u5740\uff1a{frontend_url}",
        ]
        if self.backend.is_running() or self.frontend.is_running():
            if not backend_health_ok or not frontend_health_ok:
                deploy_title = "\u4e00\u952e\u90e8\u7f72\u5b8c\u6210\uff08\u9700\u5173\u6ce8\uff09"
                deploy_notice = "\u670d\u52a1\u5df2\u542f\u52a8\uff0c\u4f46\u5065\u5eb7\u68c0\u67e5\u5b58\u5728\u544a\u8b66"
                if self.backend.is_running() and not backend_health_ok:
                    deploy_lines.append(f"\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\uff1a\u5f02\u5e38\uff0c{backend_health_detail}")
                if self.frontend.is_running() and not frontend_health_ok:
                    deploy_lines.append(f"\u524d\u7aef\u5065\u5eb7\u68c0\u67e5\uff1a\u5f02\u5e38\uff0c{frontend_health_detail}")
        else:
            deploy_lines.append("\u670d\u52a1\u542f\u52a8\u7b56\u7565\uff1a\u672c\u6b21\u90e8\u7f72\u4ec5\u5b8c\u6210\u6587\u4ef6\u3001\u4f9d\u8d56\u4e0e\u914d\u7f6e\u540c\u6b65\uff0c\u672a\u81ea\u52a8\u542f\u52a8\u670d\u52a1")

        if db_state == "ok":
            deploy_lines.append(f"\u6570\u636e\u5e93\u540e\u7f6e\u68c0\u67e5\uff1a\u6b63\u5e38\uff0c{db_detail}")
        elif db_state in {"missing_env", "missing_config", "missing_sqlcmd", "missing_runtime"}:
            if deploy_title == "\u4e00\u952e\u90e8\u7f72\u5b8c\u6210":
                deploy_title = "\u4e00\u952e\u90e8\u7f72\u5b8c\u6210\uff08\u5f85\u8865\u914d\u7f6e\uff09"
                deploy_notice = "\u670d\u52a1\u5df2\u90e8\u7f72\u5b8c\u6210\uff0c\u6570\u636e\u5e93\u4ecd\u9700\u540e\u7f6e\u914d\u7f6e"
            deploy_lines.append(f"\u6570\u636e\u5e93\u540e\u7f6e\u68c0\u67e5\uff1a\u5f85\u5904\u7406\uff0c{db_detail}")
        else:
            deploy_title = "\u4e00\u952e\u90e8\u7f72\u5b8c\u6210\uff08\u6570\u636e\u5e93\u68c0\u67e5\u544a\u8b66\uff09"
            deploy_notice = "\u670d\u52a1\u5df2\u90e8\u7f72\u5b8c\u6210\uff0c\u4f46\u6570\u636e\u5e93\u540e\u7f6e\u68c0\u67e5\u5931\u8d25"
            deploy_lines.append(f"\u6570\u636e\u5e93\u540e\u7f6e\u68c0\u67e5\uff1a\u5f02\u5e38\uff0c{db_detail}")

        self.notify_tray(deploy_title, deploy_notice)
        if deploy_title == "\u4e00\u952e\u90e8\u7f72\u5b8c\u6210":
            messagebox.showinfo(deploy_title, "\n".join(deploy_lines))
        else:
            messagebox.showwarning(deploy_title, "\n".join(deploy_lines))

    def manual_cleanup_backups(self) -> None:
        _ff_manual_cleanup_backups(self)

    def manual_cleanup_logs(self) -> None:
        _ff_manual_cleanup_logs(self)

    def send_test_alert(self) -> None:
        _ff_send_test_alert(self)

    def sync_backend_deps(self) -> None:
        _ff_sync_backend_deps(self)

    def sync_frontend_deps(self) -> None:
        _ff_sync_frontend_deps(self)

    def restore_database(self) -> None:
        _ff_restore_database(self)

    def hide_to_tray(self) -> None:
        self.root.withdraw()

    def show_window(self) -> None:
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def on_close_window(self) -> None:
        if self.manager_state.get("hide_to_tray_on_close", True):
            self.hide_to_tray()
            return
        self.exit_application()

    def exit_application(self) -> None:
        if self.exiting:
            return
        self.exiting = True
        for job_name in ("status_job", "log_job", "health_job", "db_job", "ui_queue_job"):
            job = getattr(self, job_name)
            if job:
                try:
                    self.root.after_cancel(job)
                except Exception:
                    pass
                setattr(self, job_name, None)
        if self.frontend.is_running():
            self.frontend.stop()
        if self.backend.is_running():
            self.backend.stop()
        if self.tray_icon:
            try:
                self.tray_icon.stop()
            except Exception:
                pass
        self.root.destroy()

    def refresh_status(self) -> None:
        _ff_refresh_status(self)

    def refresh_health_status(self) -> None:
        _ff_refresh_health_status(self)

    def refresh_database_status(
        self,
        force_check: bool = False,
        show_dialog: bool = False,
        sqlcmd_override: str | None = None,
        schedule_next: bool = True,
    ) -> Tuple[str, str]:
        return _ff_refresh_database_status(self, force_check, show_dialog, sqlcmd_override, schedule_next)

    def check_database_connection(self) -> None:
        self.save_manager_state()
        self.refresh_database_status(force_check=True, show_dialog=True, schedule_next=False)

    def resolve_log_path(self) -> Path:
        return _ff_resolve_log_path(self)

    def clear_current_log(self) -> None:
        _ff_clear_current_log(self)

    def summarize_log_view(self, path: Path, lines: List[str]) -> tuple[str, str]:
        return _ff_summarize_log_view(self, path, lines)

    def refresh_log_view(self, force: bool = False) -> None:
        _ff_refresh_log_view(self, force)

    def run(self) -> None:
        self.root.mainloop()


def describe_database_configuration(config: Dict[str, str]) -> Tuple[str, str]:
    conn, missing = get_database_connection_issues(config)
    if missing:
        return "missing_config", f"\u6570\u636e\u5e93\u8fde\u63a5\u914d\u7f6e\u4e0d\u5b8c\u6574\uff0c\u7f3a\u5c11\u5b57\u6bb5: {', '.join(missing)}"
    return "ready", f"{conn['host']}:{conn['port']} / {conn['dbname']} / \u7528\u6237 {conn['user']}"


def describe_database_configuration_from_disk() -> Tuple[str, str]:
    if not ENV_PATH.exists():
        return "missing_env", "\u672a\u627e\u5230 backend/.env\uff0c\u5f53\u524d\u8fd8\u6ca1\u6709\u53ef\u4f9b\u68c0\u67e5\u7684\u6570\u636e\u5e93\u8fde\u63a5\u914d\u7f6e"
    return describe_database_configuration(load_effective_config_from_disk())


def check_database_connectivity_via_backend_runtime(config: Dict[str, str]) -> Tuple[bool, str]:
    python_exe = get_backend_python_executable()
    if not python_exe.exists():
        return False, f"\u672a\u627e\u5230\u540e\u7aef\u865a\u62df\u73af\u5883 Python: {python_exe}"

    env = os.environ.copy()
    env.update(config)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    script = (
        "from sqlalchemy import create_engine, text\n"
        "from database import _build_database_url, _connect_args\n"
        "engine = create_engine(_build_database_url(), pool_pre_ping=True, connect_args=_connect_args)\n"
        "with engine.connect() as conn:\n"
        "    conn.execute(text('SELECT 1'))\n"
        "print('\\u6570\\u636e\\u5e93\\u8fde\\u63a5\\u6210\\u529f')\n"
    )
    try:
        result = subprocess.run(
            [str(python_exe), "-c", script],
            cwd=BACKEND_DIR,
            env=env,
            capture_output=True,
            text=False,
            timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        return False, f"\u540e\u7aef\u8fd0\u884c\u65f6\u6570\u636e\u5e93\u63a2\u6d4b\u5931\u8d25: {exc}"

    stdout_text = decode_console_output(result.stdout or b"").strip()
    stderr_text = decode_console_output(result.stderr or b"").strip()
    if result.returncode == 0:
        conn = extract_db_connection(config)
        return True, stdout_text or f"\u6570\u636e\u5e93 {conn.get('dbname') or ''} \u8fde\u63a5\u6b63\u5e38".strip()
    combined_text = stderr_text or stdout_text or "\u6570\u636e\u5e93\u8fde\u63a5\u5931\u8d25"
    if "ModuleNotFoundError" in combined_text or "No module named" in combined_text:
        return False, f"\u540e\u7aef\u8fd0\u884c\u73af\u5883\u7f3a\u5c11\u6570\u636e\u5e93\u4f9d\u8d56\uff0c\u65e0\u6cd5\u5b8c\u6210\u8fde\u63a5\u68c0\u67e5: {combined_text}"
    return False, combined_text


def evaluate_database_runtime_status(sqlcmd: str = "sqlcmd") -> Tuple[str, str]:
    config_state, config_detail = describe_database_configuration_from_disk()
    if config_state != "ready":
        return config_state, config_detail

    config = load_effective_config_from_disk()
    backend_host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
    backend_port = (config.get("APP_PORT") or "8100").strip() or "8100"
    if backend_port.isdigit():
        backend_ok, payload, _ = fetch_backend_health_payload(f"http://{backend_host}:{backend_port}")
        if payload:
            database_status = payload.get("database")
            if backend_ok and database_status == "ok":
                conn = extract_db_connection(config)
                return "ok", f"\u6570\u636e\u5e93 {conn.get('dbname') or ''} \u8fde\u63a5\u6b63\u5e38\uff08\u6765\u81ea\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\uff09".strip()
            if database_status and database_status != "ok":
                return "error", f"\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\u8fd4\u56de\u6570\u636e\u5e93\u72b6\u6001: {database_status}"

    runtime_python = get_backend_python_executable()
    if runtime_python.exists():
        ok, detail = check_database_connectivity_via_backend_runtime(config)
        if ok:
            return "ok", detail
        if "\u7f3a\u5c11\u6570\u636e\u5e93\u4f9d\u8d56" in detail:
            return "missing_runtime", detail
        return "error", detail

    sqlcmd = (sqlcmd or "").strip()
    sqlcmd_available = False
    if sqlcmd:
        sqlcmd_path = Path(sqlcmd)
        sqlcmd_available = sqlcmd_path.exists() if sqlcmd_path.suffix else shutil.which(sqlcmd) is not None
    if not sqlcmd_available:
        return "missing_runtime", "\u672a\u627e\u5230\u540e\u7aef\u8fd0\u884c\u65f6 Python\uff0c\u4e5f\u65e0\u6cd5\u4f7f\u7528 sqlcmd\uff0c\u65e0\u6cd5\u6267\u884c\u6570\u636e\u5e93\u8fde\u63a5\u68c0\u67e5"

    ok, detail = check_database_connectivity(config, sqlcmd)
    return ("ok" if ok else "error"), detail


def format_database_config_status(state: str, detail: str) -> str:
    prefix = {
        "ready": "\u5df2\u5c31\u7eea",
        "missing_env": "\u672a\u627e\u5230\u73af\u5883\u6587\u4ef6",
        "missing_config": "\u914d\u7f6e\u4e0d\u5b8c\u6574",
    }.get(state, "\u672a\u77e5")
    return f"{prefix} ({detail})"


def format_database_connection_status(state: str, detail: str) -> str:
    prefix = {
        "ok": "\u6b63\u5e38",
        "error": "\u5f02\u5e38",
        "missing_env": "\u672a\u627e\u5230\u73af\u5883\u6587\u4ef6",
        "missing_config": "\u914d\u7f6e\u4e0d\u5b8c\u6574",
        "missing_sqlcmd": "sqlcmd \u4e0d\u53ef\u7528",
        "missing_runtime": "\u8fd0\u884c\u65f6\u7f3a\u5931",
        "disabled": "\u5df2\u7981\u7528",
    }.get(state, "\u672a\u68c0\u67e5")
    return f"{prefix} ({detail})"


def run_database_backup(config: Dict[str, str], backup_dir: Path, sqlcmd: str) -> Tuple[bool, str]:
    backup_dir.mkdir(parents=True, exist_ok=True)
    conn = extract_db_connection(config)
    missing = [key for key in ("host", "port", "user", "dbname") if not conn.get(key)]
    if missing:
        return False, f"\u6570\u636e\u5e93\u8fde\u63a5\u914d\u7f6e\u4e0d\u5b8c\u6574\uff0c\u7f3a\u5c11\u5b57\u6bb5: {', '.join(missing)}"

    filename = f"finflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
    target_file = backup_dir / filename
    query = f"BACKUP DATABASE [{conn['dbname']}] TO DISK = N'{target_file}' WITH INIT, NAME = N'FinFlow Full Backup'"
    cmd = [sqlcmd, "-S", f"{conn['host']},{conn['port']}", "-U", conn["user"], "-d", conn["dbname"], "-Q", query]
    if conn.get("password"):
        cmd.extend(["-P", conn["password"]])

    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT_DIR,
            capture_output=True,
            text=False,
            timeout=600,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, f"\u672a\u627e\u5230 sqlcmd: {sqlcmd}"
    except Exception as exc:
        return False, f"\u6267\u884c sqlcmd \u5907\u4efd\u5931\u8d25: {exc}"

    stdout_text = decode_console_output(result.stdout or b"")
    stderr_text = decode_console_output(result.stderr or b"")
    if result.returncode != 0:
        if target_file.exists():
            remove_path(target_file)
        return False, (stderr_text or stdout_text or "\u5907\u4efd\u5931\u8d25").strip()
    return True, str(target_file)


def check_database_connectivity(config: Dict[str, str], sqlcmd: str = "sqlcmd") -> Tuple[bool, str]:
    conn, missing = get_database_connection_issues(config)
    if missing:
        return False, f"\u6570\u636e\u5e93\u8fde\u63a5\u914d\u7f6e\u4e0d\u5b8c\u6574\uff0c\u7f3a\u5c11\u5b57\u6bb5: {', '.join(missing)}"

    try:
        cmd = [sqlcmd, "-S", f"{conn['host']},{conn['port']}", "-U", conn["user"], "-d", conn["dbname"], "-Q", "SELECT 1", "-b"]
        if conn.get("password"):
            cmd.extend(["-P", conn["password"]])

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        result = subprocess.run(cmd, capture_output=True, text=False, timeout=15, creationflags=creationflags)
        if result.returncode == 0:
            return True, f"\u6570\u636e\u5e93 {conn['dbname']} \u8fde\u63a5\u6b63\u5e38"
        stderr_text = decode_console_output(result.stderr or b"")
        stdout_text = decode_console_output(result.stdout or b"")
        return False, (stderr_text or stdout_text or "\u6570\u636e\u5e93\u8fde\u63a5\u5931\u8d25").strip()
    except FileNotFoundError:
        return False, f"\u672a\u627e\u5230 sqlcmd: {sqlcmd}"
    except Exception as exc:
        return False, f"\u6570\u636e\u5e93\u8fde\u63a5\u68c0\u67e5\u5931\u8d25: {exc}"


def _is_port_conflict_message(message: str) -> bool:
    text = (message or "").lower()
    return any(token in text for token in ("\u7aef\u53e3", "\u5360\u7528", "port", "occupied", "address already in use"))


def _clean_poll_status(self: Any) -> str:
    if self.is_running():
        pid = int(getattr(getattr(self, "process", None), "pid", 0) or 0)
        return f"\u8fd0\u884c\u4e2d (PID {pid})"
    exit_code = self.capture_exit()
    if exit_code is not None:
        return f"\u5df2\u9000\u51fa (code {exit_code})"
    return "\u672a\u8fd0\u884c"


def _managed_backend_poll_status(self: Any) -> str:
    state = self._state()
    host_pid = int(state.get("service_host_pid") or 0)
    backend_pid = int(state.get("backend_pid") or 0)
    if state.get("service_host_alive") and state.get("backend_alive"):
        return f"\u8fd0\u884c\u4e2d (\u4e3b\u63a7 PID {host_pid}, \u540e\u7aef PID {backend_pid})"
    if state.get("service_host_alive"):
        return f"\u4e3b\u63a7\u8fdb\u7a0b\u8fd0\u884c\u4e2d (PID {host_pid})\uff0c\u7b49\u5f85\u540e\u7aef\u5c31\u7eea"
    if state.get("backend_alive"):
        return f"\u540e\u7aef\u8fd0\u884c\u4e2d (PID {backend_pid})\uff0c\u4f46\u4e3b\u63a7\u8fdb\u7a0b\u5df2\u4e22\u5931"
    if state.get("last_exit_code") is not None:
        return f"\u672a\u8fd0\u884c (\u4e0a\u6b21\u9000\u51fa code {state['last_exit_code']})"
    return "\u672a\u8fd0\u884c"


def _ff_build_status_tab(self: Any, parent: ttk.Frame) -> None:
    db_config_state, db_config_detail = describe_database_configuration_from_disk()
    parent.columnconfigure(0, weight=1)
    parent.rowconfigure(1, weight=1)
    self.status_badges = {}

    status_items = {
        "project_root": str(ROOT_DIR),
        "env_file": "\u5df2\u5b58\u5728" if ENV_PATH.exists() else "\u4e0d\u5b58\u5728",
        "key_file": "\u5df2\u5b58\u5728" if KEY_PATH.exists() else "\u4e0d\u5b58\u5728",
        "dist_dir": "\u5df2\u5b58\u5728" if (DIST_DIR / "index.html").exists() else "\u4e0d\u5b58\u5728",
        "backend_status": self.backend.poll_status(),
        "backend_port_owner": "\u672a\u68c0\u6d4b",
        "backend_health_status": "\u672a\u68c0\u67e5",
        "backend_url": self.get_backend_url(),
        "frontend_status": self.frontend.poll_status(),
        "frontend_port_owner": "\u672a\u68c0\u6d4b",
        "frontend_health_status": "\u672a\u68c0\u67e5",
        "frontend_url": self.get_frontend_url(),
        "db_config_status": format_database_config_status(db_config_state, db_config_detail),
        "db_connection_status": "\u672a\u68c0\u67e5",
        "db_monitor_status": "\u5df2\u542f\u7528" if self.manager_state.get("enable_db_monitor", True) else "\u5df2\u7981\u7528",
        "db_last_check_at": "\u672a\u68c0\u67e5",
        "startup_status": self.get_startup_status_text(),
        "app_url": self.get_app_url(),
    }
    for key, value in status_items.items():
        self.status_vars[key] = tk.StringVar(value=value)

    overview = ttk.LabelFrame(parent, text="\u6982\u89c8", padding=12)
    overview.grid(row=0, column=0, sticky="nsew", padx=10, pady=(10, 6))
    for col in (1, 3):
        overview.columnconfigure(col, weight=1)

    overview_fields = [
        ("\u9879\u76ee\u76ee\u5f55", "project_root", 0, 0, 320),
        ("\u5e94\u7528\u5165\u53e3", "app_url", 0, 2, 320),
        ("\u73af\u5883\u6587\u4ef6", "env_file", 1, 0, 180),
        ("\u5bc6\u94a5\u6587\u4ef6", "key_file", 1, 2, 180),
        ("\u524d\u7aef\u6784\u5efa", "dist_dir", 2, 0, 180),
        ("\u542f\u52a8\u72b6\u6001", "startup_status", 2, 2, 180),
    ]
    for label_text, key, row, col, wrap_length in overview_fields:
        ttk.Label(overview, text=f"{label_text}\uff1a").grid(row=row, column=col, sticky="w", padx=(0, 6), pady=4)
        ttk.Label(overview, textvariable=self.status_vars[key], justify="left", wraplength=wrap_length).grid(
            row=row, column=col + 1, sticky="w", padx=(0, 16), pady=4
        )

    dashboard = ttk.Frame(parent)
    dashboard.grid(row=1, column=0, sticky="nsew", padx=10, pady=6)
    dashboard.columnconfigure(0, weight=1)
    dashboard.columnconfigure(1, weight=1)
    dashboard.rowconfigure(0, weight=1)
    dashboard.rowconfigure(1, weight=1)

    sections = [
        ("\u540e\u7aef\u670d\u52a1", [("\u8fd0\u884c\u72b6\u6001", "backend_status"), ("\u7aef\u53e3\u5360\u7528", "backend_port_owner"), ("\u5065\u5eb7\u68c0\u67e5", "backend_health_status"), ("\u8bbf\u95ee\u5730\u5740", "backend_url")], 0, 0),
        ("\u524d\u7aef\u670d\u52a1", [("\u8fd0\u884c\u72b6\u6001", "frontend_status"), ("\u7aef\u53e3\u5360\u7528", "frontend_port_owner"), ("\u5065\u5eb7\u68c0\u67e5", "frontend_health_status"), ("\u8bbf\u95ee\u5730\u5740", "frontend_url")], 0, 1),
        ("\u6570\u636e\u5e93\u72b6\u6001", [("\u914d\u7f6e\u72b6\u6001", "db_config_status"), ("\u8fde\u63a5\u72b6\u6001", "db_connection_status"), ("\u76d1\u63a7\u72b6\u6001", "db_monitor_status"), ("\u4e0a\u6b21\u68c0\u67e5", "db_last_check_at")], 1, 0),
        ("\u5feb\u6377\u64cd\u4f5c", [], 1, 1),
    ]
    for title, fields, row, column in sections:
        frame = ttk.LabelFrame(dashboard, text=title, padding=12)
        frame.grid(row=row, column=column, sticky="nsew", padx=6, pady=6)
        frame.columnconfigure(1, weight=1)
        if fields:
            for field_row, (label_text, key) in enumerate(fields):
                ttk.Label(frame, text=f"{label_text}\uff1a").grid(row=field_row, column=0, sticky="nw", padx=(0, 8), pady=4)
                if key in {"backend_status", "frontend_status", "backend_health_status", "frontend_health_status", "db_connection_status", "db_monitor_status"}:
                    badge = tk.Label(frame, textvariable=self.status_vars[key], anchor="w", justify="left", padx=8, pady=3, relief="groove", bd=1)
                    badge.grid(row=field_row, column=1, sticky="ew", pady=4)
                    self.status_badges[key] = badge
                else:
                    wrap_length = 320 if key in {"db_config_status", "backend_url", "frontend_url"} else 240
                    ttk.Label(frame, textvariable=self.status_vars[key], justify="left", wraplength=wrap_length).grid(
                        row=field_row, column=1, sticky="w", pady=4
                    )
        else:
            action_groups = [
                ("\u5168\u90e8\u670d\u52a1", [("\u542f\u52a8\u5168\u90e8", self.handle_start_all), ("\u505c\u6b62\u5168\u90e8", self.handle_stop_all), ("\u91cd\u542f\u5168\u90e8", self.handle_restart_all), ("\u6253\u5f00\u524d\u7aef", self.open_frontend), ("\u6253\u5f00\u65e5\u5fd7\u76ee\u5f55", self.open_logs_folder), ("\u6700\u5c0f\u5316\u5230\u6258\u76d8", self.hide_to_tray)]),
                ("\u540e\u7aef\u670d\u52a1", [("\u542f\u52a8\u540e\u7aef", self.handle_start_backend), ("\u505c\u6b62\u540e\u7aef", self.handle_stop_backend), ("\u91cd\u542f\u540e\u7aef", self.handle_restart_backend), ("\u63a5\u7ba1\u540e\u7aef", self.handle_takeover_backend), ("\u91ca\u653e\u7aef\u53e3", self.handle_force_release_port)]),
                ("\u524d\u7aef\u4e0e\u6570\u636e\u5e93", [("\u542f\u52a8\u524d\u7aef", self.handle_start_frontend), ("\u505c\u6b62\u524d\u7aef", self.handle_stop_frontend), ("\u91cd\u542f\u524d\u7aef", self.handle_restart_frontend), ("\u68c0\u67e5\u6570\u636e\u5e93", self.check_database_connection)]),
            ]
            frame.columnconfigure(0, weight=1)
            for group_row, (group_title, action_specs) in enumerate(action_groups):
                group = ttk.LabelFrame(frame, text=group_title, padding=8)
                group.grid(row=group_row, column=0, sticky="ew", pady=4)
                for action_col in range(3):
                    group.columnconfigure(action_col, weight=1)
                for idx, (button_text, command) in enumerate(action_specs):
                    button = ttk.Button(group, text=button_text, command=command)
                    button.grid(row=idx // 3, column=idx % 3, sticky="ew", padx=3, pady=3)
                    action_key = {
                        "\u542f\u52a8\u5168\u90e8": "start_all",
                        "\u505c\u6b62\u5168\u90e8": "stop_all",
                        "\u91cd\u542f\u5168\u90e8": "restart_all",
                        "\u542f\u52a8\u540e\u7aef": "start_backend",
                        "\u505c\u6b62\u540e\u7aef": "stop_backend",
                        "\u91cd\u542f\u540e\u7aef": "restart_backend",
                        "\u542f\u52a8\u524d\u7aef": "start_frontend",
                        "\u505c\u6b62\u524d\u7aef": "stop_frontend",
                        "\u91cd\u542f\u524d\u7aef": "restart_frontend",
                    }.get(button_text)
                    if action_key:
                        self.service_action_buttons[action_key] = button

    self.refresh_status_badges()
    _update_service_action_buttons(self)


def _ff_get_status_badge_palette(self: Any, key: str, value: str) -> Tuple[str, str]:
    text = (value or "").lower()
    if any(keyword in text for keyword in ("\u8fd0\u884c\u4e2d", "\u6b63\u5e38", "\u5df2\u542f\u7528", "\u5df2\u5c31\u7eea", "\u5df2\u5b58\u5728", "http 200", "\u53ef\u8bbf\u95ee")):
        return "#e8f7ee", "#156f3d"
    if any(keyword in text for keyword in ("\u672a\u8fd0\u884c", "\u672a\u68c0\u67e5", "\u5df2\u7981\u7528", "\u4e0d\u5b58\u5728", "\u7b49\u5f85\u4e2d", "\u624b\u52a8\u68c0\u67e5", "\u672a\u68c0\u6d4b")):
        return "#eef2f7", "#5b6575"
    if any(keyword in text for keyword in ("\u672a\u627e\u5230\u73af\u5883\u6587\u4ef6", "\u914d\u7f6e\u4e0d\u5b8c\u6574", "\u7f3a\u5c11", "\u8fd0\u884c\u65f6\u7f3a\u5931", "\u4e0d\u53ef\u7528", "\u5f85\u914d\u7f6e", "warn")):
        return "#fff5dd", "#8a5a00"
    if any(keyword in text for keyword in ("\u5f02\u5e38", "\u5931\u8d25", "\u5df2\u9000\u51fa", "\u4e0d\u53ef\u8bbf\u95ee", "error", "traceback")):
        return "#fdeceb", "#b42318"
    if key in {"backend_health_status", "frontend_health_status", "db_connection_status"}:
        return "#eef2f7", "#5b6575"
    return "#f4f4f5", "#374151"


def _ff_manual_cleanup_backups(self: Any) -> None:
    self.save_manager_state()
    backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
    retention_days = int(self.ops_vars["backup_retention_days"].get().strip() or 30)
    retention_count = int(self.ops_vars["backup_retention_count"].get().strip() or 10)
    _removed, msg = cleanup_old_backups(backup_dir, retention_days, retention_count)
    messagebox.showinfo("\u6e05\u7406\u5b8c\u6210", msg)


def _ff_manual_cleanup_logs(self: Any) -> None:
    self.save_manager_state()
    retention_days = int(self.ops_vars["log_archive_retention_days"].get().strip() or 90)
    _removed, msg = cleanup_old_archived_logs(retention_days)
    messagebox.showinfo("\u6e05\u7406\u5b8c\u6210", msg)


def _ff_send_test_alert(self: Any) -> None:
    self.save_manager_state()
    webhook_url = self.ops_vars["webhook_url"].get().strip()
    if not webhook_url:
        messagebox.showwarning("\u7f3a\u5c11\u914d\u7f6e", "\u8bf7\u5148\u586b\u5199\u544a\u8b66 Webhook URL")
        return
    ok = send_webhook_notification(webhook_url, "\u6d4b\u8bd5\u544a\u8b66", "\u8fd9\u662f\u4e00\u6761\u6765\u81ea FinFlow \u670d\u52a1\u7ba1\u7406\u5668\u7684\u6d4b\u8bd5\u544a\u8b66\u6d88\u606f\u3002")
    if ok:
        messagebox.showinfo("\u53d1\u9001\u6210\u529f", "\u6d4b\u8bd5\u544a\u8b66\u5df2\u53d1\u9001\uff0c\u8bf7\u5728 Webhook \u63a5\u6536\u7aef\u786e\u8ba4\u6d88\u606f\u3002")
    else:
        messagebox.showerror("\u53d1\u9001\u5931\u8d25", "Webhook \u6d4b\u8bd5\u5931\u8d25\uff0c\u8bf7\u68c0\u67e5 URL \u662f\u5426\u6b63\u786e\u4e14\u63a5\u6536\u7aef\u53ef\u8fbe\u3002")


def _ff_sync_backend_deps(self: Any) -> None:
    proceed = messagebox.askyesno("\u540c\u6b65\u540e\u7aef\u4f9d\u8d56", "\u5c06\u4f7f\u7528 pip \u6839\u636e backend/requirements.txt \u540c\u6b65\u540e\u7aef Python \u4f9d\u8d56\u3002\n\u8fd9\u53ef\u80fd\u9700\u8981\u51e0\u5206\u949f\uff0c\u662f\u5426\u7ee7\u7eed\uff1f")
    if not proceed:
        return
    self.log_status_var.set("\u6b63\u5728\u540c\u6b65\u540e\u7aef Python \u4f9d\u8d56...")
    ok, detail = sync_backend_dependencies()
    if ok:
        messagebox.showinfo("\u540c\u6b65\u5b8c\u6210", detail)
        self.notify_tray("\u540e\u7aef\u4f9d\u8d56\u540c\u6b65\u5b8c\u6210", "\u540e\u7aef Python \u4f9d\u8d56\u5df2\u540c\u6b65")
    else:
        messagebox.showerror("\u540c\u6b65\u5931\u8d25", detail)


def _ff_sync_frontend_deps(self: Any) -> None:
    proceed = messagebox.askyesno("\u540c\u6b65\u524d\u7aef\u4f9d\u8d56", "\u5c06\u4f7f\u7528 npm install \u6839\u636e frontend/package.json \u540c\u6b65\u524d\u7aef\u4f9d\u8d56\u3002\n\u8fd9\u53ef\u80fd\u9700\u8981\u51e0\u5206\u949f\uff0c\u662f\u5426\u7ee7\u7eed\uff1f")
    if not proceed:
        return
    self.log_status_var.set("\u6b63\u5728\u540c\u6b65\u524d\u7aef Node \u4f9d\u8d56...")
    ok, detail = sync_frontend_dependencies()
    if ok:
        messagebox.showinfo("\u540c\u6b65\u5b8c\u6210", detail)
        self.notify_tray("\u524d\u7aef\u4f9d\u8d56\u540c\u6b65\u5b8c\u6210", "\u524d\u7aef Node \u4f9d\u8d56\u5df2\u540c\u6b65")
    else:
        messagebox.showerror("\u540c\u6b65\u5931\u8d25", detail)


def _ff_refresh_status(self: Any) -> None:
    backend_status = self.backend.poll_status()
    frontend_status = self.frontend.poll_status()
    config = self.get_effective_config()
    backend_port = (config.get("APP_PORT") or "8100").strip() or "8100"
    frontend_settings = resolve_frontend_service_settings(config)
    frontend_port = frontend_settings["frontend_port"]
    backend_owner_text = "\u672a\u68c0\u6d4b"
    frontend_owner_text = "\u672a\u68c0\u6d4b"

    if backend_port.isdigit():
        owner = get_port_owner_info(int(backend_port))
        if owner:
            manager_pid = self.backend.process.pid if self.backend.is_running() and self.backend.process else None
            backend_owner_text = build_port_owner_label(owner, manager_pid)
    if frontend_port.isdigit():
        owner = get_port_owner_info(int(frontend_port))
        if owner:
            manager_pid = int(getattr(self.frontend.process, "pid", 0) or 0) if self.frontend.is_running() else None
            frontend_owner_text = build_port_owner_label(owner, manager_pid)

    self.status_vars["env_file"].set("\u5df2\u5b58\u5728" if ENV_PATH.exists() else "\u4e0d\u5b58\u5728")
    self.status_vars["key_file"].set("\u5df2\u5b58\u5728" if KEY_PATH.exists() else "\u4e0d\u5b58\u5728")
    self.status_vars["dist_dir"].set("\u5df2\u5b58\u5728" if (DIST_DIR / "index.html").exists() else "\u4e0d\u5b58\u5728")
    self.status_vars["backend_status"].set(backend_status)
    self.status_vars["backend_port_owner"].set(backend_owner_text)
    self.status_vars["backend_url"].set(self.get_backend_url())
    self.status_vars["frontend_status"].set(frontend_status)
    self.status_vars["frontend_port_owner"].set(frontend_owner_text)
    self.status_vars["frontend_url"].set(self.get_frontend_url())
    self.status_vars["startup_status"].set(self.get_startup_status_text())
    self.status_vars["app_url"].set(self.get_app_url())
    db_config_state, db_config_detail = describe_database_configuration_from_disk()
    self.status_vars["db_config_status"].set(format_database_config_status(db_config_state, db_config_detail))
    self.refresh_status_badges()

    if self.backend.last_exit_at > self.last_notified_exit_at and not self.backend.user_stopped and self.backend.last_exit_code is not None:
        self.last_notified_exit_at = self.backend.last_exit_at
        fast_failed = self.backend.register_failed_start_if_needed()
        exit_message = f"\u9000\u51fa\u4ee3\u7801: {self.backend.last_exit_code}"
        self.notify_tray("\u540e\u7aef\u670d\u52a1\u5f02\u5e38\u9000\u51fa", exit_message)
        webhook_url = self.manager_state.get("webhook_url", "")
        send_webhook_notification(webhook_url, "\u540e\u7aef\u670d\u52a1\u5f02\u5e38\u9000\u51fa", exit_message)
        if self.backend.auto_restart_suppressed:
            self.status_vars["backend_status"].set(f"\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c\uff08\u6700\u8fd1\u9000\u51fa\u4ee3\u7801 {self.backend.last_exit_code}\uff09")
            suppressed_message = "\u540e\u7aef\u8fde\u7eed 3 \u6b21\u5feb\u901f\u9000\u51fa\uff0c\u5df2\u6682\u505c\u81ea\u52a8\u91cd\u542f\uff0c\u8bf7\u68c0\u67e5\u65e5\u5fd7\u4e0e\u914d\u7f6e\u540e\u624b\u52a8\u6062\u590d\u3002"
            self.notify_tray("\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c", suppressed_message)
            send_webhook_notification(webhook_url, "\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c", suppressed_message)
        elif fast_failed:
            self.status_vars["backend_status"].set(f"\u540e\u7aef\u8fde\u7eed\u5feb\u901f\u9000\u51fa\uff0c\u81ea\u52a8\u91cd\u542f\u4fdd\u62a4\u5df2\u8ba1\u6570 ({self.backend.consecutive_failed_starts}/3)")

    if self.manager_state.get("auto_restart_backend", True) and not self.backend.is_running() and not self.backend.user_stopped and not self.backend.auto_restart_suppressed and time.time() - self.backend.last_start_attempt > 8 and self.backend.last_start_attempt > 0:
        ok, message = self.backend.start(self.get_effective_config())
        if ok:
            self.notify_tray("\u540e\u7aef\u670d\u52a1\u5df2\u81ea\u52a8\u91cd\u542f", message)
            self.status_vars["backend_status"].set(self.backend.poll_status())
        elif _is_port_conflict_message(message):
            self.backend.auto_restart_suppressed = True
            self.status_vars["backend_status"].set(message)
            self.notify_tray("\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c", message)

    if self.backend.is_running():
        self.backend.consecutive_failed_starts = 0

    if self.frontend.last_exit_at > self.last_notified_frontend_exit_at and not self.frontend.user_stopped and self.frontend.last_exit_code is not None:
        self.last_notified_frontend_exit_at = self.frontend.last_exit_at
        fast_failed = self.frontend.register_failed_start_if_needed()
        exit_message = f"\u9000\u51fa\u4ee3\u7801: {self.frontend.last_exit_code}"
        self.notify_tray("\u524d\u7aef\u670d\u52a1\u5f02\u5e38\u9000\u51fa", exit_message)
        webhook_url = self.manager_state.get("webhook_url", "")
        send_webhook_notification(webhook_url, "\u524d\u7aef\u670d\u52a1\u5f02\u5e38\u9000\u51fa", exit_message)
        if self.frontend.auto_restart_suppressed:
            self.status_vars["frontend_status"].set(f"\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c\uff08\u6700\u8fd1\u9000\u51fa\u4ee3\u7801 {self.frontend.last_exit_code}\uff09")
            suppressed_message = "\u524d\u7aef\u8fde\u7eed 3 \u6b21\u5feb\u901f\u9000\u51fa\uff0c\u5df2\u6682\u505c\u81ea\u52a8\u91cd\u542f\uff0c\u8bf7\u68c0\u67e5\u65e5\u5fd7\u4e0e\u914d\u7f6e\u540e\u624b\u52a8\u6062\u590d\u3002"
            self.notify_tray("\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c", suppressed_message)
            send_webhook_notification(webhook_url, "\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c", suppressed_message)
        elif fast_failed:
            self.status_vars["frontend_status"].set(f"\u524d\u7aef\u8fde\u7eed\u5feb\u901f\u9000\u51fa\uff0c\u81ea\u52a8\u91cd\u542f\u4fdd\u62a4\u5df2\u8ba1\u6570 ({self.frontend.consecutive_failed_starts}/3)")

    if self.manager_state.get("auto_restart_frontend", True) and not self.frontend.is_running() and not self.frontend.user_stopped and not self.frontend.auto_restart_suppressed and time.time() - self.frontend.last_start_attempt > 8 and self.frontend.last_start_attempt > 0:
        ok, message = self.frontend.start(self.get_effective_config())
        if ok:
            self.notify_tray("\u524d\u7aef\u670d\u52a1\u5df2\u81ea\u52a8\u91cd\u542f", message)
            self.status_vars["frontend_status"].set(self.frontend.poll_status())
        elif _is_port_conflict_message(message):
            self.frontend.auto_restart_suppressed = True
            self.frontend._sync_runtime_state(frontend_auto_restart_suppressed=True)
            self.status_vars["frontend_status"].set(message)
            self.notify_tray("\u81ea\u52a8\u91cd\u542f\u5df2\u6682\u505c", message)

    if self.frontend.is_running():
        self.frontend.consecutive_failed_starts = 0
        self.frontend._sync_runtime_state(frontend_consecutive_failed_starts=0)

    self.update_tray_title()
    self.schedule_status_refresh()


def _ff_refresh_health_status(self: Any) -> None:
    if self.exiting:
        return
    if not self.manager_state.get("enable_health_check", True):
        self.status_vars["backend_health_status"].set("\u5df2\u7981\u7528")
        self.status_vars["frontend_health_status"].set("\u5df2\u7981\u7528")
        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        self.update_tray_title()
        self.schedule_health_refresh()
        return

    backend_ok = False
    backend_detail = "\u672a\u68c0\u67e5"
    backend_state = "unknown"
    if self.backend.is_running():
        backend_ok, backend_detail = probe_backend_health(self.get_backend_url())
        backend_state = "healthy" if backend_ok else "unhealthy"
    self.status_vars["backend_health_status"].set(f"\u6b63\u5e38 ({backend_detail})" if backend_ok else f"\u5f02\u5e38 ({backend_detail})" if backend_state == "unhealthy" else "\u672a\u68c0\u67e5")

    if self.backend.is_running() and self.last_backend_health_state not in {"unknown", backend_state}:
        webhook_url = self.manager_state.get("webhook_url", "")
        if backend_ok:
            message = f"{self.get_backend_url()} \u5df2\u6062\u590d\u6b63\u5e38\u54cd\u5e94"
            self.notify_tray("\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\u5df2\u6062\u590d", message)
            send_webhook_notification(webhook_url, "\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\u5df2\u6062\u590d", message)
        else:
            message = f"{self.get_backend_url()} \u5065\u5eb7\u68c0\u67e5\u5f02\u5e38: {backend_detail}"
            self.notify_tray("\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\u5f02\u5e38", message)
            send_webhook_notification(webhook_url, "\u540e\u7aef\u5065\u5eb7\u68c0\u67e5\u5f02\u5e38", message)
    self.last_backend_health_state = backend_state

    frontend_ok = False
    frontend_detail = "\u672a\u68c0\u67e5"
    frontend_state = "unknown"
    if self.frontend.is_running():
        frontend_ok, frontend_detail = probe_http(self.get_frontend_url())
        frontend_state = "healthy" if frontend_ok else "unhealthy"
    self.status_vars["frontend_health_status"].set(f"\u6b63\u5e38 ({frontend_detail})" if frontend_ok else f"\u5f02\u5e38 ({frontend_detail})" if frontend_state == "unhealthy" else "\u672a\u68c0\u67e5")

    if self.frontend.is_running() and self.last_frontend_health_state not in {"unknown", frontend_state}:
        webhook_url = self.manager_state.get("webhook_url", "")
        if frontend_ok:
            message = f"{self.get_frontend_url()} \u5df2\u6062\u590d\u6b63\u5e38\u54cd\u5e94"
            self.notify_tray("\u524d\u7aef\u5065\u5eb7\u68c0\u67e5\u5df2\u6062\u590d", message)
            send_webhook_notification(webhook_url, "\u524d\u7aef\u5065\u5eb7\u68c0\u67e5\u5df2\u6062\u590d", message)
        else:
            message = f"{self.get_frontend_url()} \u5065\u5eb7\u68c0\u67e5\u5f02\u5e38: {frontend_detail}"
            self.notify_tray("\u524d\u7aef\u5065\u5eb7\u68c0\u67e5\u5f02\u5e38", message)
            send_webhook_notification(webhook_url, "\u524d\u7aef\u5065\u5eb7\u68c0\u67e5\u5f02\u5e38", message)
    self.last_frontend_health_state = frontend_state
    self.update_tray_title()
    self.refresh_status_badges()
    self.schedule_health_refresh()


def _ff_refresh_database_status(self: Any, force_check: bool = False, show_dialog: bool = False, sqlcmd_override: str | None = None, schedule_next: bool = True) -> Tuple[str, str]:
    if self.exiting:
        return "unknown", "\u5e94\u7528\u6b63\u5728\u9000\u51fa\uff0c\u8df3\u8fc7\u6570\u636e\u5e93\u68c0\u67e5"

    monitor_enabled = self.manager_state.get("enable_db_monitor", True)
    if not force_check and not monitor_enabled:
        self.status_vars["db_monitor_status"].set("\u5df2\u7981\u7528")
        self.status_vars["db_connection_status"].set("\u672a\u68c0\u67e5\uff08\u6570\u636e\u5e93\u76d1\u63a7\u5df2\u7981\u7528\uff09")
        if schedule_next:
            self.schedule_db_refresh()
        self.last_database_status_state = "unknown"
        return "disabled", "\u6570\u636e\u5e93\u76d1\u63a7\u5df2\u7981\u7528"

    sqlcmd = (sqlcmd_override if sqlcmd_override is not None else self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd").strip()
    state, detail = evaluate_database_runtime_status(sqlcmd)
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.status_vars["db_connection_status"].set(format_database_connection_status(state, detail))
    self.status_vars["db_monitor_status"].set("\u624b\u52a8\u68c0\u67e5" if force_check and not monitor_enabled else "\u5df2\u542f\u7528" if monitor_enabled else "\u5df2\u7981\u7528")
    self.status_vars["db_last_check_at"].set(checked_at)

    previous_state = self.last_database_status_state
    if monitor_enabled and not force_check and previous_state not in {"unknown", state}:
        webhook_url = self.manager_state.get("webhook_url", "")
        if state == "ok":
            self.notify_tray("\u6570\u636e\u5e93\u8fde\u63a5\u5df2\u6062\u590d", detail)
            send_webhook_notification(webhook_url, "\u6570\u636e\u5e93\u8fde\u63a5\u5df2\u6062\u590d", detail)
        else:
            self.notify_tray("\u6570\u636e\u5e93\u8fde\u63a5\u5f02\u5e38", detail)
            send_webhook_notification(webhook_url, "\u6570\u636e\u5e93\u8fde\u63a5\u5f02\u5e38", detail)
    self.last_database_status_state = state

    if show_dialog:
        if state == "ok":
            messagebox.showinfo("\u6570\u636e\u5e93\u8fde\u63a5\u68c0\u67e5", detail)
        elif state == "error":
            messagebox.showerror("\u6570\u636e\u5e93\u8fde\u63a5\u68c0\u67e5\u5931\u8d25", detail)
        elif state == "missing_env":
            messagebox.showwarning("\u6570\u636e\u5e93\u73af\u5883\u914d\u7f6e", detail)
        elif state == "missing_config":
            messagebox.showwarning("\u6570\u636e\u5e93\u8fde\u63a5\u914d\u7f6e", detail)
        elif state == "missing_sqlcmd":
            messagebox.showwarning("sqlcmd \u4e0d\u53ef\u7528", detail)
        elif state == "missing_runtime":
            messagebox.showwarning("\u6570\u636e\u5e93\u68c0\u67e5\u8fd0\u884c\u65f6", detail)
        else:
            messagebox.showwarning("\u6570\u636e\u5e93\u8fde\u63a5\u68c0\u67e5", detail)

    self.refresh_status_badges()
    if schedule_next:
        self.schedule_db_refresh()
    return state, detail


FF_CONFIG_SECTIONS: List[Tuple[str, List[Tuple[str, str, bool]]]] = [
    (
        "\u57fa\u7840\u914d\u7f6e",
        [
            ("APP_HOST", "\u76d1\u542c\u5730\u5740", False),
            ("APP_PORT", "\u76d1\u542c\u7aef\u53e3", False),
            ("ALLOWED_ORIGINS", "\u5141\u8bb8\u7684\u6765\u6e90", False),
            ("ALLOW_LAN_ORIGINS", "\u5141\u8bb8\u5c40\u57df\u7f51\u6765\u6e90", False),
        ],
    ),
    (
        "\u6570\u636e\u5e93\u914d\u7f6e",
        [
            ("DATABASE_URL", "\u8fde\u63a5\u5b57\u7b26\u4e32", False),
            ("DB_HOST", "\u4e3b\u673a", False),
            ("DB_PORT", "\u7aef\u53e3", False),
            ("DB_NAME", "\u6570\u636e\u5e93\u540d", False),
            ("DB_USER", "\u7528\u6237\u540d", False),
            ("DB_PASSWORD", "\u5bc6\u7801", True),
        ],
    ),
    (
        "\u5b89\u5168\u914d\u7f6e",
        [
            ("SECRET_KEY", "JWT \u5bc6\u94a5", True),
            ("ACCESS_TOKEN_EXPIRE_MINUTES", "Token \u8fc7\u671f\u65f6\u95f4(\u5206\u949f)", False),
        ],
    ),
    (
        "\u5916\u90e8\u7cfb\u7edf",
        [
            ("MARKI_USER", "Marki \u8d26\u53f7", False),
            ("MARKI_PASSWORD", "Marki \u5bc6\u7801", True),
            ("MARKI_SYSTEM_ID", "Marki \u7cfb\u7edf ID", False),
        ],
    ),
]

FF_LOG_CHOICES = [
    "\u670d\u52a1\u5bbf\u4e3b\u65e5\u5fd7",
    "\u540e\u7aef\u6807\u51c6\u8f93\u51fa",
    "\u540e\u7aef\u6807\u51c6\u9519\u8bef",
    "\u524d\u7aef\u6807\u51c6\u8f93\u51fa",
    "\u524d\u7aef\u6807\u51c6\u9519\u8bef",
    "\u9879\u76ee\u540c\u6b65\u65e5\u5fd7",
]


def _ff_build_ui(self: Any) -> None:
    # 配置 Notebook 样式，设置合适的 TAB 标签按钮尺寸并靠左排列
    style = ttk.Style()
    style.configure('TNotebook.Tab', padding=[12, 4], font=('Microsoft YaHei UI', 10))
    style.configure('TNotebook', tabposition='nw')
    style.configure('TNotebook.Tab', anchor='w')
    
    notebook = ttk.Notebook(self.root, style='TNotebook')
    notebook.pack(fill="both", expand=True, padx=10, pady=10)
    
    # 绑定 TAB 切换事件，优化切换体验
    def on_tab_changed(event):
        # 确保切换后立即刷新 UI
        self.root.update_idletasks()
    
    notebook.bind('<<NotebookTabChanged>>', on_tab_changed)

    status_frame = ttk.Frame(notebook)
    manager_frame = ttk.Frame(notebook)
    config_frame = ttk.Frame(notebook)
    ops_frame = ttk.Frame(notebook)
    env_frame = ttk.Frame(notebook)
    logs_frame = ttk.Frame(notebook)

    notebook.add(status_frame, text="\u670d\u52a1\u72b6\u6001")
    notebook.add(manager_frame, text="\u7ba1\u7406\u8bbe\u7f6e")
    notebook.add(config_frame, text="\u7cfb\u7edf\u914d\u7f6e")
    notebook.add(ops_frame, text="\u8fd0\u7ef4\u5de5\u5177")
    notebook.add(env_frame, text="\u73af\u5883\u68c0\u67e5")
    notebook.add(logs_frame, text="\u65e5\u5fd7\u67e5\u770b")

    self.build_status_tab(status_frame)
    self.build_manager_tab(manager_frame)
    self.build_config_tab(config_frame)
    self.build_ops_tab(ops_frame)
    self.build_env_tab(env_frame)
    self.build_logs_tab(logs_frame)


def _ff_build_manager_tab(self: Any, parent: ttk.Frame) -> None:
    parent.columnconfigure(0, weight=1)

    options = ttk.LabelFrame(parent, text="\u7ba1\u7406\u9009\u9879", padding=16)
    options.pack(fill="x", padx=10, pady=10)

    option_items = [
        ("auto_restart_backend", "\u540e\u7aef\u5f02\u5e38\u9000\u51fa\u540e\u81ea\u52a8\u91cd\u542f"),
        ("auto_restart_frontend", "\u524d\u7aef\u5f02\u5e38\u9000\u51fa\u540e\u81ea\u52a8\u91cd\u542f"),
        ("start_backend_on_launch", "\u542f\u52a8\u7ba1\u7406\u5668\u65f6\u81ea\u52a8\u542f\u52a8\u540e\u7aef"),
        ("start_frontend_on_launch", "\u542f\u52a8\u7ba1\u7406\u5668\u65f6\u81ea\u52a8\u542f\u52a8\u524d\u7aef"),
        ("hide_to_tray_on_close", "\u5173\u95ed\u7a97\u53e3\u65f6\u6700\u5c0f\u5316\u5230\u6258\u76d8"),
        ("launch_manager_on_startup", "Windows \u5f00\u673a\u81ea\u542f\u7ba1\u7406\u5668"),
        ("enable_health_check", "\u542f\u7528\u524d\u540e\u7aef\u5065\u5eb7\u68c0\u67e5"),
        ("enable_db_monitor", "\u542f\u7528\u6570\u636e\u5e93\u8fde\u63a5\u76d1\u63a7"),
    ]
    for idx, (key, label) in enumerate(option_items):
        var = tk.BooleanVar(value=self.manager_state.get(key, False))
        self.manager_option_vars[key] = var
        ttk.Checkbutton(options, text=label, variable=var, command=self.save_manager_state).grid(
            row=idx // 2, column=idx % 2, padx=10, pady=6, sticky="w"
        )

    tips = ttk.LabelFrame(parent, text="\u4f7f\u7528\u8bf4\u660e", padding=16)
    tips.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    ttk.Label(
        tips,
        text=(
            "\u7ba1\u7406\u5668\u4f1a\u7edf\u4e00\u63a5\u7ba1\u540e\u7aef\u3001\u524d\u7aef\u3001\u65e5\u5fd7\u3001Git \u66f4\u65b0\u548c\u6570\u636e\u5e93\u76d1\u63a7\u3002\n"
            "\u540e\u7aef\u542f\u52a8\u4f9d\u8d56 backend/.env \u4e0e backend/.encryption.key\uff0c\u524d\u7aef\u53d1\u5e03\u4f9d\u8d56 frontend/dist\u3002\n"
            "\u4e00\u952e\u90e8\u7f72\u4f1a\u5148\u5b8c\u6210\u914d\u7f6e\u5199\u5165\u3001\u4f9d\u8d56\u540c\u6b65\u4e0e\u6784\u5efa\uff0c\u7136\u540e\u518d\u6267\u884c\u540e\u7f6e\u68c0\u67e5\u3002\n"
            "\u82e5\u542f\u7528\u5065\u5eb7\u68c0\u67e5\u6216\u6570\u636e\u5e93\u76d1\u63a7\uff0c\u7ba1\u7406\u5668\u4f1a\u6301\u7eed\u66f4\u65b0\u72b6\u6001\u5e76\u5728\u5f02\u5e38\u65f6\u53d1\u9001\u63d0\u9192\u3002"
        ),
        justify="left",
        wraplength=880,
    ).pack(anchor="w")


def _ff_build_config_tab(self: Any, parent: ttk.Frame) -> None:
    toolbar = ttk.Frame(parent)
    toolbar.pack(fill="x", padx=10, pady=10)
    ttk.Button(toolbar, text="\u4fdd\u5b58\u914d\u7f6e", command=self.handle_save_config).pack(side="left", padx=4)
    ttk.Button(toolbar, text="\u91cd\u65b0\u52a0\u8f7d", command=self.reload_form_from_disk).pack(side="left", padx=4)
    ttk.Button(toolbar, text="\u751f\u6210 JWT \u5bc6\u94a5", command=self.generate_secret_key).pack(side="left", padx=4)
    ttk.Button(toolbar, text="\u751f\u6210\u52a0\u5bc6\u5bc6\u94a5", command=self.generate_encryption_key).pack(side="left", padx=4)
    ttk.Button(toolbar, text="\u6253\u5f00 backend \u76ee\u5f55", command=self.open_backend_folder).pack(side="left", padx=4)

    container = ttk.Frame(parent)
    container.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    nav_frame = tk.Frame(container, bg="#f0f0f0", bd=1, relief="flat")
    nav_frame.pack(side="left", fill="y", padx=(0, 10))

    nav_title = tk.Label(
        nav_frame,
        text="\u914d\u7f6e\u5206\u7ec4",
        bg="#f0f0f0",
        fg="#333333",
        font=("Microsoft YaHei UI", 10, "bold"),
        anchor="w",
    )
    nav_title.pack(fill="x", padx=8, pady=(8, 12))

    content_frame = ttk.Frame(container)
    content_frame.pack(side="left", fill="both", expand=True)

    self.config_section_var = tk.StringVar(value=FF_CONFIG_SECTIONS[0][0])
    self.config_section_frames = {}

    summary_group = ttk.LabelFrame(content_frame, text="\u914d\u7f6e\u8bf4\u660e", padding=16)
    summary_group.pack(fill="x", pady=(0, 10))
    ttk.Label(
        summary_group,
        text=(
            "\u672c\u9875\u7528\u4e8e\u7ef4\u62a4 backend/.env \u7684\u5173\u952e\u914d\u7f6e\u3002\n"
            "\u4fdd\u5b58\u540e\u7ba1\u7406\u5668\u4f1a\u5c06\u6700\u65b0\u503c\u5199\u56de\u78c1\u76d8\uff0c\u4f46\u5bc6\u94a5\u7c7b\u5b57\u6bb5\u4ecd\u5efa\u8bae\u914d\u5408\u751f\u6210\u6309\u94ae\u4f7f\u7528\u3002"
        ),
        justify="left",
    ).pack(anchor="w")

    self.config_content_host = ttk.Frame(content_frame)
    self.config_content_host.pack(fill="both", expand=True)

    descriptions = {
        "\u57fa\u7840\u914d\u7f6e": "\u540e\u7aef\u76d1\u542c\u5730\u5740\u3001\u7aef\u53e3\u548c CORS \u7b49\u57fa\u672c\u8fd0\u884c\u53c2\u6570",
        "\u6570\u636e\u5e93\u914d\u7f6e": "\u6570\u636e\u5e93\u4e3b\u673a\u3001\u7aef\u53e3\u3001\u8d26\u6237\u548c\u5bc6\u7801\u7b49\u8fde\u63a5\u4fe1\u606f",
        "\u5b89\u5168\u914d\u7f6e": "JWT \u5bc6\u94a5\u3001Token \u8fc7\u671f\u65f6\u95f4\u7b49\u5b89\u5168\u53c2\u6570",
        "\u5916\u90e8\u7cfb\u7edf": "Marki \u7b49\u5916\u90e8\u7cfb\u7edf\u5bf9\u63a5\u6240\u9700\u7684\u8d26\u6237\u53c2\u6570",
    }
    config_nav_items = [(title, title, descriptions.get(title, "")) for title, _fields in FF_CONFIG_SECTIONS]
    config_nav_items.append(("git_repo", "Git \u4ed3\u5e93\u914d\u7f6e", "\u914d\u7f6e Git \u66f4\u65b0\u6240\u9700\u7684\u4ed3\u5e93\u5730\u5740\u4e0e\u5206\u652f"))
    self.create_side_nav(nav_frame, config_nav_items, self.config_section_var, self.show_config_section, self.config_nav_items)

    for section_title, fields in FF_CONFIG_SECTIONS:
        section_frame = ttk.LabelFrame(self.config_content_host, text=section_title, padding=16)
        for row, (key, label, secret) in enumerate(fields):
            ttk.Label(section_frame, text=f"{label}\uff1a", width=18).grid(row=row, column=0, sticky="w", padx=6, pady=6)
            var = tk.StringVar(value=self.config_values.get(key, ""))
            ttk.Entry(section_frame, textvariable=var, width=88, show="*" if secret else "").grid(
                row=row, column=1, sticky="ew", padx=6, pady=6
            )
            section_frame.columnconfigure(1, weight=1)
            self.form_vars[key] = var
        self.config_section_frames[section_title] = section_frame

    git_section = ttk.LabelFrame(self.config_content_host, text="Git \u4ed3\u5e93\u914d\u7f6e", padding=16)
    ttk.Label(
        git_section,
        text="\u8fd9\u91cc\u7684 Git \u914d\u7f6e\u4f1a\u88ab\u8fd0\u7ef4\u5de5\u5177\u9875\u4e2d\u7684\u66f4\u65b0\u529f\u80fd\u76f4\u63a5\u4f7f\u7528\u3002",
        justify="left",
    ).grid(row=0, column=0, columnspan=3, sticky="w", padx=6, pady=(0, 8))
    ttk.Label(git_section, text="\u4ed3\u5e93\u5730\u5740\uff1a", width=18).grid(row=1, column=0, sticky="w", padx=6, pady=6)
    ttk.Entry(git_section, textvariable=self.ops_vars["git_repo_url"], width=88).grid(row=1, column=1, sticky="ew", padx=6, pady=6)
    ttk.Label(git_section, text="\u5206\u652f\u540d\u79f0\uff1a", width=18).grid(row=2, column=0, sticky="w", padx=6, pady=6)
    ttk.Entry(git_section, textvariable=self.ops_vars["git_branch"], width=30).grid(row=2, column=1, sticky="w", padx=6, pady=6)
    ttk.Button(git_section, text="\u4fdd\u5b58 Git \u914d\u7f6e", command=self.save_manager_state).grid(row=3, column=1, sticky="w", padx=6, pady=(8, 0))
    git_section.columnconfigure(1, weight=1)
    self.config_section_frames["git_repo"] = git_section

    self.show_config_section()


def _ff_build_ops_tab(self: Any, parent: ttk.Frame) -> None:
    container = ttk.Frame(parent)
    container.pack(fill="both", expand=True, padx=10, pady=10)

    nav_frame = tk.Frame(container, bg="#f0f0f0", bd=1, relief="flat")
    nav_frame.pack(side="left", fill="y", padx=(0, 10))

    nav_title = tk.Label(
        nav_frame,
        text="\u8fd0\u7ef4\u5de5\u5177",
        bg="#f0f0f0",
        fg="#333333",
        font=("Microsoft YaHei UI", 10, "bold"),
        anchor="w",
    )
    nav_title.pack(fill="x", padx=8, pady=(8, 12))

    content_frame = ttk.Frame(container)
    content_frame.pack(side="left", fill="both", expand=True)

    self.ops_section_var = tk.StringVar(value="one_click_deploy")
    ops_nav_items = [
        ("one_click_deploy", "\u4e00\u952e\u90e8\u7f72", "\u7edf\u4e00\u5b8c\u6210\u914d\u7f6e\u5199\u5165\u3001\u4f9d\u8d56\u540c\u6b65\u3001\u6784\u5efa\u4e0e\u540e\u7f6e\u68c0\u67e5"),
        ("frontend", "\u524d\u7aef\u90e8\u7f72", "\u5355\u72ec\u90e8\u7f72 frontend/dist \u5230\u670d\u52a1\u73af\u5883"),
        ("release", "\u53d1\u5e03\u5305\u90e8\u7f72", "\u4ece ZIP \u53d1\u5e03\u5305\u76f4\u63a5\u8986\u76d6\u9879\u76ee"),
        ("git_update", "Git \u66f4\u65b0", "\u68c0\u67e5\u5e76\u62c9\u53d6\u4ed3\u5e93\u6700\u65b0\u4ee3\u7801"),
        ("migration", "\u6570\u636e\u5e93\u8fc1\u79fb", "\u6267\u884c SQL \u8fc1\u79fb\u811a\u672c"),
        ("db_monitor", "\u6570\u636e\u5e93\u76d1\u63a7", "\u67e5\u770b\u914d\u7f6e\u3001\u8fde\u63a5\u548c\u76d1\u63a7\u72b6\u6001"),
        ("backup", "\u6570\u636e\u5e93\u5907\u4efd", "\u6267\u884c\u5907\u4efd\u3001\u6062\u590d\u4e0e\u5907\u4efd\u76ee\u5f55\u7ba1\u7406"),
        ("maintenance", "\u7ef4\u62a4\u6e05\u7406", "\u6e05\u7406\u65e7\u5907\u4efd\u3001\u5f52\u6863\u65e5\u5fd7\u548c\u540c\u6b65\u4f9d\u8d56"),
        ("alert", "\u544a\u8b66\u901a\u77e5", "\u914d\u7f6e Webhook \u5e76\u53d1\u9001\u6d4b\u8bd5\u544a\u8b66"),
        ("notes", "\u8fd0\u7ef4\u8bf4\u660e", "\u67e5\u770b\u90e8\u7f72\u548c\u7ef4\u62a4\u6ce8\u610f\u4e8b\u9879"),
    ]

    for key in ("frontend_deploy_source", "release_package_path", "backup_dir", "sqlcmd_path", "git_repo_url", "git_branch", "backup_retention_days", "backup_retention_count", "log_max_size_mb", "log_archive_retention_days", "webhook_url"):
        if key not in self.ops_vars:
            self.ops_vars[key] = tk.StringVar(value=str(self.manager_state.get(key, DEFAULT_STATE.get(key, ""))))

    self.create_side_nav(nav_frame, ops_nav_items, self.ops_section_var, self.show_ops_section, self.ops_nav_items)

    self.ops_content_host = ttk.Frame(content_frame)
    self.ops_content_host.pack(fill="both", expand=True)
    self.ops_section_frames = {}

    one_click_frame = ttk.LabelFrame(self.ops_content_host, text="\u4e00\u952e\u90e8\u7f72", padding=16)
    ttk.Label(
        one_click_frame,
        text="\u4e00\u952e\u90e8\u7f72\u4f1a\u4f9d\u6b21\u6267\u884c\u4ee3\u7801\u51c6\u5907\u3001\u914d\u7f6e\u5199\u5165\u3001\u4f9d\u8d56\u540c\u6b65\u3001\u524d\u7aef\u6784\u5efa\u4e0e\u540e\u7f6e\u68c0\u67e5\u3002\u6570\u636e\u5e93\u8fde\u63a5\u5c06\u5728\u90e8\u7f72\u540e\u5355\u72ec\u68c0\u67e5\u3002",
        justify="left",
        wraplength=760,
    ).pack(anchor="w", pady=(0, 12))
    deploy_type_frame = ttk.Frame(one_click_frame)
    deploy_type_frame.pack(fill="x", pady=(0, 10))
    self.deploy_mode = tk.StringVar(value="git")
    ttk.Radiobutton(deploy_type_frame, text="Git \u66f4\u65b0\u90e8\u7f72", variable=self.deploy_mode, value="git").pack(side="left", padx=20)
    ttk.Radiobutton(deploy_type_frame, text="ZIP \u53d1\u5e03\u5305\u90e8\u7f72", variable=self.deploy_mode, value="zip").pack(side="left", padx=20)
    ttk.Label(
        one_click_frame,
        text="\u90e8\u7f72\u5b8c\u6210\u540e\u4e0d\u4f1a\u81ea\u52a8\u542f\u52a8\u524d\u540e\u7aef\uff0c\u8bf7\u5728\u300c\u670d\u52a1\u72b6\u6001\u300d\u9875\u9762\u5355\u72ec\u542f\u52a8\u5e76\u9a8c\u8bc1\u8fd0\u884c\u72b6\u6001\u3002",
        justify="left",
        wraplength=760,
    ).pack(anchor="w", pady=(0, 10))
    deploy_log_frame = ttk.LabelFrame(one_click_frame, text="\u90e8\u7f72\u65e5\u5fd7", padding=10)
    deploy_log_frame.pack(fill="both", expand=True, pady=(0, 10))
    self.deploy_log_text = scrolledtext.ScrolledText(deploy_log_frame, wrap="word", font=("Consolas", 9), height=14)
    self.deploy_log_text.pack(fill="both", expand=True)
    self.deploy_log_text.configure(state="disabled")
    deploy_actions = ttk.Frame(one_click_frame)
    deploy_actions.pack(fill="x")
    ttk.Button(deploy_actions, text="\u5f00\u59cb\u4e00\u952e\u90e8\u7f72", command=self.start_one_click_deploy).pack(side="left", padx=4)
    ttk.Button(deploy_actions, text="\u6e05\u7a7a\u65e5\u5fd7", command=self.clear_deploy_log).pack(side="left", padx=4)
    self.ops_section_frames["one_click_deploy"] = one_click_frame

    frontend_frame = ttk.LabelFrame(self.ops_content_host, text="\u524d\u7aef\u90e8\u7f72", padding=16)
    frontend_top = ttk.Frame(frontend_frame)
    frontend_top.pack(fill="x", pady=(0, 10))
    self.ops_common_group = ttk.LabelFrame(frontend_top, text="\u516c\u5171\u8def\u5f84\u8bbe\u7f6e", padding=16)
    self.ops_common_group.pack(fill="x", pady=(0, 10))
    path_fields = [
        ("frontend_deploy_source", "\u524d\u7aef dist \u76ee\u5f55", "directory"),
        ("release_package_path", "ZIP \u53d1\u5e03\u5305", "zip"),
        ("backup_dir", "\u6570\u636e\u5e93\u5907\u4efd\u76ee\u5f55", "directory"),
        ("sqlcmd_path", "sqlcmd \u53ef\u6267\u884c\u6587\u4ef6", "file"),
    ]
    for row, (key, label, select_mode) in enumerate(path_fields):
        ttk.Label(self.ops_common_group, text=f"{label}\uff1a", width=18).grid(row=row, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(self.ops_common_group, textvariable=self.ops_vars[key], width=82).grid(row=row, column=1, sticky="ew", padx=6, pady=6)
        ttk.Button(self.ops_common_group, text="\u9009\u62e9", command=lambda target_key=key, mode=select_mode: self.select_path_for_var(target_key, mode)).grid(row=row, column=2, padx=6, pady=6)
    self.ops_common_group.columnconfigure(1, weight=1)
    ttk.Button(self.ops_common_group, text="\u4fdd\u5b58\u8fd0\u7ef4\u8def\u5f84", command=self.save_manager_state).grid(row=len(path_fields), column=1, sticky="w", padx=6, pady=(10, 0))
    ttk.Label(frontend_frame, text="\u5c06\u6307\u5b9a\u7684 frontend/dist \u8986\u76d6\u5230\u670d\u52a1\u73af\u5883\u7684 dist \u76ee\u5f55\uff0c\u7528\u4e8e\u5355\u72ec\u66f4\u65b0\u524d\u7aef\u9759\u6001\u8d44\u6e90\u3002", justify="left").pack(anchor="w", pady=(0, 8))
    frontend_actions = ttk.Frame(frontend_frame)
    frontend_actions.pack(fill="x")
    ttk.Button(frontend_actions, text="\u90e8\u7f72\u524d\u7aef dist", command=self.deploy_frontend_dist).pack(side="left", padx=4)
    ttk.Button(frontend_actions, text="\u6253\u5f00 dist \u76ee\u5f55", command=self.open_dist_folder).pack(side="left", padx=4)
    self.ops_section_frames["frontend"] = frontend_frame

    release_frame = ttk.LabelFrame(self.ops_content_host, text="\u53d1\u5e03\u5305\u90e8\u7f72", padding=16)
    ttk.Label(release_frame, text="\u9009\u62e9 ZIP \u53d1\u5e03\u5305\u540e\u53ef\u76f4\u63a5\u89e3\u538b\u8986\u76d6\u5230\u5f53\u524d\u9879\u76ee\u3002\u53d1\u5e03\u5305\u5185\u5efa\u8bae\u5305\u542b backend\u3001frontend/dist\u3001tools \u548c deploy \u7b49\u76ee\u5f55\u3002", justify="left", wraplength=760).pack(anchor="w", pady=(0, 8))
    release_actions = ttk.Frame(release_frame)
    release_actions.pack(fill="x")
    ttk.Button(release_actions, text="\u9009\u62e9\u53d1\u5e03\u5305", command=lambda: self.select_path_for_var("release_package_path", "zip")).pack(side="left", padx=4)
    ttk.Button(release_actions, text="\u5e94\u7528\u53d1\u5e03\u5305", command=self.apply_release_package).pack(side="left", padx=4)
    ttk.Button(release_actions, text="\u6253\u5f00\u9879\u76ee\u76ee\u5f55", command=self.open_project_root).pack(side="left", padx=4)
    self.ops_section_frames["release"] = release_frame

    db_monitor_frame = ttk.LabelFrame(self.ops_content_host, text="\u6570\u636e\u5e93\u76d1\u63a7", padding=16)
    ttk.Label(db_monitor_frame, text="\u6570\u636e\u5e93\u76d1\u63a7\u4f1a\u7ed3\u5408 backend/.env\u3001\u540e\u7aef\u8fd0\u884c\u65f6\u548c sqlcmd \u60c5\u51b5\u7ed9\u51fa\u8fde\u63a5\u72b6\u6001\u3002", justify="left", wraplength=760).pack(anchor="w", pady=(0, 8))
    db_monitor_grid = ttk.Frame(db_monitor_frame)
    db_monitor_grid.pack(fill="x", pady=(0, 10))
    for row, (label, key) in enumerate([
        ("\u914d\u7f6e\u72b6\u6001", "db_config_status"),
        ("\u8fde\u63a5\u72b6\u6001", "db_connection_status"),
        ("\u76d1\u63a7\u72b6\u6001", "db_monitor_status"),
        ("\u4e0a\u6b21\u68c0\u67e5", "db_last_check_at"),
    ]):
        ttk.Label(db_monitor_grid, text=f"{label}\uff1a", width=14).grid(row=row, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, textvariable=self.status_vars[key]).grid(row=row, column=1, sticky="w", padx=6, pady=4)
    db_monitor_actions = ttk.Frame(db_monitor_frame)
    db_monitor_actions.pack(fill="x")
    ttk.Button(db_monitor_actions, text="\u7acb\u5373\u68c0\u67e5\u6570\u636e\u5e93", command=self.check_database_connection).pack(side="left", padx=4)
    ttk.Checkbutton(db_monitor_actions, text="\u542f\u7528\u6570\u636e\u5e93\u76d1\u63a7", variable=self.manager_option_vars["enable_db_monitor"], command=self.save_manager_state).pack(side="left", padx=12)
    self.ops_section_frames["db_monitor"] = db_monitor_frame

    backup_frame = ttk.LabelFrame(self.ops_content_host, text="\u6570\u636e\u5e93\u5907\u4efd", padding=16)
    ttk.Label(backup_frame, text="\u5907\u4efd\u4e0e\u6062\u590d\u529f\u80fd\u4f9d\u8d56 sqlcmd\uff0c\u5e76\u4f1a\u4f7f\u7528 backend/.env \u4e2d\u7684\u6570\u636e\u5e93\u8fde\u63a5\u4fe1\u606f\u3002", justify="left").pack(anchor="w", pady=(0, 8))
    backup_actions = ttk.Frame(backup_frame)
    backup_actions.pack(fill="x")
    ttk.Button(backup_actions, text="\u6267\u884c\u6570\u636e\u5e93\u5907\u4efd", command=self.backup_database).pack(side="left", padx=4)
    ttk.Button(backup_actions, text="\u4ece\u5907\u4efd\u6062\u590d", command=self.restore_database).pack(side="left", padx=4)
    ttk.Button(backup_actions, text="\u6253\u5f00\u5907\u4efd\u76ee\u5f55", command=self.open_backup_folder).pack(side="left", padx=4)
    self.ops_section_frames["backup"] = backup_frame

    git_frame = ttk.LabelFrame(self.ops_content_host, text="Git \u66f4\u65b0", padding=16)
    ttk.Label(git_frame, text="\u53ef\u4ee5\u5148\u68c0\u67e5\u66f4\u65b0\uff0c\u518d\u6267\u884c Git \u62c9\u53d6\u3002\u5982\u542f\u7528\u81ea\u52a8\u6784\u5efa\uff0c\u62c9\u53d6\u540e\u4f1a\u81ea\u52a8\u540c\u6b65\u524d\u7aef\u4f9d\u8d56\u5e76\u6784\u5efa\u3002", justify="left").pack(anchor="w", pady=(0, 8))
    git_config = ttk.LabelFrame(git_frame, text="Git \u914d\u7f6e", padding=12)
    git_config.pack(fill="x", pady=(0, 10))
    ttk.Label(git_config, text="\u4ed3\u5e93\u5730\u5740\uff1a", width=12).grid(row=0, column=0, sticky="w", padx=6, pady=6)
    ttk.Label(git_config, textvariable=self.ops_vars["git_repo_url"]).grid(row=0, column=1, sticky="ew", padx=6, pady=6)
    ttk.Label(git_config, text="\u5206\u652f\u540d\u79f0\uff1a", width=12).grid(row=1, column=0, sticky="w", padx=6, pady=6)
    ttk.Label(git_config, textvariable=self.ops_vars["git_branch"]).grid(row=1, column=1, sticky="w", padx=6, pady=6)
    ttk.Label(git_config, text="\u5982\u672a\u914d\u7f6e Git \u4ed3\u5e93\uff0c\u8bf7\u5148\u5728\u7cfb\u7edf\u914d\u7f6e\u9875\u4fdd\u5b58 Git \u5730\u5740\u4e0e\u5206\u652f\u3002", foreground="#666666").grid(row=2, column=0, columnspan=2, sticky="w", padx=6, pady=(4, 0))
    git_config.columnconfigure(1, weight=1)
    if "git_auto_build_frontend" not in self.manager_option_vars:
        self.manager_option_vars["git_auto_build_frontend"] = tk.BooleanVar(value=self.manager_state.get("git_auto_build_frontend", True))
    ttk.Checkbutton(git_frame, text="Git \u62c9\u53d6\u66f4\u65b0\u540e\u81ea\u52a8\u540c\u6b65\u4f9d\u8d56\u5e76\u6784\u5efa\u524d\u7aef", variable=self.manager_option_vars["git_auto_build_frontend"], command=self.save_manager_state).pack(anchor="w", pady=(0, 10))
    git_actions = ttk.Frame(git_frame)
    git_actions.pack(fill="x")
    ttk.Button(git_actions, text="\u68c0\u67e5\u66f4\u65b0", command=self.check_git_update).pack(side="left", padx=4)
    ttk.Button(git_actions, text="\u62c9\u53d6\u66f4\u65b0", command=self.git_pull_update).pack(side="left", padx=4)
    ttk.Button(git_actions, text="\u63d0\u4ea4\u5386\u53f2", command=self.git_show_history).pack(side="left", padx=4)
    ttk.Button(git_actions, text="\u56de\u6eda\u7248\u672c", command=self.git_rollback).pack(side="left", padx=4)
    self.ops_section_frames["git_update"] = git_frame

    migration_frame = ttk.LabelFrame(self.ops_content_host, text="\u6570\u636e\u5e93\u8fc1\u79fb", padding=16)
    ttk.Label(migration_frame, text="\u53ef\u9009\u62e9 SQL \u811a\u672c\u6216\u5728\u4e0b\u65b9\u7f16\u8f91\u7a97\u53e3\u4e2d\u76f4\u63a5\u7c98\u8d34 SQL\uff0c\u6267\u884c\u65f6\u540c\u6837\u4f1a\u4f7f\u7528 sqlcmd \u8fde\u63a5\u6307\u5b9a\u6570\u636e\u5e93\u3002", justify="left").pack(anchor="w", pady=(0, 8))
    migration_config = ttk.Frame(migration_frame)
    migration_config.pack(fill="x", pady=(0, 8))
    ttk.Button(migration_config, text="\u9009\u62e9 SQL \u6587\u4ef6", command=self.select_migration_file).pack(side="left", padx=4)
    ttk.Button(migration_config, text="\u6e05\u7a7a\u811a\u672c", command=self.clear_migration_script).pack(side="left", padx=4)
    self.migration_script_text = scrolledtext.ScrolledText(migration_frame, wrap="word", font=("Consolas", 9), height=12)
    self.migration_script_text.pack(fill="both", expand=True, pady=(0, 8))
    migration_actions = ttk.Frame(migration_frame)
    migration_actions.pack(fill="x")
    ttk.Button(migration_actions, text="\u6267\u884c\u8fc1\u79fb", command=self.execute_migration).pack(side="left", padx=4)
    self.ops_section_frames["migration"] = migration_frame

    maintenance_frame = ttk.LabelFrame(self.ops_content_host, text="\u7ef4\u62a4\u6e05\u7406", padding=16)
    maint_grid = ttk.Frame(maintenance_frame)
    maint_grid.pack(fill="x", pady=(0, 10))
    maint_fields = [
        ("backup_retention_days", "\u5907\u4efd\u4fdd\u7559\u5929\u6570", "\u4ec5\u4fdd\u7559\u6700\u8fd1 N \u5929\u7684\u5907\u4efd"),
        ("backup_retention_count", "\u5907\u4efd\u4fdd\u7559\u6570\u91cf", "\u81f3\u5c11\u4fdd\u7559\u6700\u8fd1 N \u4e2a\u5907\u4efd"),
        ("log_max_size_mb", "\u5355\u4e2a\u65e5\u5fd7\u6700\u5927\u5927\u5c0f(MB)", "\u8d85\u8fc7\u9608\u503c\u540e\u4f1a\u6267\u884c\u65e5\u5fd7\u8f6e\u6362"),
        ("log_archive_retention_days", "\u5f52\u6863\u65e5\u5fd7\u4fdd\u7559\u5929\u6570", "\u81ea\u52a8\u6e05\u7406\u8d85\u8fc7 N \u5929\u7684\u5f52\u6863\u65e5\u5fd7"),
    ]
    for idx, (key, label, desc) in enumerate(maint_fields):
        ttk.Label(maint_grid, text=f"{label}\uff1a", width=18).grid(row=idx, column=0, sticky="w", padx=6, pady=4)
        ttk.Entry(maint_grid, textvariable=self.ops_vars[key], width=20).grid(row=idx, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(maint_grid, text=desc, foreground="#666666").grid(row=idx, column=2, sticky="w", padx=6, pady=4)
    maint_actions = ttk.Frame(maintenance_frame)
    maint_actions.pack(fill="x")
    ttk.Button(maint_actions, text="\u6e05\u7406\u65e7\u5907\u4efd", command=self.manual_cleanup_backups).pack(side="left", padx=4)
    ttk.Button(maint_actions, text="\u6e05\u7406\u5f52\u6863\u65e5\u5fd7", command=self.manual_cleanup_logs).pack(side="left", padx=4)
    ttk.Button(maint_actions, text="\u540c\u6b65\u540e\u7aef\u4f9d\u8d56", command=self.sync_backend_deps).pack(side="left", padx=4)
    ttk.Button(maint_actions, text="\u540c\u6b65\u524d\u7aef\u4f9d\u8d56", command=self.sync_frontend_deps).pack(side="left", padx=4)
    self.ops_section_frames["maintenance"] = maintenance_frame

    alert_frame = ttk.LabelFrame(self.ops_content_host, text="\u544a\u8b66\u901a\u77e5", padding=16)
    ttk.Label(alert_frame, text="\u914d\u7f6e Webhook URL \u540e\uff0c\u7ba1\u7406\u5668\u53ef\u5728\u670d\u52a1\u5f02\u5e38\u3001\u6062\u590d\u6216\u6570\u636e\u5e93\u72b6\u6001\u53d8\u5316\u65f6\u53d1\u9001\u901a\u77e5\u3002", justify="left").pack(anchor="w", pady=(0, 8))
    alert_grid = ttk.Frame(alert_frame)
    alert_grid.pack(fill="x", pady=(0, 8))
    ttk.Label(alert_grid, text="Webhook URL\uff1a", width=12).grid(row=0, column=0, sticky="w", padx=6, pady=6)
    ttk.Entry(alert_grid, textvariable=self.ops_vars["webhook_url"], width=80).grid(row=0, column=1, sticky="ew", padx=6, pady=6)
    alert_grid.columnconfigure(1, weight=1)
    ttk.Button(alert_frame, text="\u53d1\u9001\u6d4b\u8bd5\u544a\u8b66", command=self.send_test_alert).pack(side="left", padx=4)
    self.ops_section_frames["alert"] = alert_frame

    notes_frame = ttk.LabelFrame(self.ops_content_host, text="\u8fd0\u7ef4\u8bf4\u660e", padding=16)
    ttk.Label(
        notes_frame,
        text=(
            "1. \u524d\u7aef\u72ec\u7acb\u90e8\u7f72\u524d\uff0c\u8bf7\u786e\u4fdd frontend/dist \u5df2\u7531 npm run build \u751f\u6210\u3002\n"
            "2. ZIP \u53d1\u5e03\u5305\u5efa\u8bae\u5305\u542b backend\u3001frontend/dist\u3001tools \u548c deploy \u76ee\u5f55\u3002\n"
            "3. \u4e00\u952e\u90e8\u7f72\u5df2\u8c03\u6574\u4e3a\u90e8\u7f72\u540e\u518d\u68c0\u67e5\u6570\u636e\u5e93\uff0c\u4ee5\u907f\u514d\u521d\u59cb\u5316\u65f6 .env \u5c1a\u672a\u751f\u6210\u7684\u95ee\u9898\u3002\n"
            "4. \u6570\u636e\u5e93\u76d1\u63a7\u4f1a\u4f18\u5148\u4f7f\u7528\u540e\u7aef\u8fd0\u884c\u65f6\u6267\u884c\u8fde\u63a5\u68c0\u67e5\uff0csqlcmd \u53ea\u4f5c\u4e3a\u540e\u5907\u65b9\u6848\u3002\n"
            "5. Git \u66f4\u65b0\u529f\u80fd\u9700\u8981\u5148\u914d\u7f6e\u4ed3\u5e93\u5730\u5740\u4e0e\u5206\u652f\uff0c\u518d\u6267\u884c\u68c0\u67e5\u6216\u62c9\u53d6\u3002"
        ),
        justify="left",
    ).pack(anchor="w")
    self.ops_section_frames["notes"] = notes_frame

    self.show_ops_section()


def _ff_build_env_tab(self: Any, parent: ttk.Frame) -> None:
    toolbar = ttk.Frame(parent)
    toolbar.pack(fill="x", padx=10, pady=10)
    ttk.Button(toolbar, text="\u5237\u65b0\u73af\u5883\u68c0\u67e5", command=self.refresh_environment_info).pack(side="left", padx=4)
    ttk.Button(toolbar, text="\u6253\u5f00 backend \u76ee\u5f55", command=self.open_backend_folder).pack(side="left", padx=4)
    ttk.Button(toolbar, text="\u6253\u5f00\u65e5\u5fd7\u76ee\u5f55", command=self.open_logs_folder).pack(side="left", padx=4)

    container = ttk.Frame(parent)
    container.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    nav_frame = tk.Frame(container, bg="#f0f0f0", bd=1, relief="flat")
    nav_frame.pack(side="left", fill="y", padx=(0, 10))
    nav_title = tk.Label(nav_frame, text="\u68c0\u67e5\u9879", bg="#f0f0f0", fg="#333333", font=("Microsoft YaHei UI", 10, "bold"), anchor="w")
    nav_title.pack(fill="x", padx=8, pady=(8, 12))

    content_frame = ttk.Frame(container)
    content_frame.pack(side="left", fill="both", expand=True)

    summary = ttk.LabelFrame(content_frame, text="\u8bf4\u660e", padding=16)
    summary.pack(fill="x", pady=(0, 10))
    ttk.Label(summary, text="\u73af\u5883\u68c0\u67e5\u9875\u4f1a\u6c47\u603b\u5f53\u524d\u8fd0\u884c\u72b6\u6001\u3001Python \u8fd0\u884c\u65f6\u3001\u4f9d\u8d56\u5b89\u88c5\u60c5\u51b5\u4ee5\u53ca\u5173\u952e\u8def\u5f84\u662f\u5426\u5b58\u5728\u3002", justify="left").pack(anchor="w")

    self.env_section_var = tk.StringVar(value="overview")
    env_nav_items = [
        ("overview", "\u603b\u89c8", "\u67e5\u770b\u540e\u7aef\u3001\u524d\u7aef\u3001\u5065\u5eb7\u68c0\u67e5\u548c\u5173\u952e\u6587\u4ef6\u72b6\u6001"),
        ("runtime", "\u8fd0\u884c\u65f6", "\u67e5\u770b\u7ba1\u7406\u5668\u4e0e\u540e\u7aef Python \u8fd0\u884c\u65f6\u60c5\u51b5"),
        ("deps", "\u4f9d\u8d56", "\u68c0\u67e5 GUI \u4f9d\u8d56\u4e0e\u540e\u7aef\u4f9d\u8d56\u662f\u5426\u9f50\u5168"),
        ("paths", "\u8def\u5f84", "\u67e5\u770b .env\u3001dist\u3001\u65e5\u5fd7\u76ee\u5f55\u548c\u7aef\u53e3\u5360\u7528"),
    ]
    self.create_side_nav(nav_frame, env_nav_items, self.env_section_var, self.show_env_section, self.env_nav_items)

    self.env_content_host = ttk.Frame(content_frame)
    self.env_content_host.pack(fill="both", expand=True)
    section_titles = {"overview": "\u603b\u89c8", "runtime": "\u8fd0\u884c\u65f6", "deps": "\u4f9d\u8d56", "paths": "\u8def\u5f84"}
    for key, title in section_titles.items():
        frame = ttk.LabelFrame(self.env_content_host, text=title, padding=12)
        text_widget = scrolledtext.ScrolledText(frame, wrap="word", font=("Consolas", 10), height=24)
        text_widget.pack(fill="both", expand=True)
        text_widget.configure(state="disabled")
        self.env_text_widgets[key] = text_widget
        self.env_section_frames[key] = frame

    self.show_env_section()
    self.refresh_environment_info()


def _ff_build_logs_tab(self: Any, parent: ttk.Frame) -> None:
    if self.log_choice.get() not in FF_LOG_CHOICES:
        self.log_choice.set(FF_LOG_CHOICES[0])
    toolbar = ttk.Frame(parent)
    toolbar.pack(fill="x", padx=10, pady=10)
    ttk.Label(toolbar, text="\u65e5\u5fd7\u7c7b\u578b\uff1a").pack(side="left")
    ttk.Combobox(toolbar, state="readonly", values=FF_LOG_CHOICES, textvariable=self.log_choice, width=20).pack(side="left", padx=6)
    ttk.Button(toolbar, text="\u5237\u65b0", command=lambda: self.refresh_log_view(force=True)).pack(side="left", padx=6)
    ttk.Checkbutton(toolbar, text="\u81ea\u52a8\u5237\u65b0", variable=self.log_auto_refresh).pack(side="left", padx=6)
    ttk.Button(toolbar, text="\u6e05\u7a7a\u5f53\u524d\u65e5\u5fd7", command=self.clear_current_log).pack(side="left", padx=6)
    ttk.Button(toolbar, text="\u6253\u5f00\u5f52\u6863\u76ee\u5f55", command=self.open_log_archive_folder).pack(side="left", padx=6)
    status_bar = ttk.Frame(parent)
    status_bar.pack(fill="x", padx=10, pady=(0, 8))
    ttk.Label(status_bar, textvariable=self.log_status_var, foreground="#4b5f73").pack(side="left")
    self.log_text = scrolledtext.ScrolledText(parent, wrap="none", font=("Consolas", 10))
    self.log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
    self.log_text.configure(state="disabled")


def _ff_refresh_environment_info(self: Any) -> None:
    config = self.get_effective_config()
    layout_ok, layout_issues = evaluate_manager_runtime_layout()
    host = (config.get("APP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    browser_host = resolve_browser_host(host)
    port = (config.get("APP_PORT") or "8100").strip() or "8100"
    backend_python = get_backend_python_executable()
    backend_venv_dir = get_backend_venv_dir()
    owner = get_port_owner_info(int(port)) if port.isdigit() else {}

    manager_modules = ["tkinter", "pystray", "PIL", "cryptography"]
    manager_module_lines = []
    for name in manager_modules:
        try:
            __import__(name)
            manager_module_lines.append(f"[OK] {name}")
        except Exception as exc:
            manager_module_lines.append(f"[MISSING] {name}: {exc}")

    backend_probe = {"ok": False, "error": f"\u672a\u627e\u5230\u540e\u7aef\u865a\u62df\u73af\u5883 Python: {get_backend_python_executable()}"}
    if backend_python.exists():
        probe_code = (
            "import importlib.util, json, sys;"
            "mods=['fastapi','uvicorn','sqlalchemy','psycopg2','cryptography','Crypto'];"
            "result={m:(importlib.util.find_spec(m) is not None) for m in mods};"
            "print(json.dumps({'ok': True, 'python': sys.executable, 'version': sys.version, 'modules': result}, ensure_ascii=False))"
        )
        backend_probe = run_python_json(backend_python, probe_code)

    overview_text = "\n".join(
        [
            "FinFlow \u73af\u5883\u603b\u89c8",
            "",
            f"\u9879\u76ee\u76ee\u5f55: {ROOT_DIR}",
            f"\u6253\u5305 EXE \u5e03\u5c40: {'\u6b63\u5e38' if layout_ok else '\u7f3a\u5931\u5173\u952e\u76ee\u5f55'}",
            f"\u5e94\u7528\u5165\u53e3: {self.get_app_url()}",
            f"\u540e\u7aef\u72b6\u6001: {self.backend.poll_status()}",
            f"\u540e\u7aef\u7aef\u53e3\u5360\u7528: {self.status_vars.get('backend_port_owner').get() if self.status_vars.get('backend_port_owner') else '\u672a\u77e5'}",
            f"\u524d\u7aef\u72b6\u6001: {self.frontend.poll_status()}",
            f"\u524d\u7aef\u7aef\u53e3\u5360\u7528: {self.status_vars.get('frontend_port_owner').get() if self.status_vars.get('frontend_port_owner') else '\u672a\u77e5'}",
            f"backend/.env: {'\u5df2\u5b58\u5728' if ENV_PATH.exists() else '\u4e0d\u5b58\u5728'}",
            f"backend/.encryption.key: {'\u5df2\u5b58\u5728' if KEY_PATH.exists() else '\u4e0d\u5b58\u5728'}",
            f"frontend/dist/index.html: {'\u5df2\u5b58\u5728' if (DIST_DIR / 'index.html').exists() else '\u4e0d\u5b58\u5728'}",
            f"\u65e5\u5fd7\u76ee\u5f55: {'\u5df2\u5b58\u5728' if LOG_DIR.exists() else '\u4e0d\u5b58\u5728'}",
            f"\u540e\u7aef\u5065\u5eb7\u72b6\u6001: {self.status_vars.get('backend_health_status').get() if self.status_vars.get('backend_health_status') else '\u672a\u68c0\u67e5'}",
            f"\u524d\u7aef\u5065\u5eb7\u72b6\u6001: {self.status_vars.get('frontend_health_status').get() if self.status_vars.get('frontend_health_status') else '\u672a\u68c0\u67e5'}",
        ]
    )
    if not layout_ok:
        overview_text += "\n\nEXE 运行布局问题:\n" + "\n".join(f"- {item}" for item in layout_issues)

    runtime_lines = [
        "\u7ba1\u7406\u5668\u8fd0\u884c\u65f6",
        "",
        f"\u7ba1\u7406\u5668 Python: {sys.executable}",
        f"\u7ba1\u7406\u5668\u7248\u672c: {sys.version}",
        f"\u64cd\u4f5c\u7cfb\u7edf: {platform.platform()}",
        f"TCL_LIBRARY: {os.environ.get('TCL_LIBRARY', '(\u672a\u8bbe\u7f6e)')}",
        f"TK_LIBRARY: {os.environ.get('TK_LIBRARY', '(\u672a\u8bbe\u7f6e)')}",
        "",
        "\u540e\u7aef\u8fd0\u884c\u65f6",
        "",
    ]
    if backend_probe.get("ok"):
        runtime_lines.extend(
            [
                f"\u540e\u7aef Python: {backend_probe.get('python', '')}",
                f"\u540e\u7aef\u7248\u672c: {backend_probe.get('version', '')}",
            ]
        )
    else:
        runtime_lines.append(f"\u540e\u7aef\u8fd0\u884c\u65f6\u5f02\u5e38: {backend_probe.get('error', '\u672a\u77e5\u9519\u8bef')}")
    runtime_text = "\n".join(runtime_lines)

    deps_lines = ["\u7ba1\u7406\u5668\u4f9d\u8d56", "", *manager_module_lines, "", "\u540e\u7aef\u4f9d\u8d56", ""]
    if backend_probe.get("ok"):
        for module_name, ok in backend_probe.get("modules", {}).items():
            deps_lines.append(f"[{'OK' if ok else 'MISSING'}] {module_name}")
    else:
        deps_lines.append(f"\u65e0\u6cd5\u68c0\u67e5\u540e\u7aef\u4f9d\u8d56: {backend_probe.get('error', '\u672a\u77e5\u9519\u8bef')}")
    deps_lines.append("")
    deps_lines.append(f"\u8bf4\u660e: \u82e5 `Crypto` \u7f3a\u5931\uff0c\u8bf7\u5728 {backend_venv_dir} \u4e2d\u5b89\u88c5 `pycryptodome`\u3002")
    deps_text = "\n".join(deps_lines)

    path_lines = [
        "\u5173\u952e\u8def\u5f84\u4e0e\u7aef\u53e3",
        "",
        f"\u540e\u7aef\u865a\u62df\u73af\u5883: {backend_venv_dir} ({'\u5df2\u5b58\u5728' if backend_python.exists() else '\u4e0d\u5b58\u5728'})",
        f"backend/.env: {'\u5df2\u5b58\u5728' if ENV_PATH.exists() else '\u4e0d\u5b58\u5728'}",
        f"backend/.encryption.key: {'\u5df2\u5b58\u5728' if KEY_PATH.exists() else '\u4e0d\u5b58\u5728'}",
        f"frontend/dist/index.html: {'\u5df2\u5b58\u5728' if (DIST_DIR / 'index.html').exists() else '\u4e0d\u5b58\u5728'}",
        f"\u65e5\u5fd7\u76ee\u5f55: {'\u5df2\u5b58\u5728' if LOG_DIR.exists() else '\u4e0d\u5b58\u5728'}",
        f"\u5f52\u6863\u76ee\u5f55: {'\u5df2\u5b58\u5728' if LOG_ARCHIVE_DIR.exists() else '\u4e0d\u5b58\u5728'}",
        "",
        f"\u76d1\u542c\u5730\u5740: {host}",
        f"\u6d4f\u89c8\u5668\u53ef\u8bbf\u95ee\u5730\u5740: {browser_host}",
        f"\u76d1\u542c\u7aef\u53e3: {port}",
        f"\u7aef\u53e3\u5360\u7528: {'\u662f' if (port.isdigit() and is_port_open(browser_host, int(port))) else '\u5426'}",
    ]
    if owner:
        path_lines.extend([f"\u5360\u7528 PID: {owner.get('pid', '')}", f"\u5360\u7528\u8bf4\u660e: {build_port_owner_label(owner)}"])
    else:
        path_lines.append("\u5360\u7528\u8bf4\u660e: \u672a\u68c0\u6d4b\u5230")
    paths_text = "\n".join(path_lines)

    self.set_readonly_text(self.env_text_widgets["overview"], overview_text)
    self.set_readonly_text(self.env_text_widgets["runtime"], runtime_text)
    self.set_readonly_text(self.env_text_widgets["deps"], deps_text)
    self.set_readonly_text(self.env_text_widgets["paths"], paths_text)


def _ff_create_tray_icon(self: Any) -> None:
    menu = Menu(
        MenuItem("\u6253\u5f00\u7ba1\u7406\u5668", lambda: self.root.after(0, self.show_window)),
        MenuItem("\u542f\u52a8\u5168\u90e8", lambda: self.root.after(0, self.handle_start_all)),
        MenuItem("\u505c\u6b62\u5168\u90e8", lambda: self.root.after(0, self.handle_stop_all)),
        MenuItem("\u91cd\u542f\u5168\u90e8", lambda: self.root.after(0, self.handle_restart_all)),
        MenuItem("\u542f\u52a8\u540e\u7aef", lambda: self.root.after(0, self.handle_start_backend)),
        MenuItem("\u505c\u6b62\u540e\u7aef", lambda: self.root.after(0, self.handle_stop_backend)),
        MenuItem("\u91cd\u542f\u540e\u7aef", lambda: self.root.after(0, self.handle_restart_backend)),
        MenuItem("\u542f\u52a8\u524d\u7aef", lambda: self.root.after(0, self.handle_start_frontend)),
        MenuItem("\u505c\u6b62\u524d\u7aef", lambda: self.root.after(0, self.handle_stop_frontend)),
        MenuItem("\u91cd\u542f\u524d\u7aef", lambda: self.root.after(0, self.handle_restart_frontend)),
        MenuItem("\u6253\u5f00\u524d\u7aef", lambda: self.root.after(0, self.open_frontend)),
        MenuItem("\u9000\u51fa\u7a0b\u5e8f", lambda: self.root.after(0, self.exit_application)),
    )
    self.tray_icon = Icon("FinFlowManager", create_tray_image(), "FinFlow \u670d\u52a1\u7ba1\u7406\u5668", menu)
    self.tray_thread = threading.Thread(target=self.tray_icon.run, daemon=True)
    self.tray_thread.start()
    self.update_tray_title()


def _ff_resolve_log_path(self: Any) -> Path:
    mapping = {
        FF_LOG_CHOICES[0]: SERVICE_HOST_LOG,
        FF_LOG_CHOICES[1]: STDOUT_LOG,
        FF_LOG_CHOICES[2]: STDERR_LOG,
        FF_LOG_CHOICES[3]: FRONTEND_STDOUT_LOG,
        FF_LOG_CHOICES[4]: FRONTEND_STDERR_LOG,
        FF_LOG_CHOICES[5]: PROJECT_SYNC_LOG,
    }
    return mapping.get(self.log_choice.get(), SERVICE_HOST_LOG)


def _ff_clear_current_log(self: Any) -> None:
    path = self.resolve_log_path()
    if path == PROJECT_SYNC_LOG:
        confirm = messagebox.askyesno("\u786e\u8ba4\u6e05\u7a7a", "\u786e\u5b9a\u8981\u6e05\u7a7a\u9879\u76ee\u540c\u6b65\u65e5\u5fd7\u5417\uff1f")
    else:
        confirm = messagebox.askyesno("\u786e\u8ba4\u6e05\u7a7a", f"\u786e\u5b9a\u8981\u6e05\u7a7a\u4ee5\u4e0b\u65e5\u5fd7\u5417\uff1f\n{path}")
    if not confirm:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    except Exception as exc:
        messagebox.showerror("\u6e05\u7a7a\u5931\u8d25", str(exc))
        return
    self.log_status_var.set("\u5f53\u524d\u65e5\u5fd7\u5df2\u6e05\u7a7a")
    self.refresh_log_view(force=True)


def _ff_summarize_log_view(self: Any, path: Path, lines: List[str]) -> tuple[str, str]:
    if path == PROJECT_SYNC_LOG:
        return "\u6b63\u5728\u67e5\u770b\u9879\u76ee\u540c\u6b65\u65e5\u5fd7", "\n".join(lines[-300:])
    if path in (FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG):
        marker = self.frontend.last_session_marker
        started_label = self.frontend.last_session_started_label
    else:
        marker = self.backend.last_session_marker
        started_label = self.backend.last_session_started_label
    if marker and marker in lines:
        marker_index = len(lines) - 1 - lines[::-1].index(marker)
        session_lines = lines[marker_index:]
        label = f"\u6b63\u5728\u67e5\u770b\u672c\u6b21\u4f1a\u8bdd\u65e5\u5fd7\uff08{started_label}\uff09"
        return label, "\n".join(session_lines[-300:])
    return "\u6b63\u5728\u67e5\u770b\u6700\u8fd1 300 \u884c\u65e5\u5fd7", "\n".join(lines[-300:])


def _ff_refresh_log_view(self: Any, force: bool = False) -> None:
    if self.exiting:
        return
    if not force and not self.log_auto_refresh.get():
        self.schedule_log_refresh()
        return
    path = self.resolve_log_path()
    content = ""
    if path in {STDOUT_LOG, STDERR_LOG, FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG, SERVICE_HOST_LOG} and not path.exists():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch(exist_ok=True)
        except Exception:
            pass
    if path.exists():
        try:
            raw_bytes = path.read_bytes()
            lines = decode_console_output(raw_bytes).splitlines()
            status_text, content = self.summarize_log_view(path, lines)
            self.log_status_var.set(status_text)
        except Exception as exc:
            content = f"\u8bfb\u53d6\u65e5\u5fd7\u5931\u8d25: {exc}"
            self.log_status_var.set("\u65e5\u5fd7\u8bfb\u53d6\u5931\u8d25")
    else:
        content = f"\u65e5\u5fd7\u6587\u4ef6\u4e0d\u5b58\u5728: {path}"
        self.log_status_var.set("\u65e5\u5fd7\u6587\u4ef6\u4e0d\u5b58\u5728")
    self.log_text.configure(state="normal")
    self.log_text.delete("1.0", tk.END)
    self.log_text.insert("1.0", content)
    self.log_text.configure(state="disabled")
    self.log_text.see(tk.END)
    self.schedule_log_refresh()


def _ff_check_git_update(self: Any) -> None:
    self.save_manager_state()
    if not self.check_git_available():
        messagebox.showerror("\u9519\u8bef", "\u672a\u627e\u5230 Git \u547d\u4ee4\uff0c\u8bf7\u5148\u5b89\u88c5 Git \u5e76\u52a0\u5165\u7cfb\u7edf PATH")
        return

    repo_url = self.ops_vars["git_repo_url"].get().strip()
    if not repo_url:
        messagebox.showerror("\u9519\u8bef", "\u8bf7\u5148\u914d\u7f6e Git \u4ed3\u5e93\u5730\u5740")
        return

    branch = self.ops_vars["git_branch"].get().strip() or "main"
    try:
        result = subprocess.run(
            ["git", "ls-remote", "--heads", repo_url, branch],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        messagebox.showerror("\u68c0\u67e5\u5931\u8d25", f"\u68c0\u67e5 Git \u66f4\u65b0\u65f6\u51fa\u9519\uff1a{exc}")
        return

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "\u672a\u77e5\u9519\u8bef").strip()
        messagebox.showerror("\u68c0\u67e5\u5931\u8d25", f"\u65e0\u6cd5\u8bbf\u95ee\u4ed3\u5e93\u6216\u5206\u652f\u4e0d\u5b58\u5728\uff1a\n{detail}")
        return
    if not result.stdout.strip():
        messagebox.showinfo("\u68c0\u67e5\u7ed3\u679c", f"\u5206\u652f '{branch}' \u4e0d\u5b58\u5728\u4e8e\u4ed3\u5e93\u4e2d")
        return
    messagebox.showinfo("\u68c0\u67e5\u7ed3\u679c", f"\u5206\u652f '{branch}' \u53ef\u8bbf\u95ee\uff0c\u53ef\u4ee5\u6267\u884c\u62c9\u53d6\u66f4\u65b0")


def _ff_git_pull_update(self: Any) -> None:
    self.save_manager_state()
    if not self.check_git_available():
        messagebox.showerror("\u9519\u8bef", "\u672a\u68c0\u6d4b\u5230 Git\uff0c\u8bf7\u5148\u5b89\u88c5 Git \u5e76\u52a0\u5165\u7cfb\u7edf PATH")
        return

    repo_url = self.ops_vars["git_repo_url"].get().strip()
    if not repo_url:
        messagebox.showerror("\u9519\u8bef", "\u8bf7\u5148\u914d\u7f6e Git \u4ed3\u5e93\u5730\u5740")
        return

    branch = self.ops_vars["git_branch"].get().strip() or "main"
    backend_was_running = self.backend.is_running()
    frontend_was_running = self.frontend.is_running()

    if not (ROOT_DIR / ".git").exists():
        proceed = messagebox.askyesno(
            "\u521d\u59cb\u5316 Git \u4ed3\u5e93",
            "\u5f53\u524d\u76ee\u5f55\u5c1a\u672a\u521d\u59cb\u5316\u4e3a Git \u4ed3\u5e93\uff0c\u5c06\u6267\u884c clone\u3002\u662f\u5426\u7ee7\u7eed\uff1f",
        )
        if not proceed:
            return
        try:
            clone_result = subprocess.run(
                ["git", "clone", "--branch", branch, repo_url, str(ROOT_DIR)],
                capture_output=True,
                text=True,
                timeout=300,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception as exc:
            messagebox.showerror("\u5931\u8d25", f"Git clone \u5931\u8d25\uff1a{exc}")
            return
        if clone_result.returncode != 0:
            detail = (clone_result.stderr or clone_result.stdout or "\u672a\u77e5\u9519\u8bef").strip()
            messagebox.showerror("\u5931\u8d25", f"Git clone \u5931\u8d25\uff1a\n{detail}")
            return
        post_ok, post_detail = self.run_git_post_update_tasks()
        if not post_ok:
            messagebox.showerror("\u5931\u8d25", f"Git \u4ed3\u5e93\u521d\u59cb\u5316\u5b8c\u6210\uff0c\u4f46\u540e\u7eed\u5904\u7406\u5931\u8d25\uff1a\n{post_detail}")
            return
        detail = "Git \u4ed3\u5e93\u521d\u59cb\u5316\u5b8c\u6210\uff0c\u4ee3\u7801\u5df2\u62c9\u53d6"
        if self.manager_state.get("git_auto_build_frontend", True):
            detail += f"\n\n{post_detail}"
        messagebox.showinfo("\u6210\u529f", detail)
        self.refresh_status()
        self.notify_tray("Git \u66f4\u65b0\u5b8c\u6210", "\u9879\u76ee\u4ee3\u7801\u5df2\u521d\u59cb\u5316\u5e76\u62c9\u53d6")
        return

    try:
        if frontend_was_running:
            self.frontend.stop()
        if backend_was_running:
            self.backend.stop()

        result = subprocess.run(
            ["git", "pull", "origin", branch],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "\u672a\u77e5\u9519\u8bef").strip()
            messagebox.showerror("\u62c9\u53d6\u5931\u8d25", f"Git pull \u5931\u8d25\uff1a\n{detail}")
            return

        stdout_text = result.stdout.strip()
        if "Already up to date" in stdout_text:
            messagebox.showinfo("\u63d0\u793a", "\u9879\u76ee\u5df2\u7ecf\u662f\u6700\u65b0\u7248\u672c")
            return

        post_ok, post_detail = self.run_git_post_update_tasks()
        if not post_ok:
            messagebox.showerror("\u62c9\u53d6\u540e\u5904\u7406\u5931\u8d25", f"Git pull \u5df2\u5b8c\u6210\uff0c\u4f46\u540e\u7eed\u5904\u7406\u5931\u8d25\uff1a\n{post_detail}")
            return

        detail = f"\u9879\u76ee\u5df2\u66f4\u65b0\n{stdout_text}"
        if self.manager_state.get("git_auto_build_frontend", True):
            detail += f"\n\n{post_detail}"
        messagebox.showinfo("\u6210\u529f", detail)
        self.refresh_status()
        self.notify_tray("Git \u66f4\u65b0\u5b8c\u6210", "\u9879\u76ee\u5df2\u4ece\u8fdc\u7a0b\u4ed3\u5e93\u62c9\u53d6")
    except Exception as exc:
        messagebox.showerror("\u5931\u8d25", f"Git \u66f4\u65b0\u5f02\u5e38\uff1a{exc}")
    finally:
        if backend_was_running and not self.backend.is_running():
            self.backend.start(self.get_effective_config())
        if frontend_was_running and not self.frontend.is_running():
            self.frontend.start(self.get_effective_config())


def _ff_git_show_history(self: Any) -> None:
    self.save_manager_state()
    if not self.check_git_available():
        messagebox.showerror("\u9519\u8bef", "\u672a\u627e\u5230 Git \u547d\u4ee4\uff0c\u8bf7\u5148\u5b89\u88c5 Git \u5e76\u52a0\u5165\u7cfb\u7edf PATH")
        return
    if not (ROOT_DIR / ".git").exists():
        messagebox.showerror("\u9519\u8bef", "\u5f53\u524d\u9879\u76ee\u76ee\u5f55\u6ca1\u6709 Git \u4ed3\u5e93")
        return

    try:
        result = subprocess.run(
            ["git", "log", "-5", "--oneline"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0 or not result.stdout.strip():
            messagebox.showinfo("\u63d0\u793a", "\u672c\u5730\u4ed3\u5e93\u6682\u65e0\u63d0\u4ea4\u5386\u53f2")
            return

        top_level = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        repo_dir = top_level.stdout.strip() if top_level.returncode == 0 else str(ROOT_DIR)

        history_window = tk.Toplevel(self.root)
        history_window.title("Git \u63d0\u4ea4\u5386\u53f2")
        history_window.geometry("600x400")

        text_widget = scrolledtext.ScrolledText(history_window, wrap="word", font=("Consolas", 10))
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)
        text_widget.insert("1.0", f"\u4ed3\u5e93\u76ee\u5f55\uff1a{repo_dir}\n\n{result.stdout.strip()}")
        text_widget.configure(state="disabled")
        ttk.Button(history_window, text="\u5173\u95ed", command=history_window.destroy).pack(pady=10)
    except Exception as exc:
        messagebox.showerror("\u9519\u8bef", f"\u67e5\u770b Git \u63d0\u4ea4\u5386\u53f2\u5931\u8d25\uff1a{exc}")


def _ff_git_rollback(self: Any) -> None:
    self.save_manager_state()
    if not self.check_git_available():
        messagebox.showerror("\u9519\u8bef", "\u672a\u627e\u5230 Git \u547d\u4ee4\uff0c\u8bf7\u5148\u5b89\u88c5 Git \u5e76\u52a0\u5165\u7cfb\u7edf PATH")
        return
    if not (ROOT_DIR / ".git").exists():
        messagebox.showerror("\u9519\u8bef", "\u5f53\u524d\u9879\u76ee\u76ee\u5f55\u6ca1\u6709 Git \u4ed3\u5e93")
        return

    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "-20"],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=30,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0 or not result.stdout.strip():
            messagebox.showinfo("\u63d0\u793a", "\u672c\u5730\u4ed3\u5e93\u6682\u65e0\u63d0\u4ea4\u5386\u53f2")
            return

        commits = []
        for line in result.stdout.strip().splitlines():
            if " " not in line:
                continue
            commit_hash, message = line.split(" ", 1)
            commits.append((commit_hash, message))
        if not commits:
            messagebox.showinfo("\u63d0\u793a", "\u672c\u5730\u4ed3\u5e93\u6682\u65e0\u63d0\u4ea4\u5386\u53f2")
            return

        rollback_window = tk.Toplevel(self.root)
        rollback_window.title("\u9009\u62e9\u56de\u6eda\u7248\u672c")
        rollback_window.geometry("520x400")
        ttk.Label(rollback_window, text="\u9009\u62e9\u8981\u56de\u6eda\u5230\u7684\u7248\u672c\uff1a").pack(pady=10)
        listbox = tk.Listbox(rollback_window, height=15, font=("Consolas", 10))
        listbox.pack(fill="both", expand=True, padx=10, pady=10)
        for commit_hash, message in commits:
            listbox.insert("end", f"{commit_hash} {message}")
        ttk.Button(
            rollback_window,
            text="\u786e\u5b9a\u56de\u6eda",
            command=lambda: self.execute_rollback(listbox, rollback_window),
        ).pack(pady=10)
    except Exception as exc:
        messagebox.showerror("\u9519\u8bef", f"\u83b7\u53d6 Git \u63d0\u4ea4\u5386\u53f2\u5931\u8d25\uff1a{exc}")


def _ff_execute_rollback(self: Any, listbox: tk.Listbox, window: tk.Toplevel) -> None:
    selection = listbox.curselection()
    if not selection:
        messagebox.showwarning("\u8b66\u544a", "\u8bf7\u5148\u9009\u62e9\u4e00\u4e2a\u7248\u672c")
        return

    selected = listbox.get(selection[0])
    commit_hash = selected.split(" ", 1)[0]
    proceed = messagebox.askyesno(
        "\u786e\u8ba4\u56de\u6eda",
        f"\u786e\u5b9a\u8981\u56de\u6eda\u5230\u7248\u672c {commit_hash} \u5417\uff1f\n\n\u6ce8\u610f\uff1a\u8fd9\u4f1a\u8986\u76d6\u672c\u5730\u7684\u4ee3\u7801\u53d8\u66f4\u3002",
    )
    if not proceed:
        return

    backend_was_running = self.backend.is_running()
    frontend_was_running = self.frontend.is_running()
    try:
        if frontend_was_running:
            self.frontend.stop()
        if backend_was_running:
            self.backend.stop()
        result = subprocess.run(
            ["git", "reset", "--hard", commit_hash],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "\u672a\u77e5\u9519\u8bef").strip()
            messagebox.showerror("\u56de\u6eda\u5931\u8d25", f"Git reset \u5931\u8d25\uff1a\n{detail}")
            return
        messagebox.showinfo("\u6210\u529f", f"\u5df2\u56de\u6eda\u5230\u7248\u672c {commit_hash}")
        self.refresh_status()
        self.notify_tray("Git \u56de\u6eda\u5b8c\u6210", f"\u5df2\u56de\u6eda\u5230\u7248\u672c {commit_hash}")
    except Exception as exc:
        messagebox.showerror("\u5931\u8d25", f"Git \u56de\u6eda\u65f6\u51fa\u9519\uff1a{exc}")
    finally:
        if backend_was_running and not self.backend.is_running():
            self.backend.start(self.get_effective_config())
        if frontend_was_running and not self.frontend.is_running():
            self.frontend.start(self.get_effective_config())
        window.destroy()


def _ff_select_migration_file(self: Any) -> None:
    path = filedialog.askopenfilename(
        title="\u9009\u62e9 SQL \u811a\u672c",
        filetypes=[("SQL \u6587\u4ef6", "*.sql"), ("\u6240\u6709\u6587\u4ef6", "*.*")],
    )
    if not path:
        return

    last_error: Exception | None = None
    for encoding in ("utf-8", "utf-8-sig", "gbk"):
        try:
            content = Path(path).read_text(encoding=encoding)
            self.migration_script_text.configure(state="normal")
            self.migration_script_text.delete("1.0", tk.END)
            self.migration_script_text.insert("1.0", content)
            self.migration_script_text.configure(state="disabled")
            return
        except Exception as exc:
            last_error = exc
    messagebox.showerror("\u8bfb\u53d6\u5931\u8d25", f"\u65e0\u6cd5\u8bfb\u53d6 SQL \u6587\u4ef6\uff1a{last_error}")


def _ff_execute_migration(self: Any) -> None:
    self.save_manager_state()
    sql_content = self.migration_script_text.get("1.0", tk.END).strip()
    if not sql_content:
        messagebox.showwarning("\u63d0\u793a", "\u8bf7\u5148\u8f93\u5165\u6216\u9009\u62e9 SQL \u811a\u672c\u5185\u5bb9")
        return

    sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
    config = self.get_effective_config()
    proceed = messagebox.askyesno(
        "\u786e\u8ba4\u6267\u884c",
        "\u786e\u5b9a\u8981\u6267\u884c\u6b64 SQL \u811a\u672c\u5417\uff1f\n\u8bf7\u786e\u4fdd\u811a\u672c\u5185\u5bb9\u6b63\u786e\uff0c\u6267\u884c\u540e\u5c06\u76f4\u63a5\u4fee\u6539\u6570\u636e\u5e93\u3002",
    )
    if not proceed:
        return

    ok, detail = execute_sql_script_via_sqlcmd(sql_content, config, sqlcmd)
    if ok:
        messagebox.showinfo("\u6267\u884c\u6210\u529f", detail)
        self.notify_tray("\u6570\u636e\u5e93\u8fc1\u79fb\u6210\u529f", "SQL \u811a\u672c\u5df2\u6267\u884c")
    else:
        messagebox.showerror("\u6267\u884c\u5931\u8d25", detail)


def _ff_restore_database(self: Any) -> None:
    self.save_manager_state()
    sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
    backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
    bak_files = sorted(backup_dir.glob("*.bak"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not bak_files:
        messagebox.showerror("\u6062\u590d\u5931\u8d25", f"\u5907\u4efd\u76ee\u5f55\u4e2d\u672a\u627e\u5230 .bak \u6587\u4ef6\uff1a{backup_dir}")
        return

    file_list = [f"{f.name} ({f.stat().st_size / (1024 * 1024):.1f} MB)" for f in bak_files[:10]]
    choice_window = tk.Toplevel(self.root)
    choice_window.title("\u9009\u62e9\u6062\u590d\u6587\u4ef6")
    choice_window.geometry("500x350")
    ttk.Label(choice_window, text="\u9009\u62e9\u8981\u6062\u590d\u7684\u5907\u4efd\u6587\u4ef6\uff1a").pack(pady=10)
    listbox = tk.Listbox(choice_window, height=12, font=("Consolas", 9))
    listbox.pack(fill="both", expand=True, padx=10, pady=5)
    for item in file_list:
        listbox.insert("end", item)

    def do_restore() -> None:
        selection = listbox.curselection()
        if not selection:
            messagebox.showwarning("\u8b66\u544a", "\u8bf7\u5148\u9009\u62e9\u4e00\u4e2a\u5907\u4efd\u6587\u4ef6")
            return
        selected_file = bak_files[selection[0]]
        proceed = messagebox.askyesno(
            "\u786e\u8ba4\u6062\u590d",
            f"\u786e\u5b9a\u8981\u4ece\u4ee5\u4e0b\u5907\u4efd\u6062\u590d\u6570\u636e\u5e93\u5417\uff1f\n{selected_file.name}\n\n\u8b66\u544a\uff1a\u8fd9\u5c06\u8986\u76d6\u5f53\u524d\u6570\u636e\u5e93\u4e2d\u7684\u6240\u6709\u6570\u636e\uff01",
        )
        if not proceed:
            return
        ok, detail = restore_database_from_backup(self.get_effective_config(), selected_file, sqlcmd)
        choice_window.destroy()
        if ok:
            messagebox.showinfo("\u6062\u590d\u6210\u529f", f"\u6570\u636e\u5e93\u5df2\u4ece {selected_file.name} \u6062\u590d")
            self.notify_tray("\u6570\u636e\u5e93\u6062\u590d\u6210\u529f", f"\u5df2\u4ece {selected_file.name} \u6062\u590d")
        else:
            messagebox.showerror("\u6062\u590d\u5931\u8d25", detail)

    ttk.Button(choice_window, text="\u786e\u8ba4\u6062\u590d", command=do_restore).pack(pady=10)


def sync_backend_dependencies(
    timeout_seconds: int = 900,
    log_callback: Callable[[str], None] | None = None,
) -> Tuple[bool, str]:
    python_exe = get_backend_python_executable()
    requirements_path = BACKEND_DIR / "requirements.txt"
    if not requirements_path.exists():
        return False, f"\u672a\u627e\u5230\u4f9d\u8d56\u6e05\u5355\uff1a{requirements_path}"

    def ensure_backend_venv() -> Tuple[bool, str]:
        nonlocal python_exe
        system_python = "python"
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        if not python_exe.exists():
            if log_callback:
                log_callback("    [RUN] 未检测到后端虚拟环境，正在重新创建...")
            result = subprocess.run(
                [system_python, "-m", "venv", str(BACKEND_VENV_DIR)],
                cwd=BACKEND_DIR,
                capture_output=True,
                text=True,
                timeout=180,
                creationflags=creationflags,
            )
            if result.returncode != 0:
                return False, (result.stderr or result.stdout or "创建后端虚拟环境失败").strip()
            python_exe = BACKEND_VENV_DIR / "Scripts" / "python.exe"

        pip_check = subprocess.run(
            [str(python_exe), "-m", "pip", "--version"],
            cwd=BACKEND_DIR,
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=creationflags,
        )
        if pip_check.returncode == 0:
            return True, "后端虚拟环境可用"

        if log_callback:
            log_callback("    [WARN] 检测到后端虚拟环境缺少 pip，正在重建虚拟环境...")
        try:
            target_venv = python_exe.parents[1]
            if target_venv.resolve() == BACKEND_LEGACY_VENV_DIR.resolve():
                target_venv = BACKEND_VENV_DIR
            shutil.rmtree(target_venv, ignore_errors=True)
        except Exception:
            pass
        result = subprocess.run(
            [system_python, "-m", "venv", str(BACKEND_VENV_DIR)],
            cwd=BACKEND_DIR,
            capture_output=True,
            text=True,
            timeout=180,
            creationflags=creationflags,
        )
        if result.returncode != 0:
            return False, (result.stderr or result.stdout or "重建后端虚拟环境失败").strip()
        python_exe = BACKEND_VENV_DIR / "Scripts" / "python.exe"
        return True, "后端虚拟环境已重建"

    venv_ok, venv_msg = ensure_backend_venv()
    if not venv_ok:
        return False, venv_msg
    if log_callback:
        log_callback(f"    [OK] {venv_msg}")

    try:
        upgrade_code, upgrade_stdout, upgrade_stderr = run_command_with_live_output(
            [str(python_exe), "-m", "pip", "install", "--upgrade", "pip"],
            cwd=BACKEND_DIR,
            timeout_seconds=120,
            log_callback=log_callback,
            heartbeat_label="\u540e\u7aef pip \u5347\u7ea7\u8fdb\u884c\u4e2d",
        )
        if upgrade_code != 0:
            return False, upgrade_stderr or upgrade_stdout or "\u5347\u7ea7 pip \u5931\u8d25"

        result_code, stdout_text, stderr_text = run_command_with_live_output(
            [str(python_exe), "-m", "pip", "install", "-r", str(requirements_path)],
            cwd=BACKEND_DIR,
            timeout_seconds=timeout_seconds,
            log_callback=log_callback,
            heartbeat_label="\u540e\u7aef\u4f9d\u8d56\u540c\u6b65\u8fdb\u884c\u4e2d",
        )
    except Exception as exc:
        return False, f"\u5b89\u88c5\u4f9d\u8d56\u5931\u8d25\uff1a{exc}"

    if result_code != 0:
        return False, stderr_text or stdout_text or "\u672a\u77e5\u9519\u8bef"
    return True, stdout_text or "\u4f9d\u8d56\u5df2\u540c\u6b65"


def sync_frontend_dependencies(
    timeout_seconds: int = 900,
    log_callback: Callable[[str], None] | None = None,
) -> Tuple[bool, str]:
    package_json = FRONTEND_DIR / "package.json"
    if not package_json.exists():
        return False, f"\u672a\u627e\u5230 package.json: {package_json}"

    npm_exe = "npm.cmd" if os.name == "nt" else "npm"
    node_exe = "node.exe" if os.name == "nt" else "node"
    node_env = build_node_process_env()

    try:
        subprocess.run(
            [node_exe, "--version"],
            capture_output=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, "Node.js \u672a\u5b89\u88c5\uff0c\u65e0\u6cd5\u540c\u6b65\u524d\u7aef\u4f9d\u8d56"
    except Exception as exc:
        return False, f"\u68c0\u67e5 Node.js \u5931\u8d25: {exc}"

    try:
        result_code, stdout_text, stderr_text = run_command_with_live_output(
            [npm_exe, "install", "--verbose"],
            cwd=FRONTEND_DIR,
            timeout_seconds=timeout_seconds,
            log_callback=log_callback,
            heartbeat_label="\u524d\u7aef\u4f9d\u8d56\u540c\u6b65\u8fdb\u884c\u4e2d",
            env=node_env,
        )
    except FileNotFoundError:
        return False, f"\u672a\u627e\u5230 npm: {npm_exe}"
    except Exception as exc:
        return False, f"\u5b89\u88c5\u524d\u7aef\u4f9d\u8d56\u5931\u8d25\uff1a{exc}"

    if result_code != 0:
        return False, (stderr_text or stdout_text or "\u672a\u77e5\u9519\u8bef").strip()
    return True, "\u524d\u7aef\u4f9d\u8d56\u5df2\u540c\u6b65"


def build_frontend(
    timeout_seconds: int = 600,
    log_callback: Callable[[str], None] | None = None,
) -> Tuple[bool, str]:
    frontend_dir = ROOT_DIR / "frontend"
    package_json = frontend_dir / "package.json"
    dist_dir = frontend_dir / "dist"
    node_modules = frontend_dir / "node_modules"

    if not package_json.exists():
        return False, "frontend/package.json \u4e0d\u5b58\u5728\uff0c\u8df3\u8fc7\u524d\u7aef\u6784\u5efa"

    npm_exe = "npm.cmd" if os.name == "nt" else "npm"
    node_exe = "node.exe" if os.name == "nt" else "node"
    node_env = build_node_process_env()

    try:
        subprocess.run(
            [node_exe, "--version"],
            capture_output=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except FileNotFoundError:
        return False, "Node.js \u672a\u5b89\u88c5\uff0c\u8df3\u8fc7\u524d\u7aef\u6784\u5efa"
    except Exception as exc:
        return False, f"Node.js \u68c0\u67e5\u6267\u884c\u5f02\u5e38\uff1a{exc}"

    if not node_modules.exists():
        try:
            install_code, install_stdout, install_stderr = run_command_with_live_output(
                [npm_exe, "install", "--verbose"],
                cwd=frontend_dir,
                timeout_seconds=300,
                log_callback=log_callback,
                heartbeat_label="\u524d\u7aef\u4f9d\u8d56\u8865\u9f50\u8fdb\u884c\u4e2d",
                env=node_env,
            )
        except FileNotFoundError:
            return False, f"\u672a\u627e\u5230 npm\uff1a{npm_exe}"
        except Exception as exc:
            return False, f"npm install \u6267\u884c\u5f02\u5e38\uff1a{exc}"
        if install_code != 0:
            return False, f"npm install \u5931\u8d25\uff1a{install_stderr or install_stdout}"

    try:
        build_code, build_stdout, build_stderr = run_command_with_live_output(
            [npm_exe, "run", "build"],
            cwd=frontend_dir,
            timeout_seconds=timeout_seconds,
            log_callback=log_callback,
            heartbeat_label="\u524d\u7aef\u6784\u5efa\u8fdb\u884c\u4e2d",
            env=node_env,
        )
    except FileNotFoundError:
        return False, f"\u672a\u627e\u5230 npm\uff1a{npm_exe}"
    except Exception as exc:
        return False, f"npm run build \u6267\u884c\u5f02\u5e38\uff1a{exc}"
    if build_code != 0:
        return False, f"npm run build \u5931\u8d25\uff1a{build_stderr or build_stdout}"

    if not dist_dir.exists():
        return False, "\u6784\u5efa\u5b8c\u6210\u4f46 dist \u76ee\u5f55\u4e0d\u5b58\u5728"

    return True, f"\u524d\u7aef\u6784\u5efa\u6210\u529f\uff1a{dist_dir}"


def _managed_backend_state_override(self: ManagedBackendController) -> Dict[str, Any]:
    try:
        state = read_managed_runtime_state()
    except Exception:
        state = dict(DEFAULT_RUNTIME_STATE)
        state["service_host_alive"] = False
        state["backend_alive"] = False
        state["frontend_alive"] = False
    try:
        backend_pid = int(state.get("backend_pid") or 0)
    except Exception:
        backend_pid = 0
    self.process = SimpleNamespace(pid=backend_pid) if backend_pid > 0 else None
    return state


def _backend_process_init_override(self: BackendProcessController) -> None:
    self.process = None
    self.stdout_handle = None
    self.stderr_handle = None
    self.user_stopped = False
    self.last_start_attempt = 0.0
    self.last_exit_code = None
    self.last_exit_at = 0.0
    self.last_started_at = 0.0
    self.consecutive_failed_starts = 0
    self.auto_restart_suppressed = False
    self.last_session_marker = ""
    self.last_session_started_label = ""
    write_runtime_state(
        backend_pid=0,
        user_stopped=False,
        last_start_attempt=0.0,
        last_started_at=0.0,
        last_exit_code=None,
        last_exit_at=0.0,
        consecutive_failed_starts=0,
        auto_restart_suppressed=False,
        last_session_marker="",
        last_session_started_label="",
    )


def _ff_refresh_status_override(self: Any) -> None:
    backend_status = self.backend.poll_status()
    frontend_status = self.frontend.poll_status()
    config = self.get_effective_config()
    backend_port = (config.get("APP_PORT") or "8100").strip() or "8100"
    frontend_settings = resolve_frontend_service_settings(config)
    frontend_port = frontend_settings["frontend_port"]
    backend_owner_text = "未检测"
    frontend_owner_text = "未检测"

    if backend_port.isdigit():
        owner = get_port_owner_info(int(backend_port))
        if owner:
            manager_pid = safe_process_pid(self.backend.process) if self.backend.is_running() else None
            backend_owner_text = build_port_owner_label(owner, manager_pid)
    if frontend_port.isdigit():
        owner = get_port_owner_info(int(frontend_port))
        if owner:
            manager_pid = safe_process_pid(self.frontend.process) if self.frontend.is_running() else None
            frontend_owner_text = build_port_owner_label(owner, manager_pid)

    self.status_vars["env_file"].set("已存在" if ENV_PATH.exists() else "不存在")
    self.status_vars["key_file"].set("已存在" if KEY_PATH.exists() else "不存在")
    self.status_vars["dist_dir"].set("已存在" if (DIST_DIR / "index.html").exists() else "不存在")
    self.status_vars["backend_status"].set(backend_status)
    self.status_vars["backend_port_owner"].set(backend_owner_text)
    self.status_vars["backend_url"].set(self.get_backend_url())
    self.status_vars["frontend_status"].set(frontend_status)
    self.status_vars["frontend_port_owner"].set(frontend_owner_text)
    self.status_vars["frontend_url"].set(frontend_settings["frontend_url"])
    _update_service_action_buttons(self)
    self.refresh_status_badges()


def _get_effective_config_override(self: Any) -> Dict[str, str]:
    data = dict(getattr(self, "config_values", {}) or {})
    try:
        for key, var in self.form_vars.items():
            data[key] = var.get().strip()
    except Exception:
        disk_values = read_env_file(ENV_PATH)
        if disk_values:
            data.update(disk_values)
    data["APP_RELOAD"] = "false"
    return data


def _safe_save_config_before_service_action(self: Any) -> Tuple[bool, str]:
    try:
        self.save_config_values()
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _handle_start_backend_override(self: Any) -> None:
    saved, save_error = _safe_save_config_before_service_action(self)
    config = self.get_effective_config()
    self.backend.clear_restart_failure_state()
    ok, message = self.backend.start(config)
    if ok:
        self.log_status_var.set(f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）")
        self.notify_tray("FinFlow 已启动", message)
        if not saved and save_error:
            message += f"\n\n注意：界面未保存的配置未能写入，已按最近一次有效配置启动。\n原因：{save_error}"
        messagebox.showinfo("启动结果", message)
    else:
        warn_message = message
        if not saved and save_error:
            warn_message += f"\n\n界面未保存配置读取失败：{save_error}"
        messagebox.showwarning("启动结果", warn_message)
    self.refresh_status()


def _handle_start_frontend_override(self: Any, show_dialog: bool = True) -> Tuple[bool, str]:
    saved, save_error = _safe_save_config_before_service_action(self)
    config = self.get_effective_config()
    self.frontend.clear_restart_failure_state()
    ok, message = self.frontend.start(config)
    if ok:
        self.log_status_var.set(f"当前显示：本次启动日志（前端，自 {self.frontend.last_session_started_label} 起）")
        self.notify_tray("前端已启动", message)
        if not saved and save_error:
            message += f"\n\n注意：界面未保存的配置未能写入，已按最近一次有效配置启动。\n原因：{save_error}"
        if show_dialog:
            messagebox.showinfo("启动结果", message)
    else:
        if not saved and save_error:
            message += f"\n\n界面未保存配置读取失败：{save_error}"
        if show_dialog:
            messagebox.showwarning("启动结果", message)
    self.refresh_status()
    return ok, message


def _handle_start_all_override(self: Any, show_dialog: bool = True) -> Tuple[bool, str]:
    saved, save_error = _safe_save_config_before_service_action(self)
    config = self.get_effective_config()
    self.backend.clear_restart_failure_state()
    backend_started, backend_message = self.backend.start(config)
    if not backend_started and self.backend.is_running():
        backend_started = True
        backend_message = backend_message or "后端已在运行"

    frontend_ok = False
    frontend_message = "后端启动失败，未继续启动前端"
    if backend_started:
        self.frontend.clear_restart_failure_state()
        frontend_ok, frontend_message = self.frontend.start(config)
        if frontend_ok:
            self.log_status_var.set(f"当前显示：本次启动日志（前端，自 {self.frontend.last_session_started_label} 起）")

    self.refresh_status()
    message = f"后端：{backend_message}\n前端：{frontend_message}"
    if not saved and save_error:
        message += f"\n\n注意：界面未保存的配置未能写入，已按最近一次有效配置执行。\n原因：{save_error}"
    if show_dialog:
        dialog = messagebox.showinfo if (backend_started and frontend_ok) else messagebox.showwarning
        dialog("启动结果", message)
    return backend_started and frontend_ok, message


def _update_service_action_buttons(self: Any) -> None:
    buttons = getattr(self, "service_action_buttons", {}) or {}
    if not buttons:
        return
    backend_running = bool(self.backend.is_running())
    frontend_running = bool(self.frontend.is_running())
    config = self.get_effective_config()
    backend_port = (config.get("APP_PORT") or "8100").strip() or "8100"
    frontend_settings = resolve_frontend_service_settings(config)
    frontend_port = frontend_settings["frontend_port"]
    backend_port_busy = backend_port.isdigit() and bool(get_port_owner_info(int(backend_port)))
    frontend_port_busy = frontend_port.isdigit() and bool(get_port_owner_info(int(frontend_port)))
    all_running = backend_running and frontend_running
    any_running = backend_running or frontend_running

    desired_states = {
        "start_backend": "disabled" if (backend_running or backend_port_busy) else "normal",
        "stop_backend": "normal" if backend_running else "disabled",
        "restart_backend": "normal" if backend_running else "disabled",
        "takeover_backend": "normal" if (not backend_running and backend_port_busy) else "disabled",
        "start_frontend": "disabled" if (frontend_running or frontend_port_busy) else "normal",
        "stop_frontend": "normal" if frontend_running else "disabled",
        "restart_frontend": "normal" if frontend_running else "disabled",
        "start_all": "disabled" if all_running else "normal",
        "stop_all": "normal" if any_running else "disabled",
        "restart_all": "normal" if any_running else "disabled",
    }
    for key, state in desired_states.items():
        button = buttons.get(key)
        if button is None:
            continue
        try:
            button.configure(state=state)
        except Exception:
            pass


def _handle_takeover_backend_override(self: Any) -> None:
    config = self.get_effective_config()
    port = (config.get("APP_PORT") or "8100").strip() or "8100"
    if not port.isdigit():
        messagebox.showerror("无法接管", "APP_PORT 不是有效数字")
        return

    owner = get_port_owner_info(int(port))
    if not owner:
        messagebox.showinfo("无需接管", f"端口 {port} 当前未被占用，可以直接使用“启动后端”")
        return

    owner_pid = int(owner.get("pid", "0") or "0")
    if self.backend.is_running() and self.backend.process and owner_pid == safe_process_pid(self.backend.process):
        messagebox.showinfo("已由管理器接管", "当前后端实例已经是由管理器启动的")
        return

    confirm = messagebox.askyesno(
        "确认接管",
        f"检测到端口 {port} 当前由 PID {owner_pid} 占用。\n\n如继续接管，管理器会记录该实例状态并按当前配置进行统一管理。\n是否继续？",
    )
    if not confirm:
        return

    self.backend.process = SimpleNamespace(pid=owner_pid)
    write_runtime_state(
        service_host_pid=0,
        backend_pid=owner_pid,
        user_stopped=False,
        last_start_attempt=time.time(),
        last_started_at=time.time(),
    )
    self.refresh_status()
    messagebox.showinfo("接管完成", f"已将 PID {owner_pid} 记录为当前后端实例")


def _backend_process_start_verified(self: BackendProcessController, config: Dict[str, str]) -> Tuple[bool, str]:
    if self.is_running():
        return False, "后端已在运行"

    requirement_ok, requirement_msg = check_backend_runtime_requirements(config)
    if not requirement_ok:
        return False, requirement_msg

    python_exe = get_backend_python_executable()
    ensure_log_dir()
    max_size = 20.0
    try:
        state = read_state_file(STATE_PATH)
        max_size = float(state.get("log_max_size_mb", 20))
    except Exception:
        pass
    for log_path in (STDOUT_LOG, STDERR_LOG):
        rotate_log_file(log_path, max_size)

    env = os.environ.copy()
    env.update(config)
    env["PYTHONUNBUFFERED"] = "1"
    env["APP_RELOAD"] = "false"
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    host = (config.get("APP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    port = (config.get("APP_PORT") or "8100").strip() or "8100"
    browser_host = resolve_browser_host(host)
    if port.isdigit():
        port_num = int(port)
        if is_port_open(browser_host, port_num) or (host == "0.0.0.0" and is_port_open("127.0.0.1", port_num)):
            if can_connect_http(browser_host, port_num):
                return False, f"端口 {port} 已被现有服务占用，且页面可访问，请先停止旧实例后再启动"
            return False, f"端口 {port} 已被其他进程占用，请先释放端口后再启动"

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    session_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.last_session_started_label = session_label
    self.last_session_marker = f"===== FinFlowManager Session Start {session_label} ====="
    for log_path in (STDOUT_LOG, STDERR_LOG):
        with open(log_path, "a", encoding="utf-8", errors="ignore") as marker_handle:
            marker_handle.write(f"\n{self.last_session_marker}\n")
    self.stdout_handle = open(STDOUT_LOG, "a", encoding="utf-8", errors="ignore")
    self.stderr_handle = open(STDERR_LOG, "a", encoding="utf-8", errors="ignore")

    try:
        self.process = subprocess.Popen(
            [str(python_exe), "-m", "uvicorn", "main:app", "--host", host, "--port", port, "--app-dir", str(BACKEND_DIR)],
            cwd=BACKEND_DIR,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=self.stdout_handle,
            stderr=self.stderr_handle,
            creationflags=creationflags,
        )
    except Exception as exc:
        self._close_handles()
        self.process = None
        return False, f"启动失败：{exc}"

    self.user_stopped = False
    self.last_start_attempt = time.time()
    self.last_started_at = self.last_start_attempt
    self.last_exit_code = None
    self.auto_restart_suppressed = False

    deadline = time.time() + 8
    while time.time() < deadline:
        exit_code = self.capture_exit()
        if exit_code is not None:
            log_tail = tail_text_file(STDERR_LOG) or tail_text_file(STDOUT_LOG)
            detail = log_tail or f"后端进程已退出，code={exit_code}"
            return False, f"后端启动失败：{detail}"
        if port.isdigit():
            port_num = int(port)
            if is_port_open(browser_host, port_num) or (host == "0.0.0.0" and is_port_open("127.0.0.1", port_num)):
                return True, f"后端已启动，PID={safe_process_pid(self.process)}，监听 {browser_host}:{port}"
        time.sleep(0.25)

    return True, f"后端进程已启动，PID={safe_process_pid(self.process)}，正在等待端口 {port} 就绪"


def _frontend_process_start_verified(self: FrontendProcessController, config: Dict[str, str]) -> Tuple[bool, str]:
    if self.is_running():
        return False, "前端已在运行"

    package_json = FRONTEND_DIR / "package.json"
    vite_cli = FRONTEND_DIR / "node_modules" / "vite" / "bin" / "vite.js"
    node_exe = "node.exe" if os.name == "nt" else "node"
    ok, detail = sync_frontend_runtime_env(config)
    if not ok:
        return False, detail

    ensure_log_dir()
    max_size = 20.0
    try:
        state = read_state_file(STATE_PATH)
        max_size = float(state.get("log_max_size_mb", 20))
    except Exception:
        pass
    for log_path in (FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG):
        rotate_log_file(log_path, max_size)

    settings = resolve_frontend_service_settings(config)
    frontend_host = settings["frontend_host"]
    frontend_port = settings["frontend_port"]
    if frontend_port.isdigit():
        port_num = int(frontend_port)
        if is_port_open(frontend_host, port_num):
            if can_connect_http(frontend_host, port_num):
                return False, f"前端端口 {frontend_port} 已被现有服务占用，请先停止旧实例后再启动"
            return False, f"前端端口 {frontend_port} 已被其他进程占用，请先释放端口后再启动"

    use_vite_dev_server = package_json.exists() and vite_cli.exists()
    env = os.environ.copy()
    env["PYTHONWARNINGS"] = "ignore"
    if use_vite_dev_server:
        try:
            subprocess.run(
                [node_exe, "--version"],
                capture_output=True,
                timeout=5,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except FileNotFoundError:
            return False, "未找到 Node.js，请先安装 Node.js 后再启动前端"
        except Exception as exc:
            return False, f"检查 Node.js 失败: {exc}"

        env = build_node_process_env(env)
        env.update(read_env_file(FRONTEND_ENV_EXAMPLE_PATH))
        env.update(read_env_file(FRONTEND_ENV_PATH))
        env["BROWSER"] = "none"

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    session_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    self.last_session_started_label = session_label
    self.last_session_marker = f"===== FinFlowManager Frontend Session Start {session_label} ====="
    for log_path in (FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG):
        with open(log_path, "a", encoding="utf-8", errors="ignore") as marker_handle:
            marker_handle.write(f"\n{self.last_session_marker}\n")
    self.stdout_handle = open(FRONTEND_STDOUT_LOG, "a", encoding="utf-8", errors="ignore")
    self.stderr_handle = open(FRONTEND_STDERR_LOG, "a", encoding="utf-8", errors="ignore")

    try:
        self.process = subprocess.Popen(
            ([node_exe, str(vite_cli), "--host", "0.0.0.0", "--port", frontend_port] if use_vite_dev_server else resolve_frontend_host_launcher()),
            cwd=FRONTEND_DIR,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=self.stdout_handle,
            stderr=self.stderr_handle,
            creationflags=creationflags,
        )
    except Exception as exc:
        self._close_handles()
        self.process = None
        return False, f"启动前端失败: {exc}"

    self.user_stopped = False
    self.last_start_attempt = time.time()
    self.last_started_at = self.last_start_attempt
    self.last_exit_code = None
    self.auto_restart_suppressed = False
    self._sync_runtime_state()

    deadline = time.time() + 8
    while time.time() < deadline:
        exit_code = self.capture_exit()
        if exit_code is not None:
            log_tail = tail_text_file(FRONTEND_STDERR_LOG) or tail_text_file(FRONTEND_STDOUT_LOG)
            detail = log_tail or f"前端进程已退出，code={exit_code}"
            return False, f"前端启动失败: {detail}"
        if frontend_port.isdigit() and is_port_open(frontend_host, int(frontend_port)):
            self._sync_runtime_state()
            mode_text = "Vite 开发服务" if use_vite_dev_server else "静态前端服务"
            return True, f"前端已启动（{mode_text}），PID={safe_process_pid(self.process)}，访问地址 {settings['frontend_url']}"
        time.sleep(0.25)

    self._sync_runtime_state()
    return True, f"前端进程已启动，PID={safe_process_pid(self.process)}，正在等待 {settings['frontend_url']} 就绪"


def _managed_backend_start_sessioned(self: ManagedBackendController, _config: Dict[str, str] | None = None) -> Tuple[bool, str]:
    try:
        state = self._state()
    except Exception:
        state = {"service_host_alive": False, "service_host_pid": 0}
    if bool(state.get("service_host_alive")):
        host_pid = int(state.get("service_host_pid") or 0)
        return False, f"服务宿主已在运行，PID={host_pid}"

    runtime_config = dict(_config or load_effective_config_from_disk())
    requirement_ok, requirement_msg = check_backend_runtime_requirements(runtime_config)
    if not requirement_ok:
        return False, requirement_msg

    host = resolve_browser_host(runtime_config.get("APP_HOST", "127.0.0.1"))
    port_text = (runtime_config.get("APP_PORT") or "8100").strip() or "8100"
    if port_text.isdigit() and is_port_open(host, int(port_text)):
        return False, f"后端端口 {port_text} 已被占用，请先确认现有实例状态"

    ensure_runtime_dir()
    clear_stop_request()
    ensure_log_dir()
    max_size = 20.0
    try:
        state_config = read_state_file(STATE_PATH)
        max_size = float(state_config.get("log_max_size_mb", 20))
    except Exception:
        pass
    rotate_log_file(SERVICE_HOST_LOG, max_size)

    creationflags = 0
    for flag_name in ("CREATE_NO_WINDOW", "DETACHED_PROCESS", "CREATE_NEW_PROCESS_GROUP"):
        creationflags |= getattr(subprocess, flag_name, 0)

    launcher = resolve_service_host_launcher()
    child_env = os.environ.copy()
    child_env["PYTHONWARNINGS"] = "ignore"
    session_label = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    session_marker = f"===== FinFlowManager Service Host Session Start {session_label} ====="

    with open(SERVICE_HOST_LOG, "a", encoding="utf-8", errors="ignore") as service_log:
        service_log.write(f"\n{session_marker}\n")
        service_log.flush()
        process = subprocess.Popen(
            launcher,
            cwd=ROOT_DIR,
            env=child_env,
            stdin=subprocess.DEVNULL,
            stdout=service_log,
            stderr=service_log,
            creationflags=creationflags,
        )

    process_pid = safe_process_pid(process)
    self.process = SimpleNamespace(pid=process_pid) if process_pid > 0 else None
    write_runtime_state(service_host_pid=process_pid, backend_pid=0, user_stopped=False)
    if process_pid <= 0:
        log_tail = read_log_since_marker(SERVICE_HOST_LOG, session_marker)
        return False, f"服务宿主启动失败：未获取到有效进程 PID。\n{log_tail or '请检查 manager.service.log'}"

    deadline = time.time() + 12
    while time.time() < deadline:
        try:
            current = self._state()
            if bool(current.get("service_host_alive")):
                host_pid = int(current.get("service_host_pid") or process_pid)
                if bool(current.get("backend_alive")):
                    backend_pid = int(current.get("backend_pid") or 0)
                    return True, f"服务宿主已启动，PID={host_pid}，后端 PID={backend_pid}"
                return True, f"服务宿主已启动，PID={host_pid}"
        except Exception:
            pass
        if not is_process_alive(process_pid):
            log_tail = read_log_since_marker(SERVICE_HOST_LOG, session_marker)
            runtime_state_text = build_runtime_state_snapshot_text()
            detail = log_tail or "请检查 manager.service.log"
            return False, f"服务宿主启动失败。\n{detail}\n{runtime_state_text}"
        time.sleep(0.25)

    if is_process_alive(process_pid):
        current = self._state()
        if bool(current.get("backend_alive")) or bool(current.get("service_host_alive")):
            host_pid = int(current.get("service_host_pid") or process_pid)
            backend_pid = int(current.get("backend_pid") or 0)
            if backend_pid > 0:
                return True, f"服务宿主已启动，PID={host_pid}，后端 PID={backend_pid}"
            return True, f"服务宿主进程已启动，PID={host_pid}，正在等待状态同步"

    log_tail = read_log_since_marker(SERVICE_HOST_LOG, session_marker)
    if not log_tail or log_tail.strip() == session_marker.strip():
        child_alive = is_process_alive(process_pid)
        diagnostics = [
            f"child_pid={process_pid}",
            f"child_alive={child_alive}",
            f"runtime_state_exists={RUNTIME_STATE_PATH.exists()}",
            f"backend_main_exists={(BACKEND_DIR / 'main.py').exists()}",
            f"backend_python={get_backend_python_executable()}",
            f"backend_python_exists={get_backend_python_executable().exists()}",
        ]
        log_tail = session_marker + "\n" + "\n".join(diagnostics)
    return False, f"服务宿主在等待时间内未报告就绪。\n{log_tail or '请检查 manager.service.log'}\n{build_runtime_state_snapshot_text()}"


BackendProcessController.__init__ = _backend_process_init_override
BackendProcessController.start = _backend_process_start_verified
FrontendProcessController.start = _frontend_process_start_verified
ManagedBackendController._state = _managed_backend_state_override
ManagedBackendController.start = _managed_backend_start_sessioned
FinFlowManagerApp.get_effective_config = _get_effective_config_override
FinFlowManagerApp.handle_start_backend = _handle_start_backend_override
FinFlowManagerApp.handle_start_frontend = _handle_start_frontend_override
FinFlowManagerApp.handle_start_all = _handle_start_all_override
FinFlowManagerApp.handle_takeover_backend = _handle_takeover_backend_override
_ff_refresh_status = _ff_refresh_status_override


if __name__ == "__main__":
    cli_parser = build_cli_parser()
    cli_args = cli_parser.parse_args()
    raise SystemExit(run_cli(cli_args))
