# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import platform
import secrets
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import http.client

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
        host = BackendServiceHost()
        return host.run()

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
        start_dir = Path(sys.executable).resolve().parent
    else:
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
ENV_PATH = BACKEND_DIR / ".env"
ENV_EXAMPLE_PATH = BACKEND_DIR / ".env.example"
FRONTEND_ENV_PATH = FRONTEND_DIR / ".env"
FRONTEND_ENV_EXAMPLE_PATH = FRONTEND_DIR / ".env.example"
KEY_PATH = BACKEND_DIR / ".encryption.key"
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


def create_tray_image() -> Image.Image:
    ensure_gui_dependencies()
    image = Image.new("RGBA", (64, 64), (27, 84, 157, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill=(39, 125, 161, 255))
    draw.rectangle((20, 18, 44, 24), fill=(255, 255, 255, 255))
    draw.rectangle((20, 30, 44, 36), fill=(255, 255, 255, 255))
    draw.rectangle((20, 42, 36, 48), fill=(255, 255, 255, 255))
    return image


def create_window_icon() -> Image.Image:
    ensure_gui_dependencies()
    image = Image.new("RGBA", (256, 256), (27, 84, 157, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((32, 32, 224, 224), radius=48, fill=(39, 125, 161, 255))
    draw.rectangle((80, 64, 176, 96), fill=(255, 255, 255, 255))
    draw.rectangle((80, 112, 176, 144), fill=(255, 255, 255, 255))
    draw.rectangle((80, 160, 144, 192), fill=(255, 255, 255, 255))
    return image


def create_high_res_icon() -> Image.Image:
    ensure_gui_dependencies()
    image = Image.new("RGBA", (512, 512), (27, 84, 157, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((64, 64, 448, 448), radius=96, fill=(39, 125, 161, 255))
    draw.rectangle((160, 128, 352, 192), fill=(255, 255, 255, 255))
    draw.rectangle((160, 224, 352, 288), fill=(255, 255, 255, 255))
    draw.rectangle((160, 320, 320, 384), fill=(255, 255, 255, 255))
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
    python_exe = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
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
    return False, stderr_text or stdout_text or "数据库连接失败"


def evaluate_database_runtime_status(sqlcmd: str = "sqlcmd") -> Tuple[str, str]:
    config_state, config_detail = describe_database_configuration_from_disk()
    if config_state != "ready":
        return config_state, config_detail

    config = load_effective_config_from_disk()
    runtime_python = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
    if runtime_python.exists():
        ok, detail = check_database_connectivity_via_backend_runtime(config)
        return ("ok" if ok else "error"), detail

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
        return False, "frontend/package.json 不存在，跳过前端构建"

    if dist_dir.exists() and node_modules.exists():
        return True, "前端构建产物已存在，跳过构建"

    node_exe = "node"
    npm_exe = "npm"

    try:
        subprocess.run([node_exe, "--version"], capture_output=True, timeout=5, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except FileNotFoundError:
        return False, "Node.js 未安装，跳过前端构建"

    if not node_modules.exists():
        result = subprocess.run(
            [npm_exe, "install"],
            cwd=frontend_dir,
            capture_output=True,
            text=True,
            timeout=300,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0:
            return False, f"npm install 失败: {result.stderr or result.stdout}"

    result = subprocess.run(
        [npm_exe, "run", "build"],
        cwd=frontend_dir,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        return False, f"npm run build 失败: {result.stderr or result.stdout}"

    if not dist_dir.exists():
        return False, "构建完成但 dist 目录不存在"

    return True, f"前端构建成功: {dist_dir}"


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
        return self.process is not None and self.process.poll() is None

    def capture_exit(self) -> int | None:
        if self.process is None:
            return None
        code = self.process.poll()
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
            assert self.process is not None
            self.process.terminate()
            self.process.wait(timeout=10)
        except Exception:
            try:
                assert self.process is not None
                self.process.kill()
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
        if self.is_running():
            assert self.process is not None
            return f"运行中 (PID {self.process.pid})"
        exit_code = self.capture_exit()
        if exit_code is not None:
            return f"已退出 (code {exit_code})"
        return "未运行"

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
            frontend_pid=int(getattr(self.process, "pid", 0) or 0),
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
        if self.process is None:
            return False
        if isinstance(self.process, subprocess.Popen):
            return self.process.poll() is None
        return is_process_alive(int(getattr(self.process, "pid", 0) or 0))

    def capture_exit(self) -> int | None:
        if self.process is None:
            return None
        if isinstance(self.process, subprocess.Popen):
            code = self.process.poll()
            if code is None:
                return None
        else:
            pid = int(getattr(self.process, "pid", 0) or 0)
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
                self.process.terminate()
                self.process.wait(timeout=10)
            else:
                pid = int(getattr(self.process, "pid", 0) or 0)
                if pid > 0:
                    os.kill(pid, signal.SIGTERM)
        except Exception:
            try:
                pid = int(getattr(self.process, "pid", 0) or 0)
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
        if self.is_running():
            return f"运行中 (PID {int(getattr(self.process, 'pid', 0) or 0)})"
        exit_code = self.capture_exit()
        if exit_code is not None:
            return f"已退出 (code {exit_code})"
        return "未运行"

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
        return [str(Path(sys.executable).resolve()), "--service-run"]

    python_exe = Path(sys.executable).resolve()
    if python_exe.name.lower() == "pythonw.exe":
        candidate = python_exe.with_name("python.exe")
        if candidate.exists():
            python_exe = candidate
    return [str(python_exe), str(Path(__file__).resolve()), "--service-run"]


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
        state = self._state()
        host_pid = int(state.get("service_host_pid") or 0)
        backend_pid = int(state.get("backend_pid") or 0)
        if state.get("service_host_alive") and state.get("backend_alive"):
            return f"运行中 (宿主 PID {host_pid}, 后端 PID {backend_pid})"
        if state.get("service_host_alive"):
            return f"宿主运行中 (PID {host_pid})，后端启动中或异常退出"
        if state.get("backend_alive"):
            return f"后端运行中 (PID {backend_pid})，但未由服务宿主管理"
        if state.get("last_exit_code") is not None:
            return f"未运行 (最近退出码 {state['last_exit_code']})"
        return "未运行"


class BackendServiceHost:
    def __init__(self) -> None:
        self.backend = BackendProcessController()
        self.stop_event = threading.Event()

    def request_stop(self, *_args: Any) -> None:
        self.stop_event.set()

    def _sync_runtime_state(self) -> None:
        backend_pid = self.backend.process.pid if self.backend.is_running() and self.backend.process else 0
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
        ok, message = self.backend.start(config)
        self._sync_runtime_state()
        if not ok:
            print(message)

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
        self.manager_option_vars: Dict[str, tk.BooleanVar] = {}
        self.ops_vars: Dict[str, tk.StringVar] = {}
        self.init_ops_vars()
        self.log_choice = tk.StringVar(value="后端标准输出")
        self.log_auto_refresh = tk.BooleanVar(value=True)
        self.log_status_var = tk.StringVar(value="当前显示：历史日志")
        self.tray_icon: Icon | None = None
        self.tray_thread: threading.Thread | None = None
        self.exiting = False
        self.last_notified_exit_at = 0.0
        self.last_notified_frontend_exit_at = 0.0
        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        self.last_health_state = "unknown"
        self.last_database_status_state = "unknown"
        self.status_job: str | None = None
        self.log_job: str | None = None
        self.health_job: str | None = None
        self.db_job: str | None = None
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
            raise ValueError("APP_PORT 必须是数字")
        token_minutes = data.get("ACCESS_TOKEN_EXPIRE_MINUTES", "")
        if token_minutes and not token_minutes.isdigit():
            raise ValueError("ACCESS_TOKEN_EXPIRE_MINUTES 必须是数字")

        write_env_file(ENV_PATH, data, self.extra_env_values)
        self.config_values = data

    def build_ui(self) -> None:
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill="both", expand=True, padx=10, pady=10)

        status_frame = ttk.Frame(notebook)
        config_frame = ttk.Frame(notebook)
        ops_frame = ttk.Frame(notebook)
        env_frame = ttk.Frame(notebook)
        logs_frame = ttk.Frame(notebook)

        notebook.add(status_frame, text="服务状态")
        notebook.add(config_frame, text="配置管理")
        notebook.add(ops_frame, text="运维工具")
        notebook.add(env_frame, text="环境检查")
        notebook.add(logs_frame, text="日志查看")

        self.build_status_tab(status_frame)
        self.build_config_tab(config_frame)
        self.build_ops_tab(ops_frame)
        self.build_env_tab(env_frame)
        self.build_logs_tab(logs_frame)

    def build_status_tab(self, parent: ttk.Frame) -> None:
        db_config_state, db_config_detail = describe_database_configuration_from_disk()
        canvas = tk.Canvas(parent, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        summary = ttk.LabelFrame(scrollable_frame, text="当前状态", padding=16)
        summary.pack(fill="x", padx=10, pady=10)

        status_items = {
            "project_root": str(ROOT_DIR),
            "env_file": "已存在" if ENV_PATH.exists() else "缺失",
            "key_file": "已存在" if KEY_PATH.exists() else "缺失",
            "dist_dir": "已存在" if (DIST_DIR / "index.html").exists() else "缺失",
            "backend_status": "未运行",
            "backend_port_owner": "未占用",
            "backend_health_status": "未检查",
            "backend_url": self.get_backend_url(),
            "frontend_status": self.frontend.poll_status(),
            "frontend_port_owner": "未占用",
            "frontend_health_status": "未检查",
            "frontend_url": self.get_frontend_url(),
            "db_config_status": format_database_config_status(db_config_state, db_config_detail),
            "db_connection_status": "未检查",
            "db_monitor_status": "监控中" if self.manager_state.get("enable_db_monitor", True) else "已关闭",
            "db_last_check_at": "未检查",
            "startup_status": self.get_startup_status_text(),
            "app_url": self.get_app_url(),
        }

        row = 0
        for key, value in status_items.items():
            label_text = {
                "project_root": "项目目录",
                "env_file": "配置文件",
                "key_file": "加密密钥",
                "dist_dir": "前端构建",
                "backend_status": "后端状态",
                "backend_port_owner": "后端端口占用",
                "backend_health_status": "后端健康检查",
                "backend_url": "后端入口",
                "frontend_status": "前端状态",
                "frontend_port_owner": "前端端口占用",
                "frontend_health_status": "前端健康检查",
                "frontend_url": "前端入口",
                "db_config_status": "数据库配置",
                "db_connection_status": "数据库连接",
                "db_monitor_status": "数据库监控",
                "db_last_check_at": "最后检查",
                "startup_status": "开机自启",
                "app_url": "默认访问地址",
            }[key]
            ttk.Label(summary, text=f"{label_text}：").grid(row=row, column=0, sticky="w", pady=4)
            var = tk.StringVar(value=value)
            ttk.Label(summary, textvariable=var).grid(row=row, column=1, sticky="w", pady=4)
            self.status_vars[key] = var
            row += 1

        actions = ttk.LabelFrame(scrollable_frame, text="服务操作", padding=16)
        actions.pack(fill="x", padx=10, pady=10)

        ttk.Button(actions, text="启动全部", command=self.handle_start_all).grid(row=0, column=0, padx=6, pady=6)
        ttk.Button(actions, text="停止全部", command=self.handle_stop_all).grid(row=0, column=1, padx=6, pady=6)
        ttk.Button(actions, text="重启全部", command=self.handle_restart_all).grid(row=0, column=2, padx=6, pady=6)
        ttk.Button(actions, text="打开前端", command=self.open_frontend).grid(row=0, column=3, padx=6, pady=6)
        ttk.Button(actions, text="打开日志目录", command=self.open_logs_folder).grid(row=0, column=4, padx=6, pady=6)
        ttk.Button(actions, text="隐藏到托盘", command=self.hide_to_tray).grid(row=0, column=5, padx=6, pady=6)
        ttk.Button(actions, text="启动后端", command=self.handle_start_backend).grid(row=1, column=0, padx=6, pady=6)
        ttk.Button(actions, text="停止后端", command=self.handle_stop_backend).grid(row=1, column=1, padx=6, pady=6)
        ttk.Button(actions, text="重启后端", command=self.handle_restart_backend).grid(row=1, column=2, padx=6, pady=6)
        ttk.Button(actions, text="接管现有后端", command=self.handle_takeover_backend).grid(row=1, column=3, padx=6, pady=6)
        ttk.Button(actions, text="释放后端端口", command=self.handle_force_release_port).grid(row=1, column=4, padx=6, pady=6)
        ttk.Button(actions, text="启动前端", command=self.handle_start_frontend).grid(row=2, column=0, padx=6, pady=6)
        ttk.Button(actions, text="停止前端", command=self.handle_stop_frontend).grid(row=2, column=1, padx=6, pady=6)
        ttk.Button(actions, text="重启前端", command=self.handle_restart_frontend).grid(row=2, column=2, padx=6, pady=6)
        ttk.Button(actions, text="检查数据库连接", command=self.check_database_connection).grid(row=2, column=3, padx=6, pady=6)

        options = ttk.LabelFrame(scrollable_frame, text="管理器选项", padding=16)
        options.pack(fill="x", padx=10, pady=10)

        option_items = [
            ("auto_restart_backend", "后端异常退出后自动拉起"),
            ("auto_restart_frontend", "前端异常退出后自动拉起"),
            ("start_backend_on_launch", "打开管理器后自动启动后端"),
            ("start_frontend_on_launch", "打开管理器后自动启动前端"),
            ("hide_to_tray_on_close", "关闭窗口时最小化到托盘"),
            ("launch_manager_on_startup", "Windows 登录后自动启动管理器"),
            ("enable_health_check", "启用健康检查与托盘状态提示"),
            ("enable_db_monitor", "启用数据库连接监控与告警"),
        ]
        for idx, (key, label) in enumerate(option_items):
            var = tk.BooleanVar(value=self.manager_state.get(key, False))
            self.manager_option_vars[key] = var
            ttk.Checkbutton(options, text=label, variable=var, command=self.save_manager_state).grid(
                row=idx // 3, column=idx % 3, padx=10, pady=4, sticky="w"
            )

        tips = ttk.LabelFrame(scrollable_frame, text="说明", padding=16)
        tips.pack(fill="both", expand=True, padx=10, pady=10)
        ttk.Label(
            tips,
            text=(
                "当前模式下不依赖 IIS、NSSM 或 Windows 服务。\n"
                "管理器负责配置 backend/.env、backend/.encryption.key，并统一管理后端 API 与前端服务的启动、停止、监控和自动拉起。\n"
                "如果前端服务未启动，而 frontend/dist 已构建完成，仍然可以通过后端入口访问内置静态页面。"
            ),
            justify="left",
        ).pack(anchor="w")

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
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", padx=10, pady=10)

        ttk.Button(toolbar, text="保存配置", command=self.handle_save_config).pack(side="left", padx=4)
        ttk.Button(toolbar, text="重新加载", command=self.reload_form_from_disk).pack(side="left", padx=4)
        ttk.Button(toolbar, text="生成 JWT 密钥", command=self.generate_secret_key).pack(side="left", padx=4)
        ttk.Button(toolbar, text="生成加密密钥", command=self.generate_encryption_key).pack(side="left", padx=4)
        ttk.Button(toolbar, text="打开 backend 目录", command=self.open_backend_folder).pack(side="left", padx=4)

        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        nav_frame = tk.Frame(
            container,
            bg="#f0f0f0",
            bd=1,
            relief="flat",
        )
        nav_frame.pack(side="left", fill="y", padx=(0, 10))

        nav_title = tk.Label(
            nav_frame,
            text="配置导航",
            bg="#f0f0f0",
            fg="#333333",
            font=("Microsoft YaHei UI", 10, "bold"),
            anchor="w",
        )
        nav_title.pack(fill="x", padx=8, pady=(8, 12))

        content_frame = ttk.Frame(container)
        content_frame.pack(side="left", fill="both", expand=True)

        self.config_section_var = tk.StringVar(value=ENV_SECTIONS[0][0])
        self.config_section_frames: Dict[str, ttk.Frame] = {}

        summary_group = ttk.LabelFrame(content_frame, text="配置说明", padding=16)
        summary_group.pack(fill="x", pady=(0, 10))
        ttk.Label(
            summary_group,
            text=(
                "左侧选择配置分组，右侧只显示当前分组字段。\n"
                "保存时会统一写入 backend/.env，未展示的其他分组内容也会一并保留。"
            ),
            justify="left",
        ).pack(anchor="w")

        self.config_content_host = ttk.Frame(content_frame)
        self.config_content_host.pack(fill="both", expand=True)

        config_nav_items = [
            ("应用配置", "应用配置", "设置监听地址、端口和跨域来源"),
            ("数据库配置", "数据库配置", "维护数据库连接串和账号参数"),
            ("认证配置", "认证配置", "管理 JWT 密钥和 token 生命周期"),
            ("外部系统配置", "外部系统配置", "配置 Marki 等外部系统接入信息"),
        ]
        config_nav_items.append(("git_repo", "Git 仓库配置", "统一维护仓库地址与分支，供运维和一键部署复用"))
        self.create_side_nav(
            nav_frame,
            config_nav_items,
            self.config_section_var,
            self.show_config_section,
            self.config_nav_items,
        )

        for section_title, fields in ENV_SECTIONS:
            section_frame = ttk.LabelFrame(self.config_content_host, text=section_title, padding=16)
            for row, (key, label, secret) in enumerate(fields):
                ttk.Label(section_frame, text=f"{label}：", width=18).grid(row=row, column=0, sticky="w", padx=6, pady=6)
                var = tk.StringVar(value=self.config_values.get(key, ""))
                ttk.Entry(section_frame, textvariable=var, width=88, show="*" if secret else "").grid(
                    row=row, column=1, sticky="ew", padx=6, pady=6
                )
                section_frame.columnconfigure(1, weight=1)
                self.form_vars[key] = var
            self.config_section_frames[section_title] = section_frame

        git_section = ttk.LabelFrame(self.config_content_host, text="Git 仓库配置", padding=16)
        ttk.Label(
            git_section,
            text="运维工具中的 Git 拉取更新和一键部署（Git 模式）统一使用此处配置。",
            justify="left",
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=6, pady=(0, 8))
        ttk.Label(git_section, text="仓库地址：", width=18).grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(git_section, textvariable=self.ops_vars["git_repo_url"], width=88).grid(
            row=1, column=1, sticky="ew", padx=6, pady=6
        )
        ttk.Label(git_section, text="分支名称：", width=18).grid(row=2, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(git_section, textvariable=self.ops_vars["git_branch"], width=30).grid(
            row=2, column=1, sticky="w", padx=6, pady=6
        )
        ttk.Button(git_section, text="保存 Git 配置", command=self.save_manager_state).grid(
            row=3, column=1, sticky="w", padx=6, pady=(8, 0)
        )
        git_section.columnconfigure(1, weight=1)
        self.config_section_frames["git_repo"] = git_section

        self.show_config_section()

    def build_ops_tab(self, parent: ttk.Frame) -> None:
        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True, padx=10, pady=10)

        nav_frame = tk.Frame(
            container,
            bg="#f0f0f0",
            bd=1,
            relief="flat",
        )
        nav_frame.pack(side="left", fill="y", padx=(0, 10))

        nav_title = tk.Label(
            nav_frame,
            text="运维导航",
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
            ("one_click_deploy", "一键部署", "全流程编排：检查、部署、启动、后置配置检查"),
            ("frontend", "前端部署", "覆盖发布 dist，更新页面静态资源"),
            ("release", "发布包升级", "导入 ZIP 发布包并执行一键升级"),
            ("git_update", "Git 拉取更新", "从远程 Git 仓库拉取最新代码"),
            ("migration", "数据库迁移", "执行 SQL 脚本更新数据库结构"),
            ("db_monitor", "数据库监控", "检查数据库连接状态并开启持续监控"),
            ("backup", "数据库备份", "调用 sqlcmd 执行数据库备份"),
            ("maintenance", "日志与备份维护", "清理过期日志和备份文件"),
            ("alert", "告警通知", "配置 Webhook 告警接收地址"),
            ("notes", "使用说明", "查看当前运维方式和操作建议"),
        ]

        for key in ("frontend_deploy_source", "release_package_path", "backup_dir", "sqlcmd_path", "git_repo_url", "git_branch", "backup_retention_days", "backup_retention_count", "log_max_size_mb", "log_archive_retention_days", "webhook_url"):
            if key not in self.ops_vars:
                self.ops_vars[key] = tk.StringVar(value=str(self.manager_state.get(key, DEFAULT_STATE.get(key, ""))))

        self.create_side_nav(
            nav_frame,
            ops_nav_items,
            self.ops_section_var,
            self.show_ops_section,
            self.ops_nav_items,
        )

        self.ops_content_host = ttk.Frame(content_frame)
        self.ops_content_host.pack(fill="both", expand=True)
        self.ops_section_frames: Dict[str, ttk.Frame] = {}

        one_click_frame = ttk.LabelFrame(self.ops_content_host, text="一键部署", padding=16)
        ttk.Label(
            one_click_frame,
            text="自动执行环境检查、代码部署、运行配置写入、依赖同步、服务启动、健康检查与数据库后置配置检查。",
            justify="left",
            wraplength=760,
        ).pack(anchor="w", pady=(0, 12))

        deploy_type_frame = ttk.Frame(one_click_frame)
        deploy_type_frame.pack(fill="x", pady=(0, 10))
        self.deploy_mode = tk.StringVar(value="git")
        ttk.Radiobutton(deploy_type_frame, text="Git 拉取部署", variable=self.deploy_mode, value="git").pack(side="left", padx=20)
        ttk.Radiobutton(deploy_type_frame, text="ZIP 发布包部署", variable=self.deploy_mode, value="zip").pack(side="left", padx=20)

        deploy_log_frame = ttk.LabelFrame(one_click_frame, text="部署日志", padding=10)
        deploy_log_frame.pack(fill="both", expand=True, pady=(0, 10))
        self.deploy_log_text = scrolledtext.ScrolledText(deploy_log_frame, wrap="word", font=("Consolas", 9), height=14)
        self.deploy_log_text.pack(fill="both", expand=True)
        self.deploy_log_text.configure(state="disabled")

        deploy_actions = ttk.Frame(one_click_frame)
        deploy_actions.pack(fill="x")
        ttk.Button(deploy_actions, text="开始一键部署", command=self.start_one_click_deploy).pack(side="left", padx=4)
        ttk.Button(deploy_actions, text="清空日志", command=self.clear_deploy_log).pack(side="left", padx=4)
        self.ops_section_frames["one_click_deploy"] = one_click_frame

        frontend_frame = ttk.LabelFrame(self.ops_content_host, text="前端部署", padding=16)
        frontend_top = ttk.Frame(frontend_frame)
        frontend_top.pack(fill="x", pady=(0, 10))
        
        self.ops_common_group = ttk.LabelFrame(frontend_top, text="基础路径设置", padding=16)
        self.ops_common_group.pack(fill="x", pady=(0, 10))

        path_fields = [
            ("frontend_deploy_source", "前端 dist 来源目录", "directory"),
            ("release_package_path", "发布包 ZIP 文件", "zip"),
            ("backup_dir", "数据库备份输出目录", "directory"),
            ("sqlcmd_path", "sqlcmd 可执行文件", "file"),
        ]

        for row, (key, label, select_mode) in enumerate(path_fields):
            ttk.Label(self.ops_common_group, text=f"{label}：", width=18).grid(row=row, column=0, sticky="w", padx=6, pady=6)
            ttk.Entry(self.ops_common_group, textvariable=self.ops_vars[key], width=82).grid(
                row=row, column=1, sticky="ew", padx=6, pady=6
            )
            ttk.Button(
                self.ops_common_group,
                text="选择",
                command=lambda target_key=key, mode=select_mode: self.select_path_for_var(target_key, mode),
            ).grid(row=row, column=2, padx=6, pady=6)
        self.ops_common_group.columnconfigure(1, weight=1)

        ttk.Button(self.ops_common_group, text="保存运维设置", command=self.save_manager_state).grid(
            row=len(path_fields), column=1, sticky="w", padx=6, pady=(10, 0)
        )

        ttk.Label(
            frontend_frame,
            text="将选定目录中的前端构建产物覆盖到 frontend/dist，部署后后端重启即可生效。",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        frontend_actions = ttk.Frame(frontend_frame)
        frontend_actions.pack(fill="x")
        ttk.Button(frontend_actions, text="部署前端 dist", command=self.deploy_frontend_dist).pack(side="left", padx=4)
        ttk.Button(frontend_actions, text="打开 dist 目录", command=self.open_dist_folder).pack(side="left", padx=4)
        self.ops_section_frames["frontend"] = frontend_frame

        release_frame = ttk.LabelFrame(self.ops_content_host, text="发布包升级", padding=16)
        ttk.Label(
            release_frame,
            text=(
                "支持导入 ZIP 发布包并一键升级。管理器会自动停止后端，覆盖发布包中的 backend、frontend/dist、tools、deploy 等内容，"
                "同时保留本机 backend/.env、.encryption.key、虚拟环境、日志和 manager_state。"
            ),
            justify="left",
            wraplength=760,
        ).pack(anchor="w", pady=(0, 8))
        release_actions = ttk.Frame(release_frame)
        release_actions.pack(fill="x")
        ttk.Button(release_actions, text="选择发布包", command=lambda: self.select_path_for_var("release_package_path", "zip")).pack(
            side="left", padx=4
        )
        ttk.Button(release_actions, text="一键升级发布包", command=self.apply_release_package).pack(side="left", padx=4)
        ttk.Button(release_actions, text="打开项目目录", command=self.open_project_root).pack(side="left", padx=4)
        self.ops_section_frames["release"] = release_frame

        db_monitor_frame = ttk.LabelFrame(self.ops_content_host, text="数据库连接状态与监控", padding=16)
        ttk.Label(
            db_monitor_frame,
            text="数据库检查以当前 backend/.env 为准；一键部署阶段不再前置校验数据库，而是在部署完成后执行后置配置检查。",
            justify="left",
            wraplength=760,
        ).pack(anchor="w", pady=(0, 8))
        db_monitor_grid = ttk.Frame(db_monitor_frame)
        db_monitor_grid.pack(fill="x", pady=(0, 10))
        ttk.Label(db_monitor_grid, text="数据库配置：", width=14).grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, textvariable=self.status_vars["db_config_status"]).grid(row=0, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, text="连接状态：", width=14).grid(row=1, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, textvariable=self.status_vars["db_connection_status"]).grid(row=1, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, text="监控状态：", width=14).grid(row=2, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, textvariable=self.status_vars["db_monitor_status"]).grid(row=2, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, text="最后检查：", width=14).grid(row=3, column=0, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, textvariable=self.status_vars["db_last_check_at"]).grid(row=3, column=1, sticky="w", padx=6, pady=4)
        ttk.Label(db_monitor_grid, text="说明：", width=14).grid(row=4, column=0, sticky="nw", padx=6, pady=4)
        ttk.Label(
            db_monitor_grid,
            text="启用后每 15 秒自动检查一次数据库连接；优先使用后端虚拟环境中的数据库驱动直连，若运行环境未就绪则再回退到 sqlcmd。",
            justify="left",
            wraplength=620,
        ).grid(row=4, column=1, sticky="w", padx=6, pady=4)
        db_monitor_actions = ttk.Frame(db_monitor_frame)
        db_monitor_actions.pack(fill="x")
        ttk.Button(db_monitor_actions, text="立即检查数据库连接", command=self.check_database_connection).pack(side="left", padx=4)
        ttk.Checkbutton(
            db_monitor_actions,
            text="启用数据库连接监控",
            variable=self.manager_option_vars["enable_db_monitor"],
            command=self.save_manager_state,
        ).pack(side="left", padx=12)
        self.ops_section_frames["db_monitor"] = db_monitor_frame

        backup_frame = ttk.LabelFrame(self.ops_content_host, text="数据库备份", padding=16)
        ttk.Label(
            backup_frame,
            text="使用 sqlcmd 对 SQL Server 数据库执行备份，连接信息来自当前 backend/.env。",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        backup_actions = ttk.Frame(backup_frame)
        backup_actions.pack(fill="x")
        ttk.Button(backup_actions, text="立即备份数据库", command=self.backup_database).pack(side="left", padx=4)
        ttk.Button(backup_actions, text="从备份恢复", command=self.restore_database).pack(side="left", padx=4)
        ttk.Button(backup_actions, text="打开备份目录", command=self.open_backup_folder).pack(side="left", padx=4)
        self.ops_section_frames["backup"] = backup_frame

        git_frame = ttk.LabelFrame(self.ops_content_host, text="Git 拉取更新", padding=16)
        ttk.Label(
            git_frame,
            text="从远程 Git 仓库拉取最新代码并自动部署，支持增量更新。",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        
        git_config = ttk.LabelFrame(git_frame, text="仓库配置", padding=12)
        git_config.pack(fill="x", pady=(0, 10))
        
        ttk.Label(git_config, text="仓库地址：", width=12).grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Label(git_config, textvariable=self.ops_vars["git_repo_url"]).grid(
            row=0, column=1, sticky="ew", padx=6, pady=6
        )
        
        ttk.Label(git_config, text="分支名称：", width=12).grid(row=1, column=0, sticky="w", padx=6, pady=6)
        ttk.Label(git_config, textvariable=self.ops_vars["git_branch"]).grid(
            row=1, column=1, sticky="w", padx=6, pady=6
        )
        ttk.Label(git_config, text="如需修改仓库配置，请到“配置管理”页操作。", foreground="#666666").grid(
            row=2, column=0, columnspan=2, sticky="w", padx=6, pady=(4, 0)
        )
        git_config.columnconfigure(1, weight=1)
        
        git_actions = ttk.Frame(git_frame)
        git_actions.pack(fill="x")
        ttk.Button(git_actions, text="检查更新", command=self.check_git_update).pack(side="left", padx=4)
        ttk.Button(git_actions, text="拉取并部署", command=self.git_pull_update).pack(side="left", padx=4)
        ttk.Button(git_actions, text="查看提交历史", command=self.git_show_history).pack(side="left", padx=4)
        ttk.Button(git_actions, text="回滚版本", command=self.git_rollback).pack(side="left", padx=4)
        self.ops_section_frames["git_update"] = git_frame

        migration_frame = ttk.LabelFrame(self.ops_content_host, text="数据库迁移", padding=16)
        ttk.Label(
            migration_frame,
            text="选择或粘贴 SQL 脚本，通过 sqlcmd 在目标数据库上执行。适用于 SQL Server 2016 结构变更。",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        
        migration_config = ttk.Frame(migration_frame)
        migration_config.pack(fill="x", pady=(0, 8))
        ttk.Button(migration_config, text="选择 SQL 文件", command=self.select_migration_file).pack(side="left", padx=4)
        ttk.Button(migration_config, text="清空脚本", command=self.clear_migration_script).pack(side="left", padx=4)
        
        self.migration_script_text = scrolledtext.ScrolledText(migration_frame, wrap="word", font=("Consolas", 9), height=12)
        self.migration_script_text.pack(fill="both", expand=True, pady=(0, 8))
        
        migration_actions = ttk.Frame(migration_frame)
        migration_actions.pack(fill="x")
        ttk.Button(migration_actions, text="执行迁移", command=self.execute_migration).pack(side="left", padx=4)
        self.ops_section_frames["migration"] = migration_frame

        maintenance_frame = ttk.LabelFrame(self.ops_content_host, text="日志与备份维护", padding=16)
        maint_grid = ttk.Frame(maintenance_frame)
        maint_grid.pack(fill="x", pady=(0, 10))
        
        maint_fields = [
            ("backup_retention_days", "备份保留天数", "保留最近 N 天的备份"),
            ("backup_retention_count", "备份保留数量", "最多保留 N 个备份文件"),
            ("log_max_size_mb", "日志轮转阈值(MB)", "单日志文件超过此大小自动归档"),
            ("log_archive_retention_days", "归档日志保留天数", "自动清理超过 N 天的归档文件"),
        ]
        for idx, (key, label, desc) in enumerate(maint_fields):
            ttk.Label(maint_grid, text=f"{label}：", width=18).grid(row=idx, column=0, sticky="w", padx=6, pady=4)
            ttk.Entry(maint_grid, textvariable=self.ops_vars[key], width=20).grid(row=idx, column=1, sticky="w", padx=6, pady=4)
            ttk.Label(maint_grid, text=desc, foreground="#666666").grid(row=idx, column=2, sticky="w", padx=6, pady=4)
            
        maint_actions = ttk.Frame(maintenance_frame)
        maint_actions.pack(fill="x")
        ttk.Button(maint_actions, text="立即清理过期备份", command=self.manual_cleanup_backups).pack(side="left", padx=4)
        ttk.Button(maint_actions, text="立即清理过期归档", command=self.manual_cleanup_logs).pack(side="left", padx=4)
        ttk.Button(maint_actions, text="同步后端依赖", command=self.sync_backend_deps).pack(side="left", padx=4)
        ttk.Button(maint_actions, text="同步前端依赖", command=self.sync_frontend_deps).pack(side="left", padx=4)
        self.ops_section_frames["maintenance"] = maintenance_frame

        alert_frame = ttk.LabelFrame(self.ops_content_host, text="告警通知", padding=16)
        ttk.Label(
            alert_frame,
            text="配置 Webhook 地址，在后端异常退出或健康检查失败时发送告警。支持企业微信/钉钉/飞书等标准格式。",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        alert_grid = ttk.Frame(alert_frame)
        alert_grid.pack(fill="x", pady=(0, 8))
        ttk.Label(alert_grid, text="Webhook URL：", width=12).grid(row=0, column=0, sticky="w", padx=6, pady=6)
        ttk.Entry(alert_grid, textvariable=self.ops_vars["webhook_url"], width=80).grid(row=0, column=1, sticky="ew", padx=6, pady=6)
        alert_grid.columnconfigure(1, weight=1)
        ttk.Button(alert_frame, text="发送测试告警", command=self.send_test_alert).pack(side="left", padx=4)
        self.ops_section_frames["alert"] = alert_frame

        notes_frame = ttk.LabelFrame(self.ops_content_host, text="使用说明", padding=16)
        ttk.Label(
            notes_frame,
            text=(
                "1. 前端建议先在构建机完成 npm run build，再把 dist 目录复制到服务器。\n"
                "2. 一键升级建议使用标准 ZIP 发布包，包内至少包含 backend 或 frontend/dist。\n"
                "3. 一键部署不再前置检查数据库连接，部署完成后会执行后置配置检查；数据库备份请在配置完成后单独执行。\n"
                "4. 数据库连接监控优先使用后端运行环境中的数据库驱动；sqlcmd 主要用于数据库备份、恢复和手工 SQL 执行。\n"
                "5. Git 拉取更新需要服务器已安装 Git，且仓库可访问。"
            ),
            justify="left",
        ).pack(anchor="w")
        self.ops_section_frames["notes"] = notes_frame

        self.show_ops_section()

    def build_env_tab(self, parent: ttk.Frame) -> None:
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", padx=10, pady=10)
        ttk.Button(toolbar, text="刷新环境检查", command=self.refresh_environment_info).pack(side="left", padx=4)
        ttk.Button(toolbar, text="打开 backend 目录", command=self.open_backend_folder).pack(side="left", padx=4)
        ttk.Button(toolbar, text="打开日志目录", command=self.open_logs_folder).pack(side="left", padx=4)

        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        nav_frame = tk.Frame(
            container,
            bg="#f0f0f0",
            bd=1,
            relief="flat",
        )
        nav_frame.pack(side="left", fill="y", padx=(0, 10))

        nav_title = tk.Label(
            nav_frame,
            text="检查导航",
            bg="#f0f0f0",
            fg="#333333",
            font=("Microsoft YaHei UI", 10, "bold"),
            anchor="w",
        )
        nav_title.pack(fill="x", padx=8, pady=(8, 12))

        content_frame = ttk.Frame(container)
        content_frame.pack(side="left", fill="both", expand=True)

        summary = ttk.LabelFrame(content_frame, text="自检说明", padding=16)
        summary.pack(fill="x", pady=(0, 10))
        ttk.Label(
            summary,
            text=(
                "这里集中展示部署机当前状态、运行环境、关键依赖和端口占用情况。\n"
                "建议部署前先刷新一遍，确认没有缺包、缺文件或端口冲突。"
            ),
            justify="left",
        ).pack(anchor="w")

        self.env_section_var = tk.StringVar(value="overview")
        env_nav_items = [
            ("overview", "概览", "查看项目状态、当前地址和关键文件情况"),
            ("runtime", "运行环境", "检查管理器与后端 Python 环境信息"),
            ("deps", "依赖检查", "核对关键 Python 包是否完整可导入"),
            ("paths", "路径与端口", "检查虚拟环境、前端构建和端口占用"),
        ]
        self.create_side_nav(
            nav_frame,
            env_nav_items,
            self.env_section_var,
            self.show_env_section,
            self.env_nav_items,
        )

        self.env_content_host = ttk.Frame(content_frame)
        self.env_content_host.pack(fill="both", expand=True)

        section_titles = {
            "overview": "概览",
            "runtime": "运行环境",
            "deps": "依赖检查",
            "paths": "路径与端口",
        }
        for key, title in section_titles.items():
            frame = ttk.LabelFrame(self.env_content_host, text=title, padding=12)
            text_widget = scrolledtext.ScrolledText(frame, wrap="word", font=("Consolas", 10), height=24)
            text_widget.pack(fill="both", expand=True)
            text_widget.configure(state="disabled")
            self.env_text_widgets[key] = text_widget
            self.env_section_frames[key] = frame

        self.show_env_section()
        self.refresh_environment_info()

    def build_logs_tab(self, parent: ttk.Frame) -> None:
        toolbar = ttk.Frame(parent)
        toolbar.pack(fill="x", padx=10, pady=10)

        ttk.Label(toolbar, text="日志文件：").pack(side="left")
        choices = ["后端标准输出", "后端错误输出", "前端标准输出", "前端错误输出", "项目同步日志"]
        ttk.Combobox(toolbar, state="readonly", values=choices, textvariable=self.log_choice, width=20).pack(
            side="left", padx=6
        )
        ttk.Button(toolbar, text="刷新", command=lambda: self.refresh_log_view(force=True)).pack(side="left", padx=6)
        ttk.Checkbutton(toolbar, text="自动刷新", variable=self.log_auto_refresh).pack(side="left", padx=6)
        ttk.Button(toolbar, text="清空当前日志", command=self.clear_current_log).pack(side="left", padx=6)
        ttk.Button(toolbar, text="打开归档目录", command=self.open_log_archive_folder).pack(side="left", padx=6)

        status_bar = ttk.Frame(parent)
        status_bar.pack(fill="x", padx=10, pady=(0, 8))
        ttk.Label(status_bar, textvariable=self.log_status_var, foreground="#4b5f73").pack(side="left")

        self.log_text = scrolledtext.ScrolledText(parent, wrap="none", font=("Consolas", 10))
        self.log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log_text.configure(state="disabled")

    def show_config_section(self) -> None:
        active_key = getattr(self, "config_section_var", tk.StringVar(value=ENV_SECTIONS[0][0])).get()
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
        
        if active_key in ("frontend", "release", "one_click_deploy"):
            self.ops_common_group.pack(fill="x", pady=(0, 10))
        else:
            self.ops_common_group.pack_forget()
        
        self.refresh_side_nav_styles(self.ops_nav_items, active_key)

    def show_env_section(self) -> None:
        active_key = getattr(self, "env_section_var", tk.StringVar(value="overview")).get()
        for section_key, frame in self.env_section_frames.items():
            if section_key == active_key:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
        self.refresh_side_nav_styles(self.env_nav_items, active_key)

    def set_readonly_text(self, widget: scrolledtext.ScrolledText, content: str) -> None:
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert("1.0", content)
        widget.configure(state="disabled")
        widget.see("1.0")

    def refresh_environment_info(self) -> None:
        config = self.get_effective_config()
        host = (config.get("APP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
        browser_host = resolve_browser_host(host)
        port = (config.get("APP_PORT") or "8100").strip() or "8100"
        backend_python = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
        owner = get_port_owner_info(int(port)) if port.isdigit() else {}

        manager_modules = ["tkinter", "pystray", "PIL", "cryptography"]
        manager_module_lines = []
        for name in manager_modules:
            try:
                __import__(name)
                manager_module_lines.append(f"[OK] {name}")
            except Exception as exc:
                manager_module_lines.append(f"[MISSING] {name}: {exc}")

        backend_probe = {"ok": False, "error": "未找到 backend/.venv/Scripts/python.exe"}
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
                "FinFlow 部署机概览",
                "",
                f"项目目录: {ROOT_DIR}",
                f"访问地址: {self.get_app_url()}",
                f"后端状态: {self.backend.poll_status()}",
                f"后端端口占用: {self.status_vars.get('backend_port_owner').get() if self.status_vars.get('backend_port_owner') else '未知'}",
                f"前端状态: {self.frontend.poll_status()}",
                f"前端端口占用: {self.status_vars.get('frontend_port_owner').get() if self.status_vars.get('frontend_port_owner') else '未知'}",
                f"配置文件: {'存在' if ENV_PATH.exists() else '缺失'}",
                f"加密密钥: {'存在' if KEY_PATH.exists() else '缺失'}",
                f"前端构建: {'存在' if (DIST_DIR / 'index.html').exists() else '缺失'}",
                f"日志目录: {'存在' if LOG_DIR.exists() else '缺失'}",
                f"后端健康检查: {self.status_vars.get('backend_health_status').get() if self.status_vars.get('backend_health_status') else '未检查'}",
                f"前端健康检查: {self.status_vars.get('frontend_health_status').get() if self.status_vars.get('frontend_health_status') else '未检查'}",
            ]
        )

        runtime_lines = [
            "管理器运行环境",
            "",
            f"管理器 Python: {sys.executable}",
            f"管理器版本: {sys.version}",
            f"操作系统: {platform.platform()}",
            f"TCL_LIBRARY: {os.environ.get('TCL_LIBRARY', '(未设置)')}",
            f"TK_LIBRARY: {os.environ.get('TK_LIBRARY', '(未设置)')}",
            "",
            "后端运行环境",
            "",
        ]
        if backend_probe.get("ok"):
            runtime_lines.extend(
                [
                    f"后端 Python: {backend_probe.get('python', '')}",
                    f"后端版本: {backend_probe.get('version', '')}",
                ]
            )
        else:
            runtime_lines.append(f"后端环境检查失败: {backend_probe.get('error', '未知错误')}")
        runtime_text = "\n".join(runtime_lines)

        deps_lines = [
            "管理器依赖检查",
            "",
            *manager_module_lines,
            "",
            "后端关键依赖检查",
            "",
        ]
        if backend_probe.get("ok"):
            for module_name, ok in backend_probe.get("modules", {}).items():
                deps_lines.append(f"[{'OK' if ok else 'MISSING'}] {module_name}")
        else:
            deps_lines.append(f"无法检查后端依赖: {backend_probe.get('error', '未知错误')}")
        deps_lines.append("")
        deps_lines.append("说明: 如果 `Crypto` 缺失，需要在 backend/.venv 中安装 `pycryptodome`。")
        deps_text = "\n".join(deps_lines)

        path_lines = [
            "路径与端口检查",
            "",
            f"backend/.venv: {'存在' if backend_python.exists() else '缺失'}",
            f"backend/.env: {'存在' if ENV_PATH.exists() else '缺失'}",
            f"backend/.encryption.key: {'存在' if KEY_PATH.exists() else '缺失'}",
            f"frontend/dist/index.html: {'存在' if (DIST_DIR / 'index.html').exists() else '缺失'}",
            f"日志目录: {'存在' if LOG_DIR.exists() else '缺失'}",
            f"归档目录: {'存在' if LOG_ARCHIVE_DIR.exists() else '缺失'}",
            "",
            f"监听地址配置: {host}",
            f"浏览器访问地址: {browser_host}",
            f"监听端口: {port}",
            f"端口可连接: {'是' if (port.isdigit() and is_port_open(browser_host, int(port))) else '否'}",
        ]
        if owner:
            path_lines.extend(
                [
                    f"端口占用 PID: {owner.get('pid', '')}",
                    f"端口占用状态: {build_port_owner_label(owner)}",
                ]
            )
        else:
            path_lines.append("端口占用进程: 无")
        paths_text = "\n".join(path_lines)

        self.set_readonly_text(self.env_text_widgets["overview"], overview_text)
        self.set_readonly_text(self.env_text_widgets["runtime"], runtime_text)
        self.set_readonly_text(self.env_text_widgets["deps"], deps_text)
        self.set_readonly_text(self.env_text_widgets["paths"], paths_text)

    def create_tray_icon(self) -> None:
        menu = Menu(
            MenuItem("打开管理器", lambda: self.root.after(0, self.show_window)),
            MenuItem("启动全部", lambda: self.root.after(0, self.handle_start_all)),
            MenuItem("停止全部", lambda: self.root.after(0, self.handle_stop_all)),
            MenuItem("重启全部", lambda: self.root.after(0, self.handle_restart_all)),
            MenuItem("启动后端", lambda: self.root.after(0, self.handle_start_backend)),
            MenuItem("停止后端", lambda: self.root.after(0, self.handle_stop_backend)),
            MenuItem("重启后端", lambda: self.root.after(0, self.handle_restart_backend)),
            MenuItem("启动前端", lambda: self.root.after(0, self.handle_start_frontend)),
            MenuItem("停止前端", lambda: self.root.after(0, self.handle_stop_frontend)),
            MenuItem("重启前端", lambda: self.root.after(0, self.handle_restart_frontend)),
            MenuItem("打开前端", lambda: self.root.after(0, self.open_frontend)),
            MenuItem("退出管理器", lambda: self.root.after(0, self.exit_application)),
        )
        self.tray_icon = Icon("FinFlowManager", create_tray_image(), "FinFlow 管理器", menu)
        self.tray_thread = threading.Thread(target=self.tray_icon.run, daemon=True)
        self.tray_thread.start()
        self.update_tray_title()

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
            return "已启用"
        if enabled and not exists:
            return "待写入"
        if exists:
            return "已存在脚本"
        return "未启用"

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
                messagebox.showerror("开机自启设置失败", str(exc))
            return
        if show_message:
            messagebox.showinfo("设置成功", "已更新管理器开机自启设置")

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
        try:
            self.save_config_values()
        except Exception as exc:
            messagebox.showerror("无法启动", str(exc))
            return
        self.backend.clear_restart_failure_state()
        ok, message = self.backend.start(self.get_effective_config())
        if not ok:
            messagebox.showwarning("启动结果", message)
        else:
            self.log_status_var.set(f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）")
            self.notify_tray("FinFlow 已启动", message)
        self.refresh_status()

    def handle_stop_backend(self) -> None:
        _, message = self.backend.stop()
        self.backend.clear_restart_failure_state()
        self.last_health_state = "unknown"
        self.refresh_status()
        messagebox.showinfo("停止结果", message)

    def handle_restart_backend(self) -> None:
        try:
            self.save_config_values()
        except Exception as exc:
            messagebox.showerror("无法重启", str(exc))
            return
        self.backend.clear_restart_failure_state()
        ok, message = self.backend.restart(self.get_effective_config())
        self.last_health_state = "unknown"
        self.refresh_status()
        if ok:
            self.log_status_var.set(f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）")
            self.notify_tray("FinFlow 已重启", message)
        messagebox.showinfo("重启结果", message)

    def handle_start_frontend(self, show_dialog: bool = True) -> Tuple[bool, str]:
        try:
            self.save_config_values()
        except Exception as exc:
            if show_dialog:
                messagebox.showerror("无法启动", str(exc))
            return False, str(exc)
        self.frontend.clear_restart_failure_state()
        ok, message = self.frontend.start(self.get_effective_config())
        if not ok:
            if show_dialog:
                messagebox.showwarning("启动结果", message)
        else:
            self.log_status_var.set(f"当前显示：本次启动日志（前端，自 {self.frontend.last_session_started_label} 起）")
            self.notify_tray("前端已启动", message)
        self.refresh_status()
        return ok, message

    def handle_stop_frontend(self, show_dialog: bool = True) -> Tuple[bool, str]:
        ok, message = self.frontend.stop()
        self.frontend.clear_restart_failure_state()
        self.last_frontend_health_state = "unknown"
        self.refresh_status()
        if show_dialog:
            messagebox.showinfo("停止结果", message)
        return ok, message

    def handle_restart_frontend(self, show_dialog: bool = True) -> Tuple[bool, str]:
        try:
            self.save_config_values()
        except Exception as exc:
            if show_dialog:
                messagebox.showerror("无法重启", str(exc))
            return False, str(exc)
        self.frontend.clear_restart_failure_state()
        ok, message = self.frontend.restart(self.get_effective_config())
        self.last_frontend_health_state = "unknown"
        self.refresh_status()
        if ok:
            self.log_status_var.set(f"当前显示：本次启动日志（前端，自 {self.frontend.last_session_started_label} 起）")
            self.notify_tray("前端已重启", message)
        if show_dialog:
            messagebox.showinfo("重启结果", message)
        return ok, message

    def handle_start_all(self, show_dialog: bool = True) -> Tuple[bool, str]:
        backend_started = False
        backend_message = ""
        try:
            self.save_config_values()
            self.backend.clear_restart_failure_state()
            backend_started, backend_message = self.backend.start(self.get_effective_config())
        except Exception as exc:
            backend_message = str(exc)
        if not backend_started and self.backend.is_running():
            backend_started = True
            backend_message = backend_message or "后端已在运行"

        frontend_ok = False
        frontend_message = "后端启动失败，未继续启动前端"
        if backend_started:
            frontend_ok, frontend_message = self.handle_start_frontend(show_dialog=False)
        self.refresh_status()
        message = f"后端：{backend_message}\n前端：{frontend_message}"
        if show_dialog:
            messagebox.showinfo("启动结果", message)
        return backend_started and frontend_ok, message

    def handle_stop_all(self, show_dialog: bool = True) -> Tuple[bool, str]:
        frontend_ok, frontend_message = self.handle_stop_frontend(show_dialog=False)
        backend_ok = False
        backend_message = ""
        try:
            backend_ok, backend_message = self.backend.stop()
        except Exception as exc:
            backend_message = str(exc)
        self.backend.clear_restart_failure_state()
        self.refresh_status()
        message = f"前端：{frontend_message}\n后端：{backend_message}"
        if show_dialog:
            messagebox.showinfo("停止结果", message)
        return frontend_ok or backend_ok, message

    def handle_restart_all(self, show_dialog: bool = True) -> Tuple[bool, str]:
        self.handle_stop_all(show_dialog=False)
        time.sleep(1)
        ok, message = self.handle_start_all(show_dialog=False)
        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        if show_dialog:
            messagebox.showinfo("重启结果", message)
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
            self.refresh_status()
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
            self.refresh_status()

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
        self.save_manager_state()
        
        if not self.check_git_available():
            messagebox.showerror("错误", "未找到 Git 命令，请先安装 Git 并添加到系统 PATH")
            return
        
        repo_url = self.ops_vars["git_repo_url"].get().strip()
        if not repo_url:
            messagebox.showerror("错误", "请先配置 Git 仓库地址")
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
            
            if result.returncode != 0:
                messagebox.showerror("检查失败", f"无法访问仓库或分支不存在：\n{result.stderr.strip()}")
                return
            
            if not result.stdout.strip():
                messagebox.showinfo("检查结果", f"分支 '{branch}' 不存在于仓库中")
                return
            
            messagebox.showinfo("检查结果", f"分支 '{branch}' 存在，可以拉取更新")
            
        except Exception as exc:
            messagebox.showerror("检查失败", f"检查更新时出错：{exc}")

    def git_pull_update(self) -> None:
        self.save_manager_state()
        
        if not self.check_git_available():
            messagebox.showerror("错误", "未找到 Git 命令，请先安装 Git 并添加到系统 PATH")
            return
        
        repo_url = self.ops_vars["git_repo_url"].get().strip()
        if not repo_url:
            messagebox.showerror("错误", "请先配置 Git 仓库地址")
            return
        
        branch = self.ops_vars["git_branch"].get().strip() or "main"
        
        if not (ROOT_DIR / ".git").exists():
            proceed = messagebox.askyesno(
                "初始化 Git 仓库",
                "当前项目目录没有 Git 仓库，是否初始化并从远程仓库克隆？\n注意：这会覆盖本地的 backend、frontend 等目录。",
            )
            if not proceed:
                return
            
            try:
                subprocess.run(
                    ["git", "clone", "--branch", branch, repo_url, str(ROOT_DIR)],
                    capture_output=True,
                    text=True,
                    timeout=300,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                messagebox.showinfo("成功", "Git 仓库初始化完成，代码已克隆")
                self.refresh_status()
                return
            except Exception as exc:
                messagebox.showerror("失败", f"克隆仓库失败：{exc}")
                return
        
        backend_was_running = self.backend.is_running()
        frontend_was_running = self.frontend.is_running()
        
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
                detail = (result.stderr or result.stdout or "未知错误").strip()
                messagebox.showerror("拉取失败", f"Git pull 失败：\n{detail}")
                return
            
            stdout_text = result.stdout.strip()
            if "Already up to date" in stdout_text:
                messagebox.showinfo("结果", "代码已是最新，无需更新")
                return
            
            messagebox.showinfo("成功", f"代码已更新：\n{stdout_text}")
            
            self.refresh_status()
            self.notify_tray("Git 更新完成", "代码已从远程仓库拉取")
            
        except Exception as exc:
            messagebox.showerror("失败", f"拉取更新时出错：{exc}")
        finally:
            if backend_was_running and not self.backend.is_running():
                self.backend.start(self.get_effective_config())
            if frontend_was_running and not self.frontend.is_running():
                self.frontend.start(self.get_effective_config())

    def git_show_history(self) -> None:
        self.save_manager_state()
        
        if not self.check_git_available():
            messagebox.showerror("错误", "未找到 Git 命令，请先安装 Git 并添加到系统 PATH")
            return
        
        repo_url = self.ops_vars["git_repo_url"].get().strip()
        if not repo_url:
            messagebox.showerror("错误", "请先配置 Git 仓库地址")
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
            
            if result.returncode != 0:
                messagebox.showinfo("提示", "本地仓库暂无提交历史")
                return
            
            history = result.stdout.strip()
            if not history:
                messagebox.showinfo("提示", "本地仓库暂无提交历史")
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
            history_window.title("Git 提交历史")
            history_window.geometry("600x400")
            
            text_widget = scrolledtext.ScrolledText(history_window, wrap="word", font=("Consolas", 10))
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)
            text_widget.insert("1.0", f"仓库目录：{repo_dir}\n\n{history}")
            text_widget.configure(state="disabled")
            
            ttk.Button(history_window, text="关闭", command=history_window.destroy).pack(pady=10)
            
        except Exception as exc:
            messagebox.showerror("错误", f"查看提交历史失败：{exc}")

    def git_rollback(self) -> None:
        self.save_manager_state()
        
        if not self.check_git_available():
            messagebox.showerror("错误", "未找到 Git 命令，请先安装 Git 并添加到系统 PATH")
            return
        
        if not (ROOT_DIR / ".git").exists():
            messagebox.showerror("错误", "当前项目目录没有 Git 仓库")
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
            
            if result.returncode != 0:
                messagebox.showinfo("提示", "本地仓库暂无提交历史")
                return
            
            history = result.stdout.strip()
            if not history:
                messagebox.showinfo("提示", "本地仓库暂无提交历史")
                return
            
            commits = []
            for line in history.split("\n"):
                if " " in line:
                    commit_hash = line.split(" ", 1)[0]
                    message = line.split(" ", 1)[1]
                    commits.append((commit_hash, message))
            
            if not commits:
                messagebox.showinfo("提示", "本地仓库暂无提交历史")
                return
            
            rollback_window = tk.Toplevel(self.root)
            rollback_window.title("选择回滚版本")
            rollback_window.geometry("500x400")
            
            ttk.Label(rollback_window, text="选择要回滚到的版本：").pack(pady=10)
            
            listbox = tk.Listbox(rollback_window, height=15, font=("Consolas", 10))
            listbox.pack(fill="both", expand=True, padx=10, pady=10)
            
            for commit_hash, message in commits:
                listbox.insert("end", f"{commit_hash} {message}")
            
            ttk.Button(rollback_window, text="确定回滚", command=lambda: self.execute_rollback(listbox, rollback_window)).pack(pady=10)
            
        except Exception as exc:
            messagebox.showerror("错误", f"获取提交历史失败：{exc}")

    def execute_rollback(self, listbox: tk.Listbox, window: tk.Toplevel) -> None:
        selection = listbox.curselection()
        if not selection:
            messagebox.showwarning("警告", "请选择一个版本")
            return
        
        selected = listbox.get(selection[0])
        commit_hash = selected.split(" ")[0]
        
        proceed = messagebox.askyesno(
            "确认回滚",
            f"确定要回滚到版本 {commit_hash} 吗？\n\n注意：这会覆盖本地的代码变更。",
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
                messagebox.showerror("回滚失败", f"Git reset 失败：\n{result.stderr.strip()}")
                return
            
            messagebox.showinfo("成功", f"已回滚到版本 {commit_hash}")
            
            self.refresh_status()
            self.notify_tray("Git 回滚完成", f"已回滚到版本 {commit_hash}")
            
        except Exception as exc:
            messagebox.showerror("失败", f"回滚时出错：{exc}")
        finally:
            if backend_was_running and not self.backend.is_running():
                self.backend.start(self.get_effective_config())
            if frontend_was_running and not self.frontend.is_running():
                self.frontend.start(self.get_effective_config())
            window.destroy()

    def select_migration_file(self) -> None:
        path = filedialog.askopenfilename(title="选择 SQL 脚本", filetypes=[("SQL 文件", "*.sql"), ("所有文件", "*.*")])
        if path:
            try:
                content = Path(path).read_text(encoding="utf-8")
                self.migration_script_text.configure(state="normal")
                self.migration_script_text.delete("1.0", tk.END)
                self.migration_script_text.insert("1.0", content)
                self.migration_script_text.configure(state="disabled")
            except Exception as exc:
                messagebox.showerror("读取失败", f"无法读取 SQL 文件：{exc}")

    def clear_migration_script(self) -> None:
        self.migration_script_text.configure(state="normal")
        self.migration_script_text.delete("1.0", tk.END)
        self.migration_script_text.configure(state="disabled")

    def execute_migration(self) -> None:
        self.save_manager_state()
        sql_content = self.migration_script_text.get("1.0", tk.END).strip()
        if not sql_content:
            messagebox.showwarning("提示", "请先输入或选择 SQL 脚本内容")
            return
        
        sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
        config = self.get_effective_config()
        
        proceed = messagebox.askyesno("确认执行", "确定要执行此 SQL 脚本吗？\n请确保脚本内容正确，执行后将直接修改数据库。")
        if not proceed:
            return
            
        ok, detail = execute_sql_script_via_sqlcmd(sql_content, config, sqlcmd)
        if ok:
            messagebox.showinfo("执行成功", detail)
            self.notify_tray("数据库迁移成功", "SQL 脚本已执行")
        else:
            messagebox.showerror("执行失败", detail)

    def append_deploy_log(self, message: str) -> None:
        self.deploy_log_text.configure(state="normal")
        self.deploy_log_text.insert("end", f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")
        self.deploy_log_text.see("end")
        self.deploy_log_text.configure(state="disabled")
        self.root.update()

    def clear_deploy_log(self) -> None:
        self.deploy_log_text.configure(state="normal")
        self.deploy_log_text.delete("1.0", tk.END)
        self.deploy_log_text.configure(state="disabled")

    def start_one_click_deploy(self) -> None:
        self.save_manager_state()
        self.clear_deploy_log()
        
        mode = self.deploy_mode.get()
        repo_url = self.ops_vars["git_repo_url"].get().strip()
        branch = self.ops_vars["git_branch"].get().strip() or "main"
        sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
        config = self.get_effective_config()
        
        if mode == "git" and not repo_url:
            messagebox.showerror("错误", "Git 部署模式需要先配置仓库地址")
            return
        if mode == "zip":
            pkg = Path(self.ops_vars["release_package_path"].get().strip())
            if not pkg.exists():
                messagebox.showerror("错误", "ZIP 部署模式需要先选择发布包文件")
                return
        
        proceed = messagebox.askyesno(
            "确认一键部署",
            "一键部署将自动执行以下流程：\n"
            "1. 环境检查（Git/Python/Node/sqlcmd）\n"
            "2. 停止前后端服务\n"
            "3. 代码部署（Git 拉取 或 ZIP 升级）\n"
            "4. 依赖同步（后端 + 前端）\n"
            "5. 运行配置写入、前端构建与运行配置同步\n"
            "6. 启动后端服务\n"
            "7. 启动前端服务\n"
            "8. 前后端健康检查\n"
            "9. 数据库后置配置检查（如未配置则提示待处理）\n\n"
            "是否继续？"
        )
        if not proceed:
            return
        
        self.root.config(cursor="watch")
        self.root.update()
        
        try:
            self._run_one_click_deploy(mode, repo_url, branch, sqlcmd, config)
        except RuntimeError:
            pass
        except Exception as exc:
            self.append_deploy_log(f"部署异常: {exc}")
        finally:
            self.root.config(cursor="")

    def _run_one_click_deploy(self, mode: str, repo_url: str, branch: str, sqlcmd: str, config: Dict[str, str]) -> None:
        steps_passed = 0
        total_steps = 9
        backend_was_running = self.backend.is_running()
        frontend_was_running = self.frontend.is_running()
        services_stopped = False
        backend_started_after_deploy = False
        frontend_started_after_deploy = False
        
        self.append_deploy_log(f"{'='*50}")
        self.append_deploy_log(f"开始一键部署 (模式: {'Git 拉取' if mode == 'git' else 'ZIP 发布包'})")
        self.append_deploy_log(f"{'='*50}")

        def abort_deploy(reason: str) -> None:
            restore_messages: List[str] = []

            if frontend_started_after_deploy and not frontend_was_running and self.frontend.is_running():
                ok, msg = self.frontend.stop()
                restore_messages.append(f"清理本次启动前端：{msg}")
            if backend_started_after_deploy and not backend_was_running and self.backend.is_running():
                ok, msg = self.backend.stop()
                restore_messages.append(f"清理本次启动后端：{msg}")

            if services_stopped:
                if backend_was_running and not self.backend.is_running():
                    ok, msg = self.backend.start(config)
                    restore_messages.append(f"恢复后端：{msg}")
                if frontend_was_running and not self.frontend.is_running():
                    ok, msg = self.frontend.start(config)
                    restore_messages.append(f"恢复前端：{msg}")

            self.last_backend_health_state = "unknown"
            self.last_frontend_health_state = "unknown"
            self.refresh_status()

            detail = reason
            if restore_messages:
                detail += "\n\n恢复结果：\n" + "\n".join(restore_messages)
            self.append_deploy_log(f"[FAIL] {reason}")
            messagebox.showerror("一键部署失败", detail)
            raise RuntimeError(reason)
        
        # Step 1: 环境检查与自动完善
        self.append_deploy_log(f"[1/{total_steps}] 环境检查...")
        
        # 检查 Python
        python_exe = BACKEND_DIR / ".venv" / "Scripts" / "python.exe"
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
                self.append_deploy_log(f"  [OK] 系统 Python 已安装: {result.stdout.strip()}")
        except Exception:
            self.append_deploy_log("  [FAIL] 系统未安装 Python，请先安装 Python 3.9+")
            messagebox.showerror("环境缺失", "请先安装 Python 3.9 或更高版本\n下载地址：https://www.python.org/downloads/")
            return
        
        # 检查并创建虚拟环境
        if not python_exe.exists():
            self.append_deploy_log("  正在创建后端虚拟环境...")
            try:
                result = subprocess.run(
                    [system_python, "-m", "venv", str(BACKEND_DIR / ".venv")],
                    cwd=BACKEND_DIR,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if result.returncode == 0:
                    self.append_deploy_log("  [OK] 后端虚拟环境已创建")
                else:
                    error_msg = result.stderr or result.stdout or "创建失败"
                    self.append_deploy_log(f"  [FAIL] 创建虚拟环境失败: {error_msg}")
                    return
            except Exception as exc:
                self.append_deploy_log(f"  [FAIL] 创建虚拟环境异常: {exc}")
                return
        else:
            self.append_deploy_log("  [OK] 后端虚拟环境已存在")
        
        # 升级虚拟环境中的 pip
        self.append_deploy_log("  正在升级虚拟环境 pip...")
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
                self.append_deploy_log("  [OK] pip 升级成功")
            else:
                self.append_deploy_log(f"  [WARN] pip 升级失败: {upgrade_result.stderr or upgrade_result.stdout}")
        except Exception as exc:
            self.append_deploy_log(f"  [WARN] pip 升级异常: {exc}")
        
        # 检查 Git
        if not self.check_git_available():
            self.append_deploy_log("  [FAIL] Git 未安装")
            messagebox.showerror("环境缺失", "请先安装 Git 并添加到系统 PATH\n下载地址：https://git-scm.com/download/win")
            return
        self.append_deploy_log("  [OK] Git 可用")
        
        # 检查并初始化 Git 仓库
        git_dir = ROOT_DIR / ".git"
        if not git_dir.exists():
            if mode == "git" and repo_url:
                self.append_deploy_log("  正在初始化 Git 仓库...")
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
                        self.append_deploy_log("  [OK] Git 仓库已初始化")
                        
                        # 添加远程仓库
                        result = subprocess.run(
                            ["git", "remote", "add", "origin", repo_url],
                            cwd=ROOT_DIR,
                            capture_output=True,
                            text=True,
                            timeout=30,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                        )
                        if result.returncode == 0:
                            self.append_deploy_log("  [OK] 远程仓库已配置")
                        else:
                            self.append_deploy_log(f"  [WARN] 添加远程仓库失败: {result.stderr}")
                    else:
                        self.append_deploy_log(f"  [FAIL] Git 初始化失败: {result.stderr}")
                        return
                except Exception as exc:
                    self.append_deploy_log(f"  [FAIL] Git 初始化异常: {exc}")
                    return
            else:
                self.append_deploy_log("  [WARN] Git 仓库未初始化，且未配置远程仓库地址")
        else:
            self.append_deploy_log("  [OK] Git 仓库已存在")
        
        # 检查 sqlcmd
        try:
            subprocess.run([sqlcmd, "-?"], capture_output=True, timeout=5, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            self.append_deploy_log("  [OK] sqlcmd 可用")
        except Exception:
            self.append_deploy_log("  [WARN] sqlcmd 不可用，数据库相关操作将跳过")
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
                self.append_deploy_log(f"  [OK] Node.js 已安装: {node_result.stdout.strip()}")
            else:
                self.append_deploy_log("  [FAIL] Node.js 不可用")
                messagebox.showerror("环境缺失", "请先安装 Node.js 18+ 后再执行一键部署")
                return
        except Exception:
            self.append_deploy_log("  [FAIL] Node.js 未安装")
            messagebox.showerror("环境缺失", "请先安装 Node.js 18+ 后再执行一键部署")
            return
        
        steps_passed += 1
        
        # Step 2: 停止前后端服务
        self.append_deploy_log(f"[2/{total_steps}] 停止前后端服务...")
        if frontend_was_running:
            ok, message = self.frontend.stop()
            self.append_deploy_log(f"  [{'OK' if ok else 'WARN'}] 前端: {message}")
        else:
            self.append_deploy_log("  [SKIP] 前端未运行")
        if backend_was_running:
            ok, message = self.backend.stop()
            self.append_deploy_log(f"  [{'OK' if ok else 'WARN'}] 后端: {message}")
        else:
            self.append_deploy_log("  [SKIP] 后端未运行")
        services_stopped = True
        steps_passed += 1
        
        # Step 3: 代码部署
        self.append_deploy_log(f"[3/{total_steps}] 代码部署...")
        
        if mode == "git":
            if not (ROOT_DIR / ".git").exists():
                self.append_deploy_log("  初始化 Git 仓库...")
                temp_clone_dir = Path(tempfile.mkdtemp(prefix="finflow_git_clone_"))
                try:
                    result = subprocess.run(
                        ["git", "clone", "--branch", branch, "--depth", "1", repo_url, str(temp_clone_dir)],
                        capture_output=True, text=True, timeout=300,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    if result.returncode != 0:
                        abort_deploy(f"Git 克隆失败: {result.stderr.strip()}")
                    
                    backup_root = UPGRADE_BACKUP_DIR / datetime.now().strftime("%Y%m%d_%H%M%S_oneclick")
                    for item_name in ("backend", "frontend", "tools", "deploy"):
                        src = temp_clone_dir / item_name
                        if src.exists():
                            overlay_directory(src, ROOT_DIR / item_name, backup_root)
                    
                    self.append_deploy_log(f"  [OK] 代码已克隆并覆盖")
                finally:
                    shutil.rmtree(temp_clone_dir, ignore_errors=True)
            else:
                diff_result = subprocess.run(
                    ["git", "diff", "--quiet"], cwd=ROOT_DIR, capture_output=True, timeout=10,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if diff_result.returncode != 0:
                    subprocess.run(
                        ["git", "stash", "push", "-m", f"One-click deploy auto-stash {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                        cwd=ROOT_DIR, capture_output=True, text=True, timeout=30,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    self.append_deploy_log("  已暂存本地变更")
                
                fetch_result = subprocess.run(
                    ["git", "fetch", "origin", branch], cwd=ROOT_DIR, capture_output=True, text=True, timeout=60,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if fetch_result.returncode != 0:
                    abort_deploy(f"Git fetch 失败: {fetch_result.stderr.strip()}")
                
                pull_result = subprocess.run(
                    ["git", "pull", "origin", branch], cwd=ROOT_DIR, capture_output=True, text=True, timeout=300,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                if pull_result.returncode != 0:
                    abort_deploy(f"Git pull 失败: {pull_result.stderr.strip()}")
                
                if "Already up to date" in pull_result.stdout:
                    self.append_deploy_log("  [OK] 代码已是最新")
                else:
                    self.append_deploy_log(f"  [OK] 代码已更新")
        else:
            package_path = Path(self.ops_vars["release_package_path"].get().strip())
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
                
                self.append_deploy_log(f"  [OK] ZIP 发布包已应用")
            finally:
                shutil.rmtree(extract_dir, ignore_errors=True)
        
        steps_passed += 1
        
        # Step 4: 依赖同步
        self.append_deploy_log(f"[4/{total_steps}] 依赖同步...")
        backend_dep_ok, backend_dep_msg = sync_backend_dependencies()
        if backend_dep_ok:
            self.append_deploy_log(f"  [OK] 后端依赖: {backend_dep_msg}")
        else:
            self.append_deploy_log(f"  [WARN] 后端依赖: {backend_dep_msg}")

        frontend_dep_ok, frontend_dep_msg = sync_frontend_dependencies()
        if frontend_dep_ok:
            self.append_deploy_log(f"  [OK] 前端依赖: {frontend_dep_msg}")
        else:
            abort_deploy(f"前端依赖同步失败: {frontend_dep_msg}")
        steps_passed += 1
        
        # Step 5: 运行配置写入、前端构建与运行配置同步
        self.append_deploy_log(f"[5/{total_steps}] 运行配置写入、前端构建与运行配置同步...")
        try:
            self.save_config_values()
            config = self.get_effective_config()
            self.append_deploy_log("  [OK] 后端运行配置已写入 backend/.env")
        except Exception as exc:
            abort_deploy(f"运行配置写入失败: {exc}")
        fe_ok, fe_msg = build_frontend()
        if fe_ok:
            self.append_deploy_log(f"  [OK] {fe_msg}")
        else:
            self.append_deploy_log(f"  [WARN] {fe_msg}")
        key_ok, key_msg = sync_keys_to_frontend(config)
        if key_ok:
            self.append_deploy_log(f"  [OK] 前端运行配置: {key_msg}")
        else:
            abort_deploy(f"前端运行配置同步失败: {key_msg}")
        steps_passed += 1
        
        # Step 6: 启动后端服务
        self.append_deploy_log(f"[6/{total_steps}] 启动后端服务...")
        ok, msg = self.backend.start(config)
        if ok or self.backend.is_running():
            backend_started_after_deploy = True
            self.append_deploy_log(f"  [OK] {msg}")
        else:
            abort_deploy(f"后端启动失败: {msg}")
        steps_passed += 1
        
        self.append_deploy_log("  等待后端服务启动...")
        time.sleep(3)

        # Step 7: 启动前端服务
        self.append_deploy_log(f"[7/{total_steps}] 启动前端服务...")
        ok, msg = self.frontend.start(config)
        if ok or self.frontend.is_running():
            frontend_started_after_deploy = True
            self.append_deploy_log(f"  [OK] {msg}")
        else:
            abort_deploy(f"前端启动失败: {msg}")
        steps_passed += 1
        self.append_deploy_log("  等待前端服务启动...")
        time.sleep(2)
        
        # Step 8: 健康检查
        self.append_deploy_log(f"[8/{total_steps}] 健康检查...")
        host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
        port = config.get("APP_PORT", "8100")
        backend_url = f"http://{host}:{port}"
        frontend_url = resolve_frontend_service_settings(config)["frontend_url"]
        backend_health_ok, backend_health_detail = probe_backend_health(backend_url)
        if backend_health_ok:
            self.append_deploy_log(f"  [OK] 后端健康检查: {backend_health_detail}")
        else:
            self.append_deploy_log(f"  [WARN] 后端健康检查: {backend_health_detail}")
        frontend_health_ok, frontend_health_detail = probe_http(frontend_url)
        if frontend_health_ok:
            self.append_deploy_log(f"  [OK] 前端健康检查: {frontend_health_detail}")
        else:
            self.append_deploy_log(f"  [WARN] 前端健康检查: {frontend_health_detail}")
        steps_passed += 1

        # Step 9: 数据库后置配置检查
        self.append_deploy_log(f"[9/{total_steps}] 数据库后置配置检查...")
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
        
        self.append_deploy_log(f"{'='*50}")
        self.append_deploy_log(f"一键部署完成! 通过 {steps_passed}/{total_steps} 个检查点")
        self.append_deploy_log(f"后端地址: {backend_url}")
        self.append_deploy_log(f"前端地址: {frontend_url}")
        self.append_deploy_log(f"{'='*50}")
        
        self.last_backend_health_state = "unknown"
        self.last_frontend_health_state = "unknown"
        self.refresh_status()
        deploy_title = "一键部署完成"
        deploy_notice = f"通过 {steps_passed}/{total_steps} 个检查点"
        deploy_lines = [
            f"部署成功！通过 {steps_passed}/{total_steps} 个检查点。",
            f"后端地址：{backend_url}",
            f"前端地址：{frontend_url}",
        ]
        if not backend_health_ok or not frontend_health_ok:
            deploy_title = "一键部署完成（需关注）"
            deploy_notice = "服务已启动，但健康检查存在告警"
            if not backend_health_ok:
                deploy_lines.append(f"后端健康检查：异常，{backend_health_detail}")
            if not frontend_health_ok:
                deploy_lines.append(f"前端健康检查：异常，{frontend_health_detail}")

        if db_state == "ok":
            deploy_lines.append(f"数据库后置检查：正常，{db_detail}")
        elif db_state in {"missing_env", "missing_config", "missing_sqlcmd", "missing_runtime"}:
            if deploy_title == "一键部署完成":
                deploy_title = "一键部署完成（待补配置）"
                deploy_notice = "服务已部署完成，数据库仍需后置配置"
            deploy_lines.append(f"数据库后置检查：待处理，{db_detail}")
        else:
            deploy_title = "一键部署完成（数据库检查告警）"
            deploy_notice = "服务已部署完成，但数据库后置检查失败"
            deploy_lines.append(f"数据库后置检查：异常，{db_detail}")

        self.notify_tray(deploy_title, deploy_notice)
        if deploy_title == "一键部署完成":
            messagebox.showinfo(deploy_title, "\n".join(deploy_lines))
        else:
            messagebox.showwarning(deploy_title, "\n".join(deploy_lines))

    def manual_cleanup_backups(self) -> None:
        self.save_manager_state()
        backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
        retention_days = int(self.ops_vars["backup_retention_days"].get().strip() or 30)
        retention_count = int(self.ops_vars["backup_retention_count"].get().strip() or 10)
        
        removed, msg = cleanup_old_backups(backup_dir, retention_days, retention_count)
        messagebox.showinfo("清理完成", msg)

    def manual_cleanup_logs(self) -> None:
        self.save_manager_state()
        retention_days = int(self.ops_vars["log_archive_retention_days"].get().strip() or 90)
        
        removed, msg = cleanup_old_archived_logs(retention_days)
        messagebox.showinfo("清理完成", msg)

    def send_test_alert(self) -> None:
        self.save_manager_state()
        webhook_url = self.ops_vars["webhook_url"].get().strip()
        if not webhook_url:
            messagebox.showwarning("提示", "请先配置 Webhook URL")
            return
            
        ok = send_webhook_notification(webhook_url, "测试告警", "这是一条来自 FinFlow 管理器的测试告警消息。")
        if ok:
            messagebox.showinfo("发送成功", "测试告警已发送，请检查接收端。")
        else:
            messagebox.showerror("发送失败", "Webhook 发送失败，请检查 URL 和网络连接。")

    def sync_backend_deps(self) -> None:
        proceed = messagebox.askyesno("确认同步", "将使用 pip 安装 backend/requirements.txt 中的所有依赖。\n这可能需要几分钟，是否继续？")
        if not proceed:
            return
        
        self.log_status_var.set("当前显示：正在同步后端依赖...")
        ok, detail = sync_backend_dependencies()
        if ok:
            messagebox.showinfo("同步成功", detail)
            self.notify_tray("依赖同步成功", "后端 Python 依赖已更新")
        else:
            messagebox.showerror("同步失败", detail)

    def sync_frontend_deps(self) -> None:
        proceed = messagebox.askyesno("确认同步", "将使用 npm install 安装 frontend/package.json 中的所有依赖。\n这可能需要几分钟，是否继续？")
        if not proceed:
            return

        self.log_status_var.set("当前显示：正在同步前端依赖...")
        ok, detail = sync_frontend_dependencies()
        if ok:
            messagebox.showinfo("同步成功", detail)
            self.notify_tray("依赖同步成功", "前端 Node 依赖已更新")
        else:
            messagebox.showerror("同步失败", detail)

    def restore_database(self) -> None:
        self.save_manager_state()
        sqlcmd = self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd"
        backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
        
        bak_files = sorted(backup_dir.glob("*.bak"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not bak_files:
            messagebox.showerror("恢复失败", f"备份目录中未找到 .bak 文件：{backup_dir}")
            return
        
        file_list = [f"{f.name} ({f.stat().st_size / (1024*1024):.1f} MB)" for f in bak_files[:10]]
        choice_window = tk.Toplevel(self.root)
        choice_window.title("选择恢复文件")
        choice_window.geometry("500x350")
        
        ttk.Label(choice_window, text="选择要恢复的备份文件：").pack(pady=10)
        listbox = tk.Listbox(choice_window, height=12, font=("Consolas", 9))
        listbox.pack(fill="both", expand=True, padx=10, pady=5)
        for item in file_list:
            listbox.insert("end", item)
        
        def do_restore():
            sel = listbox.curselection()
            if not sel:
                messagebox.showwarning("警告", "请选择一个备份文件")
                return
            selected_file = bak_files[sel[0]]
            proceed = messagebox.askyesno("确认恢复", f"确定要从以下备份恢复数据库吗？\n{selected_file.name}\n\n警告：这将覆盖当前数据库中的所有数据！")
            if not proceed:
                return
            
            ok, detail = restore_database_from_backup(self.get_effective_config(), selected_file, sqlcmd)
            choice_window.destroy()
            if ok:
                messagebox.showinfo("恢复成功", f"数据库已从 {selected_file.name} 恢复")
                self.notify_tray("数据库恢复成功", f"已从 {selected_file.name} 恢复")
            else:
                messagebox.showerror("恢复失败", detail)
        
        ttk.Button(choice_window, text="确认恢复", command=do_restore).pack(pady=10)

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
        for job_name in ("status_job", "log_job", "health_job", "db_job"):
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
        backend_status = self.backend.poll_status()
        frontend_status = self.frontend.poll_status()
        config = self.get_effective_config()
        backend_port = (config.get("APP_PORT") or "8100").strip() or "8100"
        frontend_settings = resolve_frontend_service_settings(config)
        frontend_port = frontend_settings["frontend_port"]
        backend_owner_text = "未占用"
        frontend_owner_text = "未占用"
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
        self.status_vars["env_file"].set("已存在" if ENV_PATH.exists() else "缺失")
        self.status_vars["key_file"].set("已存在" if KEY_PATH.exists() else "缺失")
        self.status_vars["dist_dir"].set("已存在" if (DIST_DIR / "index.html").exists() else "缺失")
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

        if (
            self.backend.last_exit_at > self.last_notified_exit_at
            and not self.backend.user_stopped
            and self.backend.last_exit_code is not None
        ):
            self.last_notified_exit_at = self.backend.last_exit_at
            fast_failed = self.backend.register_failed_start_if_needed()
            self.notify_tray("后端异常退出", f"退出码：{self.backend.last_exit_code}")
            webhook_url = self.manager_state.get("webhook_url", "")
            send_webhook_notification(webhook_url, "后端异常退出", f"退出码：{self.backend.last_exit_code}")
            if self.backend.auto_restart_suppressed:
                self.status_vars["backend_status"].set(
                    f"启动失败过多，已暂停自动拉起 (最近退出码 {self.backend.last_exit_code})"
                )
                self.notify_tray("已暂停自动拉起", "后端连续快速失败 3 次，请先修复依赖或配置后再手动启动")
                send_webhook_notification(webhook_url, "已暂停自动拉起", "后端连续快速失败 3 次")
            elif fast_failed:
                self.status_vars["backend_status"].set(
                    f"启动后快速退出，等待自动重试 ({self.backend.consecutive_failed_starts}/3)"
                )

        if (
            self.manager_state.get("auto_restart_backend", True)
            and not self.backend.is_running()
            and not self.backend.user_stopped
            and not self.backend.auto_restart_suppressed
            and time.time() - self.backend.last_start_attempt > 8
            and self.backend.last_start_attempt > 0
        ):
            ok, message = self.backend.start(self.get_effective_config())
            if ok:
                self.notify_tray("后端已自动拉起", message)
                self.status_vars["backend_status"].set(self.backend.poll_status())
            else:
                if "端口" in message and "占用" in message:
                    self.backend.auto_restart_suppressed = True
                    self.status_vars["backend_status"].set(message)
                    self.notify_tray("自动拉起已暂停", message)

        if self.backend.is_running():
            self.backend.consecutive_failed_starts = 0

        if (
            self.frontend.last_exit_at > self.last_notified_frontend_exit_at
            and not self.frontend.user_stopped
            and self.frontend.last_exit_code is not None
        ):
            self.last_notified_frontend_exit_at = self.frontend.last_exit_at
            fast_failed = self.frontend.register_failed_start_if_needed()
            self.notify_tray("前端异常退出", f"退出码：{self.frontend.last_exit_code}")
            webhook_url = self.manager_state.get("webhook_url", "")
            send_webhook_notification(webhook_url, "前端异常退出", f"退出码：{self.frontend.last_exit_code}")
            if self.frontend.auto_restart_suppressed:
                self.status_vars["frontend_status"].set(
                    f"启动失败过多，已暂停自动拉起 (最近退出码 {self.frontend.last_exit_code})"
                )
                self.notify_tray("前端已暂停自动拉起", "前端连续快速失败 3 次，请先修复依赖或配置后再手动启动")
                send_webhook_notification(webhook_url, "前端已暂停自动拉起", "前端连续快速失败 3 次")
            elif fast_failed:
                self.status_vars["frontend_status"].set(
                    f"启动后快速退出，等待自动重试 ({self.frontend.consecutive_failed_starts}/3)"
                )

        if (
            self.manager_state.get("auto_restart_frontend", True)
            and not self.frontend.is_running()
            and not self.frontend.user_stopped
            and not self.frontend.auto_restart_suppressed
            and time.time() - self.frontend.last_start_attempt > 8
            and self.frontend.last_start_attempt > 0
        ):
            ok, message = self.frontend.start(self.get_effective_config())
            if ok:
                self.notify_tray("前端已自动拉起", message)
                self.status_vars["frontend_status"].set(self.frontend.poll_status())
            else:
                if "端口" in message and "占用" in message:
                    self.frontend.auto_restart_suppressed = True
                    self.frontend._sync_runtime_state(frontend_auto_restart_suppressed=True)
                    self.status_vars["frontend_status"].set(message)
                    self.notify_tray("前端自动拉起已暂停", message)

        if self.frontend.is_running():
            self.frontend.consecutive_failed_starts = 0
            self.frontend._sync_runtime_state(frontend_consecutive_failed_starts=0)

        self.update_tray_title()

        self.schedule_status_refresh()

    def refresh_health_status(self) -> None:
        if self.exiting:
            return

        if not self.manager_state.get("enable_health_check", True):
            self.status_vars["backend_health_status"].set("已关闭")
            self.status_vars["frontend_health_status"].set("已关闭")
            self.last_backend_health_state = "unknown"
            self.last_frontend_health_state = "unknown"
            self.update_tray_title()
            self.schedule_health_refresh()
            return

        backend_ok = False
        backend_detail = "未运行"
        backend_state = "unknown"
        if self.backend.is_running():
            backend_ok, backend_detail = probe_backend_health(self.get_backend_url())
            backend_state = "healthy" if backend_ok else "unhealthy"
        self.status_vars["backend_health_status"].set(f"正常 ({backend_detail})" if backend_ok else f"异常 ({backend_detail})" if backend_state == "unhealthy" else "未运行")

        if self.backend.is_running() and self.last_backend_health_state not in {"unknown", backend_state}:
            webhook_url = self.manager_state.get("webhook_url", "")
            if backend_ok:
                self.notify_tray("后端健康检查恢复", f"{self.get_backend_url()} 已恢复可访问")
                send_webhook_notification(webhook_url, "后端健康检查恢复", f"{self.get_backend_url()} 已恢复可访问")
            else:
                self.notify_tray("后端健康检查异常", f"{self.get_backend_url()} 当前不可访问：{backend_detail}")
                send_webhook_notification(webhook_url, "后端健康检查异常", f"{self.get_backend_url()} 当前不可访问：{backend_detail}")
        self.last_backend_health_state = backend_state

        frontend_ok = False
        frontend_detail = "未运行"
        frontend_state = "unknown"
        if self.frontend.is_running():
            frontend_ok, frontend_detail = probe_http(self.get_frontend_url())
            frontend_state = "healthy" if frontend_ok else "unhealthy"
        self.status_vars["frontend_health_status"].set(
            f"正常 ({frontend_detail})" if frontend_ok else f"异常 ({frontend_detail})" if frontend_state == "unhealthy" else "未运行"
        )

        if self.frontend.is_running() and self.last_frontend_health_state not in {"unknown", frontend_state}:
            webhook_url = self.manager_state.get("webhook_url", "")
            if frontend_ok:
                self.notify_tray("前端健康检查恢复", f"{self.get_frontend_url()} 已恢复可访问")
                send_webhook_notification(webhook_url, "前端健康检查恢复", f"{self.get_frontend_url()} 已恢复可访问")
            else:
                self.notify_tray("前端健康检查异常", f"{self.get_frontend_url()} 当前不可访问：{frontend_detail}")
                send_webhook_notification(webhook_url, "前端健康检查异常", f"{self.get_frontend_url()} 当前不可访问：{frontend_detail}")
        self.last_frontend_health_state = frontend_state
        self.update_tray_title()
        self.schedule_health_refresh()

    def refresh_database_status(
        self,
        force_check: bool = False,
        show_dialog: bool = False,
        sqlcmd_override: str | None = None,
        schedule_next: bool = True,
    ) -> Tuple[str, str]:
        if self.exiting:
            return "unknown", "应用正在退出"

        monitor_enabled = self.manager_state.get("enable_db_monitor", True)
        if not force_check and not monitor_enabled:
            self.status_vars["db_monitor_status"].set("已关闭")
            self.status_vars["db_connection_status"].set("未检查 (数据库监控已关闭)")
            if schedule_next:
                self.schedule_db_refresh()
            self.last_database_status_state = "unknown"
            return "disabled", "数据库监控已关闭"

        sqlcmd = (sqlcmd_override if sqlcmd_override is not None else self.ops_vars["sqlcmd_path"].get().strip() or "sqlcmd").strip()
        state, detail = evaluate_database_runtime_status(sqlcmd)
        checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.status_vars["db_connection_status"].set(format_database_connection_status(state, detail))
        self.status_vars["db_monitor_status"].set("手动检查" if force_check and not monitor_enabled else "监控中" if monitor_enabled else "已关闭")
        self.status_vars["db_last_check_at"].set(checked_at)

        previous_state = self.last_database_status_state
        if (
            monitor_enabled
            and not force_check
            and previous_state not in {"unknown", state}
        ):
            webhook_url = self.manager_state.get("webhook_url", "")
            if state == "ok":
                self.notify_tray("数据库连接已恢复", detail)
                send_webhook_notification(webhook_url, "数据库连接已恢复", detail)
            else:
                self.notify_tray("数据库连接状态变化", detail)
                send_webhook_notification(webhook_url, "数据库连接状态变化", detail)
        self.last_database_status_state = state

        if show_dialog:
            if state == "ok":
                messagebox.showinfo("数据库连接检查", detail)
            elif state == "error":
                messagebox.showerror("数据库连接检查失败", detail)
            elif state == "missing_env":
                messagebox.showwarning("数据库待配置", detail)
            elif state == "missing_config":
                messagebox.showwarning("数据库配置不完整", detail)
            elif state == "missing_sqlcmd":
                messagebox.showwarning("数据库检查工具不可用", detail)
            elif state == "missing_runtime":
                messagebox.showwarning("数据库检查环境未就绪", detail)
            else:
                messagebox.showwarning("数据库连接检查", detail)

        if schedule_next:
            self.schedule_db_refresh()
        return state, detail

    def check_database_connection(self) -> None:
        self.save_manager_state()
        self.refresh_database_status(force_check=True, show_dialog=True, schedule_next=False)

    def resolve_log_path(self) -> Path:
        choice = self.log_choice.get()
        if choice == "后端错误输出":
            return STDERR_LOG
        if choice == "前端标准输出":
            return FRONTEND_STDOUT_LOG
        if choice == "前端错误输出":
            return FRONTEND_STDERR_LOG
        if choice == "项目同步日志":
            return PROJECT_SYNC_LOG
        return STDOUT_LOG

    def clear_current_log(self) -> None:
        path = self.resolve_log_path()
        if path == PROJECT_SYNC_LOG:
            confirm = messagebox.askyesno("确认清空", "将清空当前项目同步日志文件，是否继续？")
        else:
            confirm = messagebox.askyesno("确认清空", f"将清空当前日志文件：\n{path}\n是否继续？")
        if not confirm:
            return

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")
        except Exception as exc:
            messagebox.showerror("清空失败", str(exc))
            return

        self.log_status_var.set("当前显示：日志已清空")
        self.refresh_log_view(force=True)

    def summarize_log_view(self, path: Path, lines: List[str]) -> tuple[str, str]:
        if path == PROJECT_SYNC_LOG:
            return "当前显示：项目同步日志（历史累计）", "\n".join(lines[-300:])

        if path in (FRONTEND_STDOUT_LOG, FRONTEND_STDERR_LOG):
            marker = self.frontend.last_session_marker
            started_label = self.frontend.last_session_started_label
        else:
            marker = self.backend.last_session_marker
            started_label = self.backend.last_session_started_label
        if marker and marker in lines:
            marker_index = len(lines) - 1 - lines[::-1].index(marker)
            session_lines = lines[marker_index:]
            label = f"当前显示：本次启动日志（自 {started_label} 起）"
            return label, "\n".join(session_lines[-300:])

        label = "当前显示：历史日志（尚未找到本次启动分界线）"
        return label, "\n".join(lines[-300:])

    def refresh_log_view(self, force: bool = False) -> None:
        if self.exiting:
            return
        if not force and not self.log_auto_refresh.get():
            self.schedule_log_refresh()
            return

        path = self.resolve_log_path()
        content = ""
        if path.exists():
            try:
                raw_bytes = path.read_bytes()
                lines = decode_console_output(raw_bytes).splitlines()
                status_text, content = self.summarize_log_view(path, lines)
                self.log_status_var.set(status_text)
            except Exception as exc:
                content = f"读取日志失败：{exc}"
                self.log_status_var.set("当前显示：日志读取失败")
        else:
            content = f"日志文件不存在：{path}"
            self.log_status_var.set("当前显示：日志文件不存在")

        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.insert("1.0", content)
        self.log_text.configure(state="disabled")
        self.log_text.see(tk.END)

        self.schedule_log_refresh()

    def run(self) -> None:
        self.root.mainloop()


if __name__ == "__main__":
    cli_parser = build_cli_parser()
    cli_args = cli_parser.parse_args()
    raise SystemExit(run_cli(cli_args))
