import json
import os
import platform
import secrets
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
from typing import Any, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import http.client

import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk

try:
    from PIL import Image, ImageDraw
except ModuleNotFoundError as exc:
    print(
        "缺少管理器依赖 Pillow。\n"
        "请先执行：python -m pip install -r deploy\\windows\\manager_requirements.txt\n"
        f"详细信息：{exc}"
    )
    raise SystemExit(1) from exc

try:
    from pystray import Icon, Menu, MenuItem
except ModuleNotFoundError as exc:
    print(
        "缺少管理器依赖 pystray。\n"
        "请先执行：python -m pip install -r deploy\\windows\\manager_requirements.txt\n"
        f"详细信息：{exc}"
    )
    raise SystemExit(1) from exc


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
KEY_PATH = BACKEND_DIR / ".encryption.key"
STATE_PATH = ROOT_DIR / "deploy" / "windows" / "manager_state.json"
STDOUT_LOG = LOG_DIR / "backend.stdout.log"
STDERR_LOG = LOG_DIR / "backend.stderr.log"
PROJECT_SYNC_LOG = BACKEND_DIR / "scripts" / "fetch_projects.log"
BACKUP_DIR = ROOT_DIR / "backups"
UPGRADE_BACKUP_DIR = ROOT_DIR / "deploy" / "windows" / "upgrade_backups"
LOG_ARCHIVE_DIR = LOG_DIR / "archive"
STARTUP_DIR = Path(os.environ.get("APPDATA") or str(ROOT_DIR)) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
STARTUP_SCRIPT_PATH = STARTUP_DIR / "FinFlowManager.cmd"
STATUS_REFRESH_INTERVAL_MS = 2000
LOG_REFRESH_INTERVAL_MS = 2500
HEALTH_CHECK_INTERVAL_MS = 5000
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
    "DB_PORT": "5432",
    "DB_NAME": "finflow",
    "DB_USER": "postgres",
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
    "start_backend_on_launch": False,
    "hide_to_tray_on_close": True,
    "launch_manager_on_startup": False,
    "enable_health_check": True,
    "frontend_deploy_source": "",
    "release_package_path": "",
    "backup_dir": str(BACKUP_DIR),
    "pg_dump_path": "pg_dump",
}


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


def create_tray_image() -> Image.Image:
    image = Image.new("RGBA", (64, 64), (27, 84, 157, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((8, 8, 56, 56), radius=12, fill=(39, 125, 161, 255))
    draw.rectangle((20, 18, 44, 24), fill=(255, 255, 255, 255))
    draw.rectangle((20, 30, 44, 36), fill=(255, 255, 255, 255))
    draw.rectangle((20, 42, 36, 48), fill=(255, 255, 255, 255))
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


def extract_postgres_connection(config: Dict[str, str]) -> Dict[str, str]:
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

    return {
        "host": (config.get("DB_HOST") or "127.0.0.1").strip(),
        "port": (config.get("DB_PORT") or "5432").strip(),
        "user": (config.get("DB_USER") or "").strip(),
        "password": (config.get("DB_PASSWORD") or "").strip(),
        "dbname": (config.get("DB_NAME") or "").strip(),
    }


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


class FinFlowManagerApp:
    def __init__(self) -> None:
        ensure_tcl_tk_environment()
        self.root = tk.Tk()
        self.root.title("FinFlow 管理器")
        self.root.geometry("980x760")
        self.root.minsize(900, 680)

        self.backend = BackendProcessController()
        self.manager_state = read_state_file(STATE_PATH)
        self.config_values, self.extra_env_values = self.load_config_values()
        self.form_vars: Dict[str, tk.StringVar] = {}
        self.status_vars: Dict[str, tk.StringVar] = {}
        self.manager_option_vars: Dict[str, tk.BooleanVar] = {}
        self.ops_vars: Dict[str, tk.StringVar] = {}
        self.log_choice = tk.StringVar(value="后端标准输出")
        self.log_auto_refresh = tk.BooleanVar(value=True)
        self.log_status_var = tk.StringVar(value="当前显示：历史日志")
        self.tray_icon: Icon | None = None
        self.tray_thread: threading.Thread | None = None
        self.exiting = False
        self.last_notified_exit_at = 0.0
        self.last_health_state = "unknown"
        self.status_job: str | None = None
        self.log_job: str | None = None
        self.health_job: str | None = None
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
        self.root.after(1500, self.maybe_start_backend_on_launch)

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
        summary = ttk.LabelFrame(parent, text="当前状态", padding=16)
        summary.pack(fill="x", padx=10, pady=10)

        status_items = {
            "project_root": str(ROOT_DIR),
            "env_file": "已存在" if ENV_PATH.exists() else "缺失",
            "key_file": "已存在" if KEY_PATH.exists() else "缺失",
            "dist_dir": "已存在" if (DIST_DIR / "index.html").exists() else "缺失",
            "backend_status": "未运行",
            "port_owner": "未占用",
            "health_status": "未检查",
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
                "port_owner": "端口占用",
                "health_status": "健康检查",
                "startup_status": "开机自启",
                "app_url": "访问地址",
            }[key]
            ttk.Label(summary, text=f"{label_text}：").grid(row=row, column=0, sticky="w", pady=4)
            var = tk.StringVar(value=value)
            ttk.Label(summary, textvariable=var).grid(row=row, column=1, sticky="w", pady=4)
            self.status_vars[key] = var
            row += 1

        actions = ttk.LabelFrame(parent, text="服务操作", padding=16)
        actions.pack(fill="x", padx=10, pady=10)

        ttk.Button(actions, text="启动后端", command=self.handle_start_backend).grid(row=0, column=0, padx=6, pady=6)
        ttk.Button(actions, text="停止后端", command=self.handle_stop_backend).grid(row=0, column=1, padx=6, pady=6)
        ttk.Button(actions, text="重启后端", command=self.handle_restart_backend).grid(row=0, column=2, padx=6, pady=6)
        ttk.Button(actions, text="打开前端", command=self.open_frontend).grid(row=0, column=3, padx=6, pady=6)
        ttk.Button(actions, text="接管现有实例", command=self.handle_takeover_backend).grid(row=0, column=4, padx=6, pady=6)
        ttk.Button(actions, text="强制释放端口", command=self.handle_force_release_port).grid(row=0, column=5, padx=6, pady=6)
        ttk.Button(actions, text="打开日志目录", command=self.open_logs_folder).grid(row=0, column=6, padx=6, pady=6)
        ttk.Button(actions, text="隐藏到托盘", command=self.hide_to_tray).grid(row=0, column=7, padx=6, pady=6)

        options = ttk.LabelFrame(parent, text="管理器选项", padding=16)
        options.pack(fill="x", padx=10, pady=10)

        option_items = [
            ("auto_restart_backend", "后端异常退出后自动拉起"),
            ("start_backend_on_launch", "打开管理器后自动启动后端"),
            ("hide_to_tray_on_close", "关闭窗口时最小化到托盘"),
            ("launch_manager_on_startup", "Windows 登录后自动启动管理器"),
            ("enable_health_check", "启用健康检查与托盘状态提示"),
        ]
        for idx, (key, label) in enumerate(option_items):
            var = tk.BooleanVar(value=self.manager_state.get(key, False))
            self.manager_option_vars[key] = var
            ttk.Checkbutton(options, text=label, variable=var, command=self.save_manager_state).grid(
                row=idx // 2, column=idx % 2, padx=10, pady=4, sticky="w"
            )

        tips = ttk.LabelFrame(parent, text="说明", padding=16)
        tips.pack(fill="both", expand=True, padx=10, pady=10)
        ttk.Label(
            tips,
            text=(
                "当前模式下不依赖 IIS、NSSM 或 Windows 服务。\n"
                "管理器负责配置 backend/.env、backend/.encryption.key，以及后端进程的启动、停止、监控和自动拉起。\n"
                "前端由 FastAPI 直接托管 frontend/dist，访问时只需要一个端口。"
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
            card = tk.Frame(parent, bd=1, relief="solid", bg="#f5f7fa", padx=10, pady=8, cursor="hand2")
            card.pack(fill="x", pady=4)

            title_label = tk.Label(
                card,
                text=title,
                bg="#f5f7fa",
                fg="#15304b",
                font=("Microsoft YaHei UI", 10, "bold"),
                anchor="w",
                justify="left",
                cursor="hand2",
            )
            title_label.pack(fill="x", anchor="w")

            desc_label = tk.Label(
                card,
                text=description,
                bg="#f5f7fa",
                fg="#5f6b7a",
                font=("Microsoft YaHei UI", 9),
                anchor="w",
                justify="left",
                wraplength=190,
                cursor="hand2",
            )
            desc_label.pack(fill="x", anchor="w", pady=(4, 0))

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
            card_bg = "#dceeff" if selected else "#f5f7fa"
            title_fg = "#0f4c81" if selected else "#15304b"
            desc_fg = "#2c5d86" if selected else "#5f6b7a"
            for widget_key, widget in widgets.items():
                if widget_key == "card":
                    widget.configure(bg=card_bg, highlightbackground="#c7d8e8")
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

        nav_frame = tk.LabelFrame(
            container,
            text="配置导航",
            bd=1,
            relief="solid",
            bg="#eef3f8",
            fg="#15304b",
            padx=10,
            pady=10,
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        nav_frame.pack(side="left", fill="y", padx=(0, 10))

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

        self.show_config_section()

    def build_ops_tab(self, parent: ttk.Frame) -> None:
        container = ttk.Frame(parent)
        container.pack(fill="both", expand=True, padx=10, pady=10)

        nav_frame = tk.LabelFrame(
            container,
            text="运维导航",
            bd=1,
            relief="solid",
            bg="#eef3f8",
            fg="#15304b",
            padx=10,
            pady=10,
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        nav_frame.pack(side="left", fill="y", padx=(0, 10))

        content_frame = ttk.Frame(container)
        content_frame.pack(side="left", fill="both", expand=True)

        self.ops_section_var = tk.StringVar(value="frontend")
        ops_nav_items = [
            ("frontend", "前端部署", "覆盖发布 dist，更新页面静态资源"),
            ("release", "发布包升级", "导入 ZIP 发布包并执行一键升级"),
            ("backup", "数据库备份", "调用 pg_dump 执行数据库逻辑备份"),
            ("notes", "使用说明", "查看当前运维方式和操作建议"),
        ]

        for key in ("frontend_deploy_source", "release_package_path", "backup_dir", "pg_dump_path"):
            if key not in self.ops_vars:
                self.ops_vars[key] = tk.StringVar(value=str(self.manager_state.get(key, DEFAULT_STATE[key])))

        self.create_side_nav(
            nav_frame,
            ops_nav_items,
            self.ops_section_var,
            self.show_ops_section,
            self.ops_nav_items,
        )

        common_group = ttk.LabelFrame(content_frame, text="基础路径设置", padding=16)
        common_group.pack(fill="x", pady=(0, 10))

        path_fields = [
            ("frontend_deploy_source", "前端 dist 来源目录", "directory"),
            ("release_package_path", "发布包 ZIP 文件", "zip"),
            ("backup_dir", "数据库备份输出目录", "directory"),
            ("pg_dump_path", "pg_dump 可执行文件", "file"),
        ]

        for row, (key, label, select_mode) in enumerate(path_fields):
            ttk.Label(common_group, text=f"{label}：", width=18).grid(row=row, column=0, sticky="w", padx=6, pady=6)
            ttk.Entry(common_group, textvariable=self.ops_vars[key], width=82).grid(
                row=row, column=1, sticky="ew", padx=6, pady=6
            )
            ttk.Button(
                common_group,
                text="选择",
                command=lambda target_key=key, mode=select_mode: self.select_path_for_var(target_key, mode),
            ).grid(row=row, column=2, padx=6, pady=6)
        common_group.columnconfigure(1, weight=1)

        ttk.Button(common_group, text="保存运维设置", command=self.save_manager_state).grid(
            row=len(path_fields), column=1, sticky="w", padx=6, pady=(10, 0)
        )

        self.ops_content_host = ttk.Frame(content_frame)
        self.ops_content_host.pack(fill="both", expand=True)
        self.ops_section_frames: Dict[str, ttk.Frame] = {}

        frontend_frame = ttk.LabelFrame(self.ops_content_host, text="前端部署", padding=16)
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

        backup_frame = ttk.LabelFrame(self.ops_content_host, text="数据库备份", padding=16)
        ttk.Label(
            backup_frame,
            text="使用 pg_dump 对 PostgreSQL 数据库执行逻辑备份，连接信息来自当前 backend/.env。",
            justify="left",
        ).pack(anchor="w", pady=(0, 8))
        backup_actions = ttk.Frame(backup_frame)
        backup_actions.pack(fill="x")
        ttk.Button(backup_actions, text="立即备份数据库", command=self.backup_database).pack(side="left", padx=4)
        ttk.Button(backup_actions, text="打开备份目录", command=self.open_backup_folder).pack(side="left", padx=4)
        self.ops_section_frames["backup"] = backup_frame

        notes_frame = ttk.LabelFrame(self.ops_content_host, text="使用说明", padding=16)
        ttk.Label(
            notes_frame,
            text=(
                "1. 前端建议先在构建机完成 npm run build，再把 dist 目录复制到服务器。\n"
                "2. 一键升级建议使用标准 ZIP 发布包，包内至少包含 backend 或 frontend/dist。\n"
                "3. 数据库备份只支持当前项目默认的 PostgreSQL 场景，运行前请先确认 pg_dump 可执行文件可用。"
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

        nav_frame = tk.LabelFrame(
            container,
            text="检查导航",
            bd=1,
            relief="solid",
            bg="#eef3f8",
            fg="#15304b",
            padx=10,
            pady=10,
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        nav_frame.pack(side="left", fill="y", padx=(0, 10))

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
        choices = ["后端标准输出", "后端错误输出", "项目同步日志"]
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
        active_key = getattr(self, "ops_section_var", tk.StringVar(value="frontend")).get()
        for section_key, frame in self.ops_section_frames.items():
            if section_key == active_key:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
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
                f"端口占用: {self.status_vars.get('port_owner').get() if self.status_vars.get('port_owner') else '未知'}",
                f"配置文件: {'存在' if ENV_PATH.exists() else '缺失'}",
                f"加密密钥: {'存在' if KEY_PATH.exists() else '缺失'}",
                f"前端构建: {'存在' if (DIST_DIR / 'index.html').exists() else '缺失'}",
                f"日志目录: {'存在' if LOG_DIR.exists() else '缺失'}",
                f"健康检查: {self.status_vars.get('health_status').get() if self.status_vars.get('health_status') else '未检查'}",
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
            MenuItem("启动后端", lambda: self.root.after(0, self.handle_start_backend)),
            MenuItem("停止后端", lambda: self.root.after(0, self.handle_stop_backend)),
            MenuItem("重启后端", lambda: self.root.after(0, self.handle_restart_backend)),
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
        health_status = self.status_vars.get("health_status")
        backend_text = backend_status.get() if backend_status else "未运行"
        health_text = health_status.get() if health_status else "未检查"
        self.tray_icon.title = f"FinFlow 管理器 | {backend_text} | 健康：{health_text}"

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

    def save_manager_state(self) -> None:
        for key, var in self.manager_option_vars.items():
            self.manager_state[key] = bool(var.get())
        for key, var in self.ops_vars.items():
            self.manager_state[key] = var.get().strip()
        write_state_file(STATE_PATH, self.manager_state)
        self.sync_startup_entry(show_message=False)
        self.refresh_status()

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

    def maybe_start_backend_on_launch(self) -> None:
        if self.manager_state.get("start_backend_on_launch"):
            self.handle_start_backend()

    def handle_save_config(self) -> None:
        try:
            self.save_config_values()
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

    def get_app_url(self) -> str:
        config = self.get_effective_config()
        host = resolve_browser_host(config.get("APP_HOST", "127.0.0.1"))
        port = config.get("APP_PORT", "8100") or "8100"
        return f"http://{host}:{port}/"

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
            "升级过程会自动停止后端，覆盖发布包中的代码和前端构建文件，同时保留本机配置、密钥、虚拟环境与日志。是否继续？",
        )
        if not proceed:
            return

        extract_dir = Path(tempfile.mkdtemp(prefix="finflow_release_"))
        backup_root = UPGRADE_BACKUP_DIR / datetime.now().strftime("%Y%m%d_%H%M%S")
        was_running = self.backend.is_running()
        restored_after_error = False

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

            if was_running:
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

            restart_message = ""
            if was_running:
                ok, restart_message = self.backend.start(self.get_effective_config())
                if not ok:
                    raise RuntimeError(f"升级完成，但后端自动重启失败：{restart_message}")

            self.last_health_state = "unknown"
            self.refresh_status()
            self.notify_tray("发布包升级完成", f"已应用：{package_path.name}")
            summary = "，".join(f"{key}={value}" for key, value in copied_counts.items() if value)
            detail = f"升级完成。\n备份目录：{backup_root}\n覆盖内容：{summary or '无文件变更'}"
            if restart_message:
                detail += f"\n后端重启：{restart_message}"
            messagebox.showinfo("升级完成", detail)
        except Exception as exc:
            if was_running and not self.backend.is_running():
                ok, restart_message = self.backend.start(self.get_effective_config())
                restored_after_error = ok
                if ok:
                    self.last_health_state = "unknown"
            else:
                restart_message = "后端未重启"

            backup_info = str(backup_root) if backup_root.exists() else "未生成备份目录"
            detail = f"{exc}\n备份目录：{backup_info}\n恢复结果：{restart_message}"
            if restored_after_error:
                self.notify_tray("发布包升级失败", "已尝试恢复后端运行")
            messagebox.showerror("升级失败", detail)
        finally:
            shutil.rmtree(extract_dir, ignore_errors=True)
            self.refresh_status()

    def backup_database(self) -> None:
        self.save_manager_state()
        backup_dir = Path(self.ops_vars["backup_dir"].get().strip() or str(BACKUP_DIR))
        pg_dump = self.ops_vars["pg_dump_path"].get().strip() or "pg_dump"
        backup_dir.mkdir(parents=True, exist_ok=True)

        config = self.get_effective_config()
        conn = extract_postgres_connection(config)
        missing = [key for key in ("host", "port", "user", "dbname") if not conn.get(key)]
        if missing:
            messagebox.showerror("备份失败", f"数据库配置不完整，缺少：{', '.join(missing)}")
            return

        filename = f"finflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        target_file = backup_dir / filename
        env = os.environ.copy()
        if conn.get("password"):
            env["PGPASSWORD"] = conn["password"]

        cmd = [
            pg_dump,
            "-h",
            conn["host"],
            "-p",
            conn["port"],
            "-U",
            conn["user"],
            "-d",
            conn["dbname"],
            "-f",
            str(target_file),
        ]

        try:
            result = subprocess.run(
                cmd,
                env=env,
                cwd=ROOT_DIR,
                capture_output=True,
                text=False,
                timeout=600,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except FileNotFoundError:
            messagebox.showerror("备份失败", f"未找到 pg_dump：{pg_dump}")
            return
        except Exception as exc:
            messagebox.showerror("备份失败", f"执行 pg_dump 失败：{exc}")
            return

        stdout_text = decode_console_output(result.stdout or b"")
        stderr_text = decode_console_output(result.stderr or b"")

        if result.returncode != 0:
            detail = (stderr_text or stdout_text or "未知错误").strip()
            messagebox.showerror("备份失败", detail)
            if target_file.exists():
                remove_path(target_file)
            return

        messagebox.showinfo("备份成功", f"数据库已备份到：{target_file}")

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
        for job_name in ("status_job", "log_job", "health_job"):
            job = getattr(self, job_name)
            if job:
                try:
                    self.root.after_cancel(job)
                except Exception:
                    pass
                setattr(self, job_name, None)
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
        config = self.get_effective_config()
        port = (config.get("APP_PORT") or "8100").strip() or "8100"
        owner_text = "未占用"
        if port.isdigit():
            owner = get_port_owner_info(int(port))
            if owner:
                manager_pid = self.backend.process.pid if self.backend.is_running() and self.backend.process else None
                owner_text = build_port_owner_label(owner, manager_pid)
        self.status_vars["env_file"].set("已存在" if ENV_PATH.exists() else "缺失")
        self.status_vars["key_file"].set("已存在" if KEY_PATH.exists() else "缺失")
        self.status_vars["dist_dir"].set("已存在" if (DIST_DIR / "index.html").exists() else "缺失")
        self.status_vars["backend_status"].set(backend_status)
        self.status_vars["port_owner"].set(owner_text)
        self.status_vars["startup_status"].set(self.get_startup_status_text())
        self.status_vars["app_url"].set(self.get_app_url())

        if (
            self.backend.last_exit_at > self.last_notified_exit_at
            and not self.backend.user_stopped
            and self.backend.last_exit_code is not None
        ):
            self.last_notified_exit_at = self.backend.last_exit_at
            fast_failed = self.backend.register_failed_start_if_needed()
            self.notify_tray("后端异常退出", f"退出码：{self.backend.last_exit_code}")
            if self.backend.auto_restart_suppressed:
                self.status_vars["backend_status"].set(
                    f"启动失败过多，已暂停自动拉起 (最近退出码 {self.backend.last_exit_code})"
                )
                self.notify_tray("已暂停自动拉起", "后端连续快速失败 3 次，请先修复依赖或配置后再手动启动")
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

        self.update_tray_title()

        self.schedule_status_refresh()

    def refresh_health_status(self) -> None:
        if self.exiting:
            return

        if not self.manager_state.get("enable_health_check", True):
            self.status_vars["health_status"].set("已关闭")
            self.last_health_state = "unknown"
            self.update_tray_title()
            self.schedule_health_refresh()
            return

        if not self.backend.is_running():
            self.status_vars["health_status"].set("未运行")
            self.last_health_state = "unknown"
            self.update_tray_title()
            self.schedule_health_refresh()
            return

        ok, detail = probe_http(self.get_app_url())
        new_state = "healthy" if ok else "unhealthy"
        self.status_vars["health_status"].set(f"正常 ({detail})" if ok else f"异常 ({detail})")
        if self.last_health_state not in {"unknown", new_state}:
            if ok:
                self.notify_tray("健康检查恢复", f"{self.get_app_url()} 已恢复可访问")
            else:
                self.notify_tray("健康检查异常", f"{self.get_app_url()} 当前不可访问：{detail}")
        self.last_health_state = new_state
        self.update_tray_title()
        self.schedule_health_refresh()

    def resolve_log_path(self) -> Path:
        choice = self.log_choice.get()
        if choice == "后端错误输出":
            return STDERR_LOG
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

        marker = self.backend.last_session_marker
        if marker and marker in lines:
            marker_index = len(lines) - 1 - lines[::-1].index(marker)
            session_lines = lines[marker_index:]
            label = f"当前显示：本次启动日志（自 {self.backend.last_session_started_label} 起）"
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
    try:
        app = FinFlowManagerApp()
        app.run()
    except tk.TclError as exc:
        print(
            "当前 Python 环境的 Tcl/Tk 组件不可用，无法启动 Tkinter 图形界面。\n"
            "当前脚本已经尝试自动补全 Tcl/Tk 路径；如果仍失败，请确认你的 Python 安装目录下存在 tcl\\tcl8.6 和 tcl\\tk8.6。\n"
            "如果你坚持使用 Python 3.13，请优先检查虚拟环境是否引用到了正确的 base Python 安装。\n"
            "推荐步骤：\n"
            "1. 确认 C:\\Program Files\\Python313\\tcl\\tcl8.6 与 tk8.6 存在\n"
            "2. 重新进入虚拟环境后再启动管理器\n"
            "3. 如仍失败，再重建 backend/.venv\n"
            f"详细错误：{exc}"
        )
        raise SystemExit(1) from exc
