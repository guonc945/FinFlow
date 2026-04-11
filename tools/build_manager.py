# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def remove_tree_with_retries(path: Path, attempts: int = 5, delay_seconds: float = 1.0) -> bool:
    if not path.exists():
        return True
    for _ in range(attempts):
        try:
            shutil.rmtree(path, ignore_errors=False)
            return True
        except Exception:
            time.sleep(delay_seconds)
    return not path.exists()


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    tools_dir = project_root / "tools"
    deploy_windows_dir = project_root / "deploy" / "windows"
    spec_file = tools_dir / "finflow_manager.spec"
    requirements_file = deploy_windows_dir / "manager_requirements.txt"
    dist_dir = deploy_windows_dir / "dist"
    build_dir = deploy_windows_dir / "build"

    print("=" * 64)
    print("FinFlowManager EXE Build")
    print("=" * 64)
    print(f"Project root : {project_root}")
    print(f"Spec file    : {spec_file}")
    print(f"Requirements : {requirements_file}")
    print(f"Dist dir     : {dist_dir}")
    print(f"Build dir    : {build_dir}")
    print()

    if not spec_file.exists():
        print(f"Missing spec file: {spec_file}")
        return 1
    if not requirements_file.exists():
        print(f"Missing requirements file: {requirements_file}")
        return 1

    try:
        import PyInstaller  # noqa: F401
    except ImportError:
        print("PyInstaller is not installed.")
        print(f"Run: {sys.executable} -m pip install pyinstaller")
        return 1

    print("Installing manager build dependencies...")
    install_cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        str(requirements_file),
    ]
    install_result = subprocess.run(install_cmd, cwd=project_root)
    if install_result.returncode != 0:
        print("Failed to install manager build dependencies.")
        return install_result.returncode

    if dist_dir.exists():
        print(f"Cleaning dist directory: {dist_dir}")
        remove_tree_with_retries(dist_dir)
    if build_dir.exists():
        print(f"Cleaning build directory: {build_dir}")
        remove_tree_with_retries(build_dir)

    dist_dir.mkdir(parents=True, exist_ok=True)
    build_dir.mkdir(parents=True, exist_ok=True)

    build_cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--distpath",
        str(dist_dir),
        "--workpath",
        str(build_dir),
        str(spec_file),
    ]
    print("Running PyInstaller...")
    print(" ".join(f'"{part}"' if " " in part else part for part in build_cmd))
    print()

    build_env = dict(os.environ)
    build_env["PYTHONWARNINGS"] = "ignore"
    result = subprocess.run(build_cmd, cwd=project_root, env=build_env)
    if result.returncode != 0:
        print()
        print("Build failed.")
        return result.returncode

    exe_path = dist_dir / "FinFlowManager.exe"
    print()
    if not exe_path.exists():
        print(f"Build finished but EXE was not found: {exe_path}")
        return 1

    size_mb = exe_path.stat().st_size / (1024 * 1024)
    if build_dir.exists():
        print(f"Cleaning intermediate build directory: {build_dir}")
        if not remove_tree_with_retries(build_dir):
            print(f"Warning: failed to fully remove intermediate build directory: {build_dir}")
    print("Build succeeded.")
    print(f"EXE path: {exe_path}")
    print(f"EXE size: {size_mb:.2f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
