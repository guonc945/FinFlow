import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKEND_DIR = PROJECT_ROOT / "backend"
FRONTEND_DIR = PROJECT_ROOT / "frontend"
FRONTEND_DIST_DIR = PROJECT_ROOT / "frontend" / "dist"

BACKEND_INCLUDE_DIRS = {
    "api",
    "services",
    "utils",
}
BACKEND_INCLUDE_ROOT_FILES = {
    "__init__.py",
    "database.py",
    "fetch_bills.py",
    "fetch_charge_items.py",
    "fetch_deposit_records.py",
    "fetch_houses.py",
    "fetch_parks.py",
    "fetch_prepayment_records.py",
    "fetch_receipt_bills.py",
    "fetch_residents.py",
    "main.py",
    "models.py",
    "receipt_bill_deposit_links.py",
    "requirements.txt",
    "schemas.py",
    "sync_tracker.py",
    "voucher_field_mapping.py",
    "voucher_source_registry.py",
}
BACKEND_INCLUDE_EXTRA_FILES = {
    Path("backend/scripts/run_sync_schedule_target.py"),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_backend_files() -> list[Path]:
    files: set[Path] = set()

    for filename in BACKEND_INCLUDE_ROOT_FILES:
        file_path = BACKEND_DIR / filename
        if file_path.is_file():
            files.add(file_path.relative_to(PROJECT_ROOT))

    for dirname in BACKEND_INCLUDE_DIRS:
        directory = BACKEND_DIR / dirname
        if not directory.is_dir():
            continue
        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() == ".pyc":
                continue
            files.add(file_path.relative_to(PROJECT_ROOT))

    for rel_path in BACKEND_INCLUDE_EXTRA_FILES:
        file_path = PROJECT_ROOT / rel_path
        if file_path.is_file():
            files.add(rel_path)

    return sorted(files)


def iter_frontend_dist_files() -> list[Path]:
    files: list[Path] = []
    for path in FRONTEND_DIST_DIR.rglob("*"):
        if path.is_file():
            files.append(path.relative_to(PROJECT_ROOT))
    return sorted(files)


def build_manifest(files: list[Path]) -> dict:
    entries = []
    for rel_path in files:
        full_path = PROJECT_ROOT / rel_path
        entries.append(
            {
                "path": rel_path.as_posix(),
                "size": full_path.stat().st_size,
                "sha256": sha256_file(full_path),
            }
        )
    return {
        "package_type": "finflow_update",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "project_root": "FinFlow",
        "includes": {
            "backend": True,
            "frontend_dist": True,
            "deploy_windows": False,
        },
        "file_count": len(entries),
        "entries": entries,
    }


def build_frontend_dist() -> None:
    package_json = FRONTEND_DIR / "package.json"
    if not package_json.exists():
        raise RuntimeError(f"未找到前端项目文件: {package_json}")

    npm_executable = shutil.which("npm")
    if not npm_executable:
        raise RuntimeError("未找到 npm，请先安装 Node.js 并确保 npm 在 PATH 中")

    print("[RUN] 正在构建前端 dist ...")
    result = subprocess.run(
        [npm_executable, "run", "build"],
        cwd=FRONTEND_DIR,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError("前端构建失败，请先修复构建错误后再打包更新 ZIP")


def create_update_package(output_path: str | None = None) -> Path:
    build_frontend_dist()
    if not FRONTEND_DIST_DIR.exists() or not (FRONTEND_DIST_DIR / "index.html").exists():
        raise RuntimeError("前端构建已执行，但仍未找到 frontend/dist/index.html")

    if output_path:
        archive_path = Path(output_path)
    else:
        archive_path = PROJECT_ROOT / f"FinFlow_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"

    archive_path.parent.mkdir(parents=True, exist_ok=True)

    backend_files = iter_backend_files()
    frontend_files = iter_frontend_dist_files()
    all_files = backend_files + frontend_files
    manifest = build_manifest(all_files)

    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for rel_path in all_files:
            archive.write(PROJECT_ROOT / rel_path, f"FinFlow/{rel_path.as_posix()}")
        archive.writestr(
            "FinFlow/_package/release_manifest.json",
            json.dumps(manifest, ensure_ascii=False, indent=2),
        )

    return archive_path


def main() -> int:
    parser = argparse.ArgumentParser(description="生成 FinFlow 更新 ZIP，仅包含前端 dist 与必要后端文件")
    parser.add_argument("-o", "--output", help="输出 ZIP 路径")
    args = parser.parse_args()

    try:
        archive_path = create_update_package(args.output)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    print(f"[OK] 更新包已生成: {archive_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
