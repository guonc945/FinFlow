from pathlib import Path
import zipfile
import shutil

ROOT_DIR = Path('d:/FinFlow')
extract_dir = Path('d:/FinFlow/test_extract')
extract_dir.mkdir(exist_ok=True)

with zipfile.ZipFile('d:/FinFlow/FinFlow_deploy_20260416_104510.zip', 'r') as archive:
    archive.extractall(extract_dir)

def detect_release_root(extract_dir):
    current = extract_dir
    for i in range(3):
        if any((current / name).exists() for name in ('backend', 'frontend', 'tools', 'deploy')):
            return current
        children = [item for item in current.iterdir() if item.name != '__MACOSX']
        if len(children) == 1 and children[0].is_dir():
            current = children[0]
            continue
        break
    return extract_dir

release_root = detect_release_root(extract_dir)
print(f'release root: {release_root}')
print(f'release_root/frontend/package.json: {(release_root / "frontend" / "package.json").exists()}')

# 检查 overlay_directory
SKIP_PART_NAMES = {'__pycache__', '.git', '.venv', 'node_modules'}
PROTECTED_EXACT_PATHS = {
    Path('.encryption.key'),
    Path('backend/.env'),
    Path('backend/.encryption.key'),
    Path('frontend/.env'),
    Path('deploy/windows/manager_state.json'),
}
PROTECTED_PREFIXES = {
    Path('.git'),
    Path('.venv'),
    Path('backend/.venv'),
    Path('backend/logs'),
    Path('frontend/node_modules'),
    Path('deploy/windows/build'),
    Path('deploy/windows/dist'),
    Path('deploy/windows/upgrade_backups'),
    Path('backups'),
}

def should_skip_release_path(relative_path):
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

# 模拟 overlay_directory
source_dir = release_root / 'frontend'
target_dir = ROOT_DIR / 'frontend'

print(f'\n源目录: {source_dir}')
print(f'目标目录: {target_dir}')

copied = 0
skipped = 0
for source_file in source_dir.rglob('*'):
    if source_file.is_dir():
        continue
    destination = target_dir / source_file.relative_to(source_dir)
    relative_to_root = destination.relative_to(ROOT_DIR)
    
    if should_skip_release_path(relative_to_root):
        skipped += 1
        if 'package.json' in str(relative_to_root):
            print(f'  跳过: {relative_to_root}')
        continue
    
    copied += 1
    if 'package.json' in str(relative_to_root):
        print(f'  复制: {relative_to_root}')

print(f'\n复制: {copied} 个文件')
print(f'跳过: {skipped} 个文件')

# 检查目标目录
print(f'\n目标目录 frontend/package.json 是否存在: {(target_dir / "package.json").exists()}')

# 清理
shutil.rmtree(extract_dir, ignore_errors=True)
