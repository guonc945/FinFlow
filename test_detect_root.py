from pathlib import Path
import zipfile

extract_dir = Path('d:/FinFlow/test_extract')
extract_dir.mkdir(exist_ok=True)

with zipfile.ZipFile('d:/FinFlow/FinFlow_deploy_20260416_104510.zip', 'r') as archive:
    archive.extractall(extract_dir)

def detect_release_root(extract_dir):
    current = extract_dir
    for i in range(3):
        print(f'  层级 {i}: {current}')
        if any((current / name).exists() for name in ('backend', 'frontend', 'tools', 'deploy')):
            print(f'  找到 release root: {current}')
            return current
        children = [item for item in current.iterdir() if item.name != '__MACOSX']
        print(f'  子目录: {[c.name for c in children]}')
        if len(children) == 1 and children[0].is_dir():
            current = children[0]
            continue
        break
    print(f'  返回 extract_dir: {extract_dir}')
    return extract_dir

release_root = detect_release_root(extract_dir)
print(f'\n最终 release root: {release_root}')
print(f'frontend/package.json 是否存在: {(release_root / "frontend" / "package.json").exists()}')

# 清理
import shutil
shutil.rmtree(extract_dir, ignore_errors=True)
