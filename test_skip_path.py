from pathlib import Path

ROOT_DIR = Path('d:/FinFlow')
SKIP_PART_NAMES = {'__pycache__', '.git', '.venv', 'node_modules'}

def should_skip_release_path(relative_path):
    if not relative_path.parts:
        return True
    normalized = Path(*relative_path.parts)
    if any(part in SKIP_PART_NAMES for part in normalized.parts):
        return True
    return False

# 测试 frontend/package.json
test_paths = [
    Path('frontend/package.json'),
    Path('frontend/package-lock.json'),
    Path('frontend/node_modules/some-package'),
]

for p in test_paths:
    skip = should_skip_release_path(p)
    status = "跳过" if skip else "包含"
    print(f'{p}: {status}')
