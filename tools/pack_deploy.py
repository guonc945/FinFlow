import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 排除的目录名（任何层级）
EXCLUDE_DIR_NAMES = {
    '.git', '__pycache__', 'node_modules', '.venv', 'venv',
    'dist', 'build', '_internal', '.tmp', 'bin', 'obj',
    'logs', '.pytest_cache', '.mypy_cache', 'coverage',
    '.next', 'out', 'cache', '.idea', '.vscode',
    'tests', 'tools',
}

# 排除的完整路径（相对路径）
EXCLUDE_PATHS = {
    'deploy/windows/dist',
    'deploy/windows/build',
    'tools/dist',
    'backend/.venv',
    'frontend/node_modules',
    'node_modules',
}

# 排除的文件名
EXCLUDE_NAMES = {
    'diagnose_host_service.py', 'diagnose_overview.py', 
    'diagnose_service_startup.py', 'diagnose_status.py',
    'test_process_alive.py', 'test_runtime_path.py',
    'test_service_fix.py', 'test_status_fix.py',
    'tmp_query_receipt.py', 'tmp_profile_preview.py', 'tmp_profile_preview2.py',
    'check-env.js', 'start_app.bat', 'stop',
    'UI性能优化说明.md', '前后端连接问题排查指南.md',
    '前端配置说明.md', '启动服务指南.md', '服务器域名访问配置指南.md',
    '状态显示修复说明.md', '硬编码端口修复说明.md', '窗口状态显示修复说明.md',
    '部署指引操作手册.md', 'FinFlowManager_使用说明.md',
    '马克账单表.xlsx', 'database.txt',
    'FinFlowManager.exe',
}

# 排除的扩展名
EXCLUDE_EXTS = {'.pyc', '.pyo', '.log', '.db', '.sqlite3', '.exe', '.dll', '.zip'}


def should_exclude_dir(dirname: str) -> bool:
    """检查目录名是否应该被排除"""
    return dirname in EXCLUDE_DIR_NAMES or dirname.startswith('.')


def should_exclude_path(rel_path: str) -> bool:
    """检查完整路径是否应该被排除"""
    # 转换为正斜杠并小写以便比较
    path_norm = rel_path.replace(os.sep, '/').lower()
    
    # 检查是否在排除路径列表中
    for exclude_path in EXCLUDE_PATHS:
        if path_norm.startswith(exclude_path.lower()):
            return True
    
    # 检查路径中是否包含排除的目录名
    parts = path_norm.split('/')
    for part in parts:
        if part in EXCLUDE_DIR_NAMES:
            return True
    
    return False


def should_exclude_file(filename: str) -> bool:
    """检查文件是否应该被排除"""
    if filename in EXCLUDE_NAMES:
        return True
    
    # 排除打包脚本自身生成的文件
    if filename.startswith('FinFlow_deploy_') and filename.endswith('.zip'):
        return True
    
    name, ext = os.path.splitext(filename)
    
    if ext.lower() in EXCLUDE_EXTS:
        return True
    
    if filename.startswith('.'):
        # 保留 .env.example 和 .env.*.example 模板文件
        if filename == '.env.example' or (filename.startswith('.env.') and filename.endswith('.example')):
            pass
        else:
            return True
    
    if filename.endswith(('.xlsx', '.xls')):
        return True
    
    return False


def create_deploy_package(output_path: str = None) -> Path:
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = PROJECT_ROOT / f'FinFlow_deploy_{timestamp}.zip'
    else:
        output_path = Path(output_path)
    
    print(f'📦 开始打包部署文件...')
    print(f'   项目根目录: {PROJECT_ROOT}')
    print(f'   输出文件: {output_path.name}')
    print()
    
    # 首先扫描预估
    print('   扫描文件中...')
    total_files = 0
    for root, dirs, files in os.walk(PROJECT_ROOT):
        # 过滤目录
        dirs[:] = [d for d in dirs if not should_exclude_dir(d)]
        
        for file in files:
            if should_exclude_file(file):
                continue
            
            file_path = Path(root) / file
            rel_path = str(file_path.relative_to(PROJECT_ROOT))
            
            if should_exclude_path(rel_path):
                continue
            
            total_files += 1
    
    print(f'   预计包含: {total_files} 个文件')
    print()
    
    included_count = 0
    excluded_count = 0
    total_size = 0
    main_dirs = {}
    
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(PROJECT_ROOT):
            # 过滤目录
            dirs[:] = [d for d in dirs if not should_exclude_dir(d)]
            
            for file in files:
                if should_exclude_file(file):
                    excluded_count += 1
                    continue
                
                file_path = Path(root) / file
                rel_path = str(file_path.relative_to(PROJECT_ROOT))
                
                # 检查完整路径
                if should_exclude_path(rel_path):
                    excluded_count += 1
                    continue
                
                # 不打包输出文件本身
                if file_path == output_path:
                    continue
                
                try:
                    file_size = file_path.stat().st_size
                    total_size += file_size
                    
                    arcname = f'FinFlow/{rel_path}'
                    zipf.write(str(file_path), arcname)
                    
                    included_count += 1
                    
                    top_dir = rel_path.split(os.sep)[0] if os.sep in rel_path else ''
                    if top_dir:
                        main_dirs[top_dir] = main_dirs.get(top_dir, 0) + 1
                    
                    if included_count % 50 == 0:
                        pct = (included_count / total_files * 100) if total_files > 0 else 0
                        print(f'   进度: {included_count}/{total_files} ({pct:.1f}%)', end='\r')
                        
                except (OSError, PermissionError):
                    excluded_count += 1
                    continue
    
    zip_size = output_path.stat().st_size if output_path.exists() else 0
    
    print(f'\n\n✅ 打包完成!')
    print(f'   📄 包含文件: {included_count}')
    print(f'   ❌ 排除文件: {excluded_count}')
    print(f'   📊 原始大小: {total_size / 1024 / 1024:.2f} MB')
    print(f'   📦 压缩大小: {zip_size / 1024 / 1024:.2f} MB')
    print(f'\n📋 包含目录:')
    
    for d in sorted(main_dirs.keys()):
        print(f'   📁 {d}/ ({main_dirs[d]} 个文件)')
    
    print(f'\n📍 文件位置: {output_path}')
    
    # 如果压缩包太大，给出警告
    if zip_size > 100 * 1024 * 1024:  # 100MB
        print(f'\n⚠️ 警告: 压缩包超过 100MB，可能包含了不必要的文件!')
        print(f'   请检查是否排除了 node_modules, .venv, dist 等目录')
    
    return output_path


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='打包 FinFlow 部署文件 (仅包含必要文件)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  python pack_deploy.py                    # 默认输出到项目根目录
  python pack_deploy.py -o D:/deploy.zip   # 指定输出路径
        '''
    )
    parser.add_argument('-o', '--output', help='输出文件路径 (默认: FinFlow_deploy_时间戳.zip)')
    args = parser.parse_args()
    
    try:
        result = create_deploy_package(args.output)
        print(f'\n🎉 部署包已生成!')
        print(f'   可将此文件复制到服务器解压部署')
        print(f'\n📝 服务器部署步骤:')
        print(f'   1. 上传压缩包到服务器')
        print(f'   2. 解压: unzip FinFlow_deploy_xxx.zip')
        print(f'   3. 进入目录: cd FinFlow')
        print(f'   4. 配置后端: cp backend/.env.example backend/.env && vim backend/.env')
        print(f'   5. 安装后端依赖: pip install -r backend/requirements.txt')
        print(f'   6. 安装前端依赖: cd frontend && npm install && npm run build')
        print(f'   7. 启动服务')
    except Exception as e:
        print(f'\n❌ 打包失败: {e}', file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
