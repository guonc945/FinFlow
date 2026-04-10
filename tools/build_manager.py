# -*- coding: utf-8 -*-
"""
FinFlow 管理器打包脚本
打包 finflow_manager.py 为独立的 EXE 文件
"""
import subprocess
import sys
from pathlib import Path

def main():
    tools_dir = Path(__file__).parent.resolve()
    spec_file = tools_dir / "finflow_manager.spec"
    
    print("=" * 60)
    print("FinFlow 管理器打包工具")
    print("=" * 60)
    print(f"工作目录：{tools_dir}")
    print(f"Spec 文件：{spec_file}")
    print()
    
    # 检查 PyInstaller 是否安装
    try:
        import PyInstaller
        print(f"PyInstaller 版本：{PyInstaller.__version__}")
    except ImportError:
        print("错误：未找到 PyInstaller")
        print("请先安装：pip install pyinstaller")
        sys.exit(1)
    
    print()
    print("开始打包...")
    print()
    
    # 运行 PyInstaller
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--clean",
        str(spec_file)
    ]
    
    try:
        result = subprocess.run(
            cmd,
            cwd=str(tools_dir),
            check=True,
            capture_output=False
        )
        
        print()
        print("=" * 60)
        print("打包完成！")
        print("=" * 60)
        
        dist_dir = tools_dir / "dist" / "FinFlow 管理器"
        if dist_dir.exists():
            print(f"输出目录：{dist_dir}")
            print()
            print("打包产物：")
            for file in dist_dir.iterdir():
                size = file.stat().st_size
                if size > 1024 * 1024:
                    size_str = f"{size / (1024 * 1024):.2f} MB"
                else:
                    size_str = f"{size / 1024:.2f} KB"
                print(f"  - {file.name}: {size_str}")
        
        print()
        print("提示：打包后的程序包含以下文件：")
        print("  - FinFlow 管理器.exe (主程序)")
        print("  - finflow_manager_icon.png (图标文件)")
        print("  - finflow_manager_icon.ico (ICO 图标)")
        print()
        
    except subprocess.CalledProcessError as e:
        print()
        print("=" * 60)
        print("打包失败！")
        print("=" * 60)
        print(f"错误代码：{e.returncode}")
        sys.exit(1)
    except Exception as e:
        print()
        print("=" * 60)
        print("打包失败！")
        print("=" * 60)
        print(f"错误：{e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
