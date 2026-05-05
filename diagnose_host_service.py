# -*- coding: utf-8 -*-
"""FinFlow 宿主服务状态诊断脚本"""
import sys
import os
import json
from pathlib import Path
from datetime import datetime

# 添加 tools 目录到路径
tools_dir = Path(__file__).resolve().parent / "tools"
sys.path.insert(0, str(tools_dir))

from finflow_manager import (
    discover_root_dir,
    read_runtime_state,
    read_managed_runtime_state,
    is_process_alive,
    ROOT_DIR,
    RUNTIME_STATE_PATH,
    SERVICE_HOST_LOG,
    query_windows_service_info,
    WINDOWS_SERVICE_NAME,
)

def print_section(title):
    """打印章节标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def check_root_dir():
    """检查项目根目录"""
    print_section("1. 项目根目录检查")
    print(f"ROOT_DIR: {ROOT_DIR}")
    print(f"discover_root_dir(): {discover_root_dir()}")
    print(f"ROOT_DIR 存在：{ROOT_DIR.exists()}")
    print(f"backend/main.py 存在：{(ROOT_DIR / 'backend' / 'main.py').exists()}")
    print(f"frontend 目录存在：{(ROOT_DIR / 'frontend').exists()}")
    print(f"tools 目录存在：{(ROOT_DIR / 'tools').exists()}")

def check_runtime_state():
    """检查运行时状态"""
    print_section("2. 运行时状态检查")
    print(f"RUNTIME_STATE_PATH: {RUNTIME_STATE_PATH}")
    print(f"文件存在：{RUNTIME_STATE_PATH.exists()}")
    
    if RUNTIME_STATE_PATH.exists():
        state = read_runtime_state()
        print(f"\n运行时状态内容:")
        print(f"  service_host_pid: {state.get('service_host_pid', 0)}")
        print(f"  backend_pid: {state.get('backend_pid', 0)}")
        print(f"  frontend_pid: {state.get('frontend_pid', 0)}")
        print(f"  host_started_at: {state.get('host_started_at', 0)}")
        print(f"  host_last_transition: {state.get('host_last_transition', '')}")
        print(f"  user_stopped: {state.get('user_stopped', False)}")
        
        # 检查进程存活状态
        host_pid = int(state.get('service_host_pid', 0))
        backend_pid = int(state.get('backend_pid', 0))
        frontend_pid = int(state.get('frontend_pid', 0))
        
        print(f"\n进程存活状态:")
        print(f"  宿主进程 (PID {host_pid}): {is_process_alive(host_pid)}")
        print(f"  后端进程 (PID {backend_pid}): {is_process_alive(backend_pid)}")
        print(f"  前端进程 (PID {frontend_pid}): {is_process_alive(frontend_pid)}")

def check_managed_state():
    """检查管理状态"""
    print_section("3. 管理状态检查")
    state = read_managed_runtime_state()
    print(f"service_host_alive: {state.get('service_host_alive', False)}")
    print(f"backend_alive: {state.get('backend_alive', False)}")
    print(f"frontend_alive: {state.get('frontend_alive', False)}")

def check_windows_service():
    """检查 Windows 服务状态"""
    print_section("4. Windows 服务检查")
    service_info = query_windows_service_info()
    print(f"服务名称：{WINDOWS_SERVICE_NAME}")
    print(f"已安装：{service_info.get('installed', False)}")
    print(f"运行状态：{service_info.get('state', '')}")
    print(f"正在运行：{service_info.get('running', False)}")
    print(f"服务 PID: {service_info.get('pid', 0)}")
    print(f"启动类型：{service_info.get('start_type', '')}")
    print(f"二进制路径：{service_info.get('binary_path', '')}")
    
    if service_info.get('detail'):
        print(f"\n服务详细信息:")
        print(service_info.get('detail'))

def check_processes():
    """检查进程状态"""
    print_section("5. 进程状态检查")
    import subprocess
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq FinFlow*'], 
                              capture_output=True, text=True, encoding='gbk')
        print("FinFlow 相关进程:")
        print(result.stdout)
    except Exception as e:
        print(f"检查进程失败：{e}")

def check_service_log():
    """检查服务日志"""
    print_section("6. 服务日志检查")
    print(f"日志文件路径：{SERVICE_HOST_LOG}")
    print(f"日志文件存在：{SERVICE_HOST_LOG.exists()}")
    
    if SERVICE_HOST_LOG.exists():
        try:
            with open(SERVICE_HOST_LOG, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print(f"\n日志文件最后 50 行:")
                for line in lines[-50:]:
                    print(line.rstrip())
        except Exception as e:
            print(f"读取日志失败：{e}")
    else:
        print("日志文件不存在")

def check_configuration():
    """检查配置文件"""
    print_section("7. 配置文件检查")
    
    # 检查 .env 文件
    env_path = ROOT_DIR / "backend" / ".env"
    print(f"backend/.env 存在：{env_path.exists()}")
    
    # 检查 .encryption.key 文件
    key_path = ROOT_DIR / "backend" / ".encryption.key"
    print(f"backend/.encryption.key 存在：{key_path.exists()}")
    
    # 检查虚拟环境
    venv_python = ROOT_DIR / "backend" / ".venv" / "Scripts" / "python.exe"
    print(f"backend/.venv/Scripts/python.exe 存在：{venv_python.exists()}")
    
    # 检查前端依赖
    frontend_package = ROOT_DIR / "frontend" / "package.json"
    print(f"frontend/package.json 存在：{frontend_package.exists()}")
    
    vite_cli = ROOT_DIR / "frontend" / "node_modules" / "vite" / "bin" / "vite.js"
    print(f"frontend/node_modules/vite/bin/vite.js 存在：{vite_cli.exists()}")

def check_permissions():
    """检查权限设置"""
    print_section("8. 权限检查")
    import subprocess
    
    # 检查 EXE 文件权限
    exe_path = ROOT_DIR / "FinFlowManagerHost.exe"
    if not exe_path.exists():
        exe_path = ROOT_DIR / "FinFlowManagerHost" / "FinFlowManagerHost.exe"
    
    if exe_path.exists():
        try:
            result = subprocess.run(['icacls', str(exe_path)], 
                                  capture_output=True, text=True, encoding='gbk')
            print(f"{exe_path} 的权限:")
            print(result.stdout)
        except Exception as e:
            print(f"检查权限失败：{e}")
    else:
        print(f"EXE 文件不存在：{exe_path}")

def diagnose():
    """执行完整诊断"""
    print("=" * 70)
    print("  FinFlow 宿主服务状态诊断报告")
    print(f"  生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    check_root_dir()
    check_runtime_state()
    check_managed_state()
    check_windows_service()
    check_processes()
    check_service_log()
    check_configuration()
    check_permissions()
    
    print_section("诊断总结")
    
    # 检查关键问题
    issues = []
    
    # 检查 Windows 服务
    service_info = query_windows_service_info()
    if not service_info.get('installed'):
        issues.append("❌ Windows 服务未安装")
    elif not service_info.get('running'):
        issues.append("⚠️  Windows 服务已安装但未运行")
    else:
        issues.append("✅ Windows 服务正在运行")
    
    # 检查运行时状态
    state = read_runtime_state()
    host_pid = int(state.get('service_host_pid', 0))
    if host_pid > 0:
        if is_process_alive(host_pid):
            issues.append("✅ 宿主进程正在运行")
        else:
            issues.append("❌ 宿主进程已停止 (PID 记录存在但进程不存在)")
    else:
        issues.append("❌ 宿主进程未启动 (PID 为 0)")
    
    # 检查前后端
    backend_pid = int(state.get('backend_pid', 0))
    frontend_pid = int(state.get('frontend_pid', 0))
    
    if backend_pid > 0 and is_process_alive(backend_pid):
        issues.append("✅ 后端服务正在运行")
    else:
        issues.append("❌ 后端服务未运行")
    
    if frontend_pid > 0 and is_process_alive(frontend_pid):
        issues.append("✅ 前端服务正在运行")
    else:
        issues.append("❌ 前端服务未运行")
    
    # 打印总结
    for issue in issues:
        print(issue)
    
    print("\n" + "=" * 70)
    print("诊断完成")
    print("=" * 70)

if __name__ == "__main__":
    diagnose()
