# -*- coding: utf-8 -*-
"""诊断服务启动问题"""
import sys
import os
import json
import time
import subprocess
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
    query_windows_service_info,
    ROOT_DIR,
    RUNTIME_STATE_PATH,
    SERVICE_HOST_LOG,
)

def print_section(title):
    """打印章节标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def check_windows_service():
    """检查 Windows 服务状态"""
    print_section("1. Windows 服务状态")
    service_info = query_windows_service_info()
    print(f"已安装：{service_info.get('installed', False)}")
    print(f"运行状态：{service_info.get('state', '未知')}")
    print(f"正在运行：{service_info.get('running', False)}")
    print(f"服务 PID: {service_info.get('pid', 0)}")
    print(f"二进制路径：{service_info.get('binary_path', '')}")
    
    return service_info

def check_runtime_state():
    """检查运行时状态"""
    print_section("2. 运行时状态")
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
        print(f"  host_last_heartbeat_at: {state.get('host_last_heartbeat_at', 0)}")
        print(f"  user_stopped: {state.get('user_stopped', False)}")
        
        # 检查进程存活状态
        host_pid = int(state.get('service_host_pid', 0))
        backend_pid = int(state.get('backend_pid', 0))
        frontend_pid = int(state.get('frontend_pid', 0))
        
        print(f"\n进程存活状态:")
        print(f"  宿主进程 (PID {host_pid}): {is_process_alive(host_pid)}")
        print(f"  后端进程 (PID {backend_pid}): {is_process_alive(backend_pid)}")
        print(f"  前端进程 (PID {frontend_pid}): {is_process_alive(frontend_pid)}")
        
        return state
    return {}

def check_processes():
    """检查进程状态"""
    print_section("3. 进程状态检查")
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq FinFlow*'], 
                              capture_output=True, text=True, encoding='gbk')
        print("FinFlow 相关进程:")
        print(result.stdout if result.stdout else "  无 FinFlow 进程")
    except Exception as e:
        print(f"检查进程失败：{e}")
    
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq python*'], 
                              capture_output=True, text=True, encoding='gbk')
        print("\nPython 进程:")
        lines = result.stdout.split('\n') if result.stdout else []
        for line in lines[:10]:  # 只显示前10行
            if line.strip():
                print(f"  {line}")
    except Exception as e:
        print(f"检查 Python 进程失败：{e}")
    
    try:
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq node*'], 
                              capture_output=True, text=True, encoding='gbk')
        print("\nNode 进程:")
        lines = result.stdout.split('\n') if result.stdout else []
        for line in lines[:10]:  # 只显示前10行
            if line.strip():
                print(f"  {line}")
    except Exception as e:
        print(f"检查 Node 进程失败：{e}")

def check_service_log():
    """检查服务日志"""
    print_section("4. 服务日志检查")
    print(f"日志文件路径：{SERVICE_HOST_LOG}")
    print(f"日志文件存在：{SERVICE_HOST_LOG.exists()}")
    
    if SERVICE_HOST_LOG.exists():
        try:
            with open(SERVICE_HOST_LOG, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print(f"\n日志文件最后 30 行:")
                for line in lines[-30:]:
                    print(line.rstrip())
        except Exception as e:
            print(f"读取日志失败：{e}")
    else:
        print("日志文件不存在")

def check_ports():
    """检查端口占用"""
    print_section("5. 端口占用检查")
    ports = [8110, 5273]
    for port in ports:
        try:
            result = subprocess.run(['netstat', '-ano'], capture_output=True, text=True, encoding='gbk')
            lines = [line for line in result.stdout.split('\n') if f':{port}' in line]
            if lines:
                print(f"\n端口 {port} 占用情况:")
                for line in lines:
                    print(f"  {line.strip()}")
            else:
                print(f"\n端口 {port}: 未被占用")
        except Exception as e:
            print(f"检查端口 {port} 失败：{e}")

def check_configuration():
    """检查配置文件"""
    print_section("6. 配置文件检查")
    
    # 检查 manager_state.json
    manager_state_path = ROOT_DIR / "deploy" / "windows" / "runtime" / "manager_state.json"
    print(f"\nmanager_state.json:")
    print(f"  路径：{manager_state_path}")
    print(f"  存在：{manager_state_path.exists()}")
    
    if manager_state_path.exists():
        try:
            with open(manager_state_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"  start_backend_on_launch: {config.get('start_backend_on_launch', False)}")
            print(f"  start_frontend_on_launch: {config.get('start_frontend_on_launch', False)}")
        except Exception as e:
            print(f"  读取失败：{e}")

def diagnose():
    """执行完整诊断"""
    print("=" * 70)
    print("  FinFlow 服务启动问题诊断报告")
    print(f"  生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    service_info = check_windows_service()
    state = check_runtime_state()
    check_processes()
    check_service_log()
    check_ports()
    check_configuration()
    
    print_section("诊断总结")
    
    issues = []
    
    # 检查 Windows 服务
    if not service_info.get('installed'):
        issues.append("❌ Windows 服务未安装")
    elif not service_info.get('running'):
        issues.append("⚠️  Windows 服务已安装但未运行")
    else:
        issues.append("✅ Windows 服务正在运行")
    
    # 检查运行时状态
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
    
    # 提供建议
    print("\n📋 建议操作：")
    if not service_info.get('running'):
        print("1. 以管理员身份运行 CMD 或 PowerShell")
        print("2. 执行：sc start FinFlowManagerHost")
        print("3. 等待 5 秒后再次运行此诊断脚本")
    elif not is_process_alive(host_pid):
        print("1. 服务已启动但进程不存在，可能是启动失败")
        print("2. 查看服务日志了解详细错误")
        print("3. 尝试重启服务：sc stop FinFlowManagerHost && sc start FinFlowManagerHost")
    elif not is_process_alive(backend_pid) or not is_process_alive(frontend_pid):
        print("1. 服务已启动但前后端未启动")
        print("2. 检查配置文件中的 start_backend_on_launch 和 start_frontend_on_launch 是否为 true")
        print("3. 检查端口 8110 和 5273 是否被占用")

if __name__ == "__main__":
    diagnose()
