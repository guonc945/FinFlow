# -*- coding: utf-8 -*-
"""诊断概览页面状态显示问题"""
import sys
import os
from pathlib import Path

tools_dir = Path(__file__).resolve().parent / "tools"
sys.path.insert(0, str(tools_dir))

from finflow_manager import (
    discover_root_dir,
    read_managed_runtime_state,
    is_process_alive,
    ROOT_DIR,
    RUNTIME_STATE_PATH,
)

def main():
    print("=" * 60)
    print("FinFlow 概览页面状态诊断")
    print("=" * 60)
    
    print(f"\n1. 项目根目录: {ROOT_DIR}")
    print(f"   discover_root_dir(): {discover_root_dir()}")
    print(f"   文件存在: {ROOT_DIR.exists()}")
    
    print(f"\n2. 运行时状态文件: {RUNTIME_STATE_PATH}")
    print(f"   文件存在: {RUNTIME_STATE_PATH.exists()}")
    
    if RUNTIME_STATE_PATH.exists():
        state = read_managed_runtime_state()
        print(f"\n3. 运行时状态内容:")
        print(f"   service_host_pid: {state.get('service_host_pid', 0)}")
        print(f"   backend_pid: {state.get('backend_pid', 0)}")
        print(f"   frontend_pid: {state.get('frontend_pid', 0)}")
        
        host_pid = int(state.get('service_host_pid', 0))
        backend_pid = int(state.get('backend_pid', 0))
        frontend_pid = int(state.get('frontend_pid', 0))
        
        print(f"\n4. 进程存活状态:")
        print(f"   宿主 PID {host_pid}: {is_process_alive(host_pid)}")
        print(f"   后端 PID {backend_pid}: {is_process_alive(backend_pid)}")
        print(f"   前端 PID {frontend_pid}: {is_process_alive(frontend_pid)}")
        
        # 模拟 get_host_status_text 逻辑
        print(f"\n5. 模拟 get_host_status_text():")
        host_alive = is_process_alive(host_pid)
        backend_alive = is_process_alive(backend_pid)
        frontend_alive = is_process_alive(frontend_pid)
        
        if host_alive:
            details = [f"PID {host_pid}"]
            if backend_alive or backend_pid > 0:
                details.append(f"backend {backend_pid}")
            if frontend_alive or frontend_pid > 0:
                details.append(f"frontend {frontend_pid}")
            print(f"   结果: 运行中 ({', '.join(details)})")
        elif backend_alive or frontend_alive or backend_pid > 0 or frontend_pid > 0:
            fragments = []
            if backend_alive or backend_pid > 0:
                fragments.append(f"backend {backend_pid}")
            if frontend_alive or frontend_pid > 0:
                fragments.append(f"frontend {frontend_pid}")
            print(f"   结果: 宿主未运行，但子进程存活 ({', '.join(fragments)})")
        else:
            print(f"   结果: 未运行")
    else:
        print("\n   运行时状态文件不存在！")
    
    print("\n" + "=" * 60)
    print("诊断完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
