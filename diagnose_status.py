# -*- coding: utf-8 -*-
"""诊断状态显示问题"""
import sys
import os
from pathlib import Path

tools_dir = Path(__file__).resolve().parent / "tools"
sys.path.insert(0, str(tools_dir))

from finflow_manager import (
    read_runtime_state,
    read_managed_runtime_state,
    is_process_alive,
    RUNTIME_STATE_PATH,
)

def main():
    print("=" * 60)
    print("FinFlow 状态诊断")
    print("=" * 60)
    
    # 1. 检查运行时状态文件
    print(f"\n1. 运行时状态文件: {RUNTIME_STATE_PATH}")
    print(f"   文件存在: {RUNTIME_STATE_PATH.exists()}")
    
    if RUNTIME_STATE_PATH.exists():
        # 2. 读取原始状态
        state = read_runtime_state()
        print(f"\n2. 运行时状态内容:")
        print(f"   service_host_pid: {state.get('service_host_pid', 0)}")
        print(f"   backend_pid: {state.get('backend_pid', 0)}")
        print(f"   frontend_pid: {state.get('frontend_pid', 0)}")
        print(f"   service_host_alive: {state.get('service_host_alive', False)}")
        print(f"   backend_alive: {state.get('backend_alive', False)}")
        print(f"   user_stopped: {state.get('user_stopped', False)}")
        
        # 3. 检测进程实际状态
        host_pid = int(state.get('service_host_pid', 0))
        backend_pid = int(state.get('backend_pid', 0))
        
        print(f"\n3. 进程实际状态:")
        print(f"   主控 PID {host_pid} 存活: {is_process_alive(host_pid)}")
        print(f"   后端 PID {backend_pid} 存活: {is_process_alive(backend_pid)}")
        
        # 4. 模拟 ManagedBackendController 的检测逻辑
        print(f"\n4. ManagedBackendController.is_running() 逻辑:")
        host_alive = is_process_alive(host_pid)
        backend_alive = is_process_alive(backend_pid)
        is_running = host_alive or backend_alive
        print(f"   host_alive: {host_alive}")
        print(f"   backend_alive: {backend_alive}")
        print(f"   is_running: {is_running}")
        
        # 5. 模拟 poll_status 逻辑
        print(f"\n5. poll_status() 应该返回:")
        if host_alive and backend_alive:
            print(f"   '运行中 (主控 PID {host_pid}, 后端 PID {backend_pid})'")
        elif host_alive and not backend_alive:
            print(f"   '主控进程运行中 (PID {host_pid})，等待后端就绪'")
        elif backend_alive and not host_alive:
            print(f"   '后端运行中 (PID {backend_pid})，但主控进程已丢失'")
        elif state.get("last_exit_code") is not None:
            print(f"   '未运行 (上次退出 code {state['last_exit_code']})'")
        else:
            print(f"   '未运行'")
    else:
        print("   运行时状态文件不存在！")
    
    print("\n" + "=" * 60)
    print("诊断完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
