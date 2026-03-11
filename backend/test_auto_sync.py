import sys
import os
import logging

# 添加后端目录到路径
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)

# 配置日志到标准输出
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from utils.marki_client import marki_client
from fetch_bills import sync_bills

def test_auto_sync():
    print("--- 马克联自动登录与同步测试 ---")
    
    # 强制清理 Cookie 来触发重登
    marki_client.cookie = "" 
    marki_client.update_headers()
    
    try:
        # 尝试同步默认园区
        sync_bills([10956])
        print("✅ 测试成功！")
    except Exception as e:
        print(f"❌ 测试失败: {e}")

if __name__ == "__main__":
    test_auto_sync()
