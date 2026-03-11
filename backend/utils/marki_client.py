import os
import requests
import json
import logging
import re
import binascii
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
import sys

# 获取项目根目录添加到sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import SessionLocal
from models import ExternalService, ExternalApi

logger = logging.getLogger("marki_client")

class MarkiClient:
    """
    Marki API 集中客户端 (增强版)
    支持自动登录、动态 stoken 获取以及 RSA 加密
    """
    def __init__(self):
        self.appid = "1435186595"
        self.modulus = "b5f53d3e7ab166d99b91bdee1414364e97a5569d9a4da971dcf241e9aec4ee4ee7a27b203f278be7cc695207d19b9209f0e50a3ea367100e06ad635e4ccde6f8a7179d84b7b9b7365a6a7533a9909695f79f3f531ea3c329b7ede2cd9bb9722104e95c0f234f1a72222b0210579f6582fcaa9d8fa62c431a37d88a4899ebce3d"
        self.exponent = 65537
        
        self.cookie = None
        self.username = None
        self.password = None
        
        self.session = requests.Session()
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://charge.markiapp.com",
            "referer": "https://charge.markiapp.com/",
            "x-authtype": "1"
        }
        # 不在初始化时固定加载环境变量
    
    def _load_config(self):
        """动态从数据库加载配置"""
        db = SessionLocal()
        try:
            service = db.query(ExternalService).filter_by(service_name="marki").first()
            if service:
                self.username = service.app_id
                self.password = service.app_secret
                self.cookie = service.extra_info
                
                if self.cookie:
                    self.headers["cookie"] = self.cookie
        finally:
            db.close()

    def _save_cookie(self, new_cookie):
        """将最新的 Cookie 写入数据库"""
        db = SessionLocal()
        try:
            service = db.query(ExternalService).filter_by(service_name="marki").first()
            if service:
                service.extra_info = new_cookie
                db.commit()
                self.cookie = new_cookie
        finally:
            db.close()

    def _encrypt(self, text):
        """实现马克联特定的 RSA 加密 (基于其使用的 BigInt.js Logic)"""
        # 1. 字符串反转
        reversed_text = text[::-1]
        
        # 2. 转换为字节数组并填充
        # 根据 JS 源码：chunkSize = 126 (对于 1024-bit 模数)
        chunk_size = 126 
        a = [ord(c) for c in reversed_text]
        while len(a) < chunk_size:
            a.append(0)
            
        # 3. 构造大整数 (Little Endian)
        # JS 逻辑: block.digits[j] = a[k++] + (a[k++] << 8)
        # 这等同于从字节数组的小端字节序解释为整数
        m_int = int.from_bytes(bytes(a[:chunk_size]), byteorder='little')
        
        # 4. 执行模幂运算 (Raw RSA: m^e mod n)
        n = int(self.modulus, 16)
        e = self.exponent
        crypt_int = pow(m_int, e, n)
        
        # 5. 转换为十六进制字符串，并确保长度为 256 (1024-bit)
        hex_result = hex(crypt_int)[2:].lower()
        # 补全前导零
        hex_result = hex_result.zfill(256)
        
        return hex_result

    def login(self):
        """执行自动登录逻辑"""
        self._load_config()
        if not self.username or not self.password:
            logger.warning("未在集成服务中配置马克系统账密，无法执行自动登录")
            return False

        try:
            logger.info(f"开始自动登录流程 (用户: {self.username})")
            
            # 清理旧会话
            self.session.cookies.clear()
            
            # 1. 获取初始 stoken
            auth_params = (
                "appid=1435186595&thirdAppid=wx326b64f3df7ffb0f&"
                "callback=https%3A%2F%2Fcharge.markiapp.com%2Flogin.html%3FisFrame%3D1&"
                "type=acct&errPos=inputBottom&lang=zh-CN&autoTime=7&bind_mobile=true"
            )
            auth_url = f"https://sttc-os-lgn.markiapp.com/lgn/login/authorize.do?{auth_params}"
            resp = self.session.get(auth_url, timeout=30)
            stoken_match = re.search(r'stoken\s*:\s*["\']([^"\']+)["\']', resp.text)
            if not stoken_match:
                stoken_match = re.search(r'stoken\s*=\s*["\']([^"\']+)["\']', resp.text)
            
            if not stoken_match:
                logger.error(f"无法获取 stoken. 响应大小: {len(resp.text)}")
                return False
            
            stoken = stoken_match.group(1)
            
            # 2. 发送验证请求
            verify_url = "https://sttc-os-lgn.markiapp.com/lgn/login/verify.do"
            params = {
                "appid": self.appid,
                "stoken": stoken,
                "acct": self._encrypt(f"0086{self.username}"),
                "pwd": self._encrypt(self.password),
                "auto": "true",
                "autoTime": "7"
            }
            
            login_resp = self.session.post(verify_url, params=params, timeout=30)
            result = login_resp.json()
            
            rescode = result.get("rescode")
            # 兼容字符串或整数 0
            if str(rescode) == "0":
                logger.info(f"✅ 登录验证成功 (rescode: 0)。")
                
                # 关键步骤：访问回调页面或主页以触发 Cookie 激活
                # 在浏览器中，成功后会跳转回 callback 页面
                callback_url = "https://charge.markiapp.com/login.html?isFrame=1"
                self.session.get(callback_url, timeout=30)
                
                # 访问一个业务 API 确保 Session 已经同步到业务域名
                try:
                    test_url = get_api_url("getCommunityList")
                except Exception:
                    test_url = "https://charge-api.markiapp.com/mkg/api/v2/Charge/getCommunityList"
                self.session.get(test_url, timeout=30)
                
                # 更新内部 cookie 字符串并持久化
                new_cookie = "; ".join([f"{k}={v}" for k, v in self.session.cookies.items()])
                self._save_cookie(new_cookie)
                
                if "cookie" in self.headers:
                    del self.headers["cookie"]
                    
                return True
            else:
                logger.error(f"❌ 登录失败: {result.get('resmsg', '未知错误')} (rescode: {rescode})")
                return False
                
        except Exception as e:
            logger.error(f"❌ 自动登录严重异常: {e}")
            return False

    def request(self, method, url, params=None, json_data=None, timeout=30, retry_on_401=True):
        """统一请求封装"""
        self._load_config()
        try:
            resp = self.session.request(
                method, 
                url, 
                headers=self.headers, 
                params=params, 
                json=json_data, 
                timeout=timeout
            )
            
            # 马克联 602 或 401 状态码处理
            if resp.status_code in [401, 602] and retry_on_401:
                logger.warning(f"检测到 {resp.status_code} Session 过期，尝试自动重登重试...")
                if self.login():
                    return self.request(method, url, params, json_data, timeout, retry_on_401=False)
            
            try:
                return resp.json()
            except json.JSONDecodeError:
                text_peek = resp.text[:200].strip()
                status_code = resp.status_code
                logger.error(f"非 JSON 响应 (状态码: {status_code}). 内容预览: {text_peek or '[空]'}")
                
                # 如果是 200 但内容为空或包含 login，说明可能还是没登上去或被拦截了
                if status_code == 200 and ("login" in text_peek.lower() or not text_peek):
                    if retry_on_401:
                        logger.warning("响应虽为 200 但内容疑似登录页，尝试自动重登...")
                        if self.login():
                            return self.request(method, url, params, json_data, timeout, retry_on_401=False)
                
                resp.raise_for_status()
                raise ValueError(f"解析 JSON 失败 (状态码 {status_code})")
                
        except requests.exceptions.RequestException as e:
            # 处理 HTTP 错误，如 401 等
            status_code = getattr(getattr(e, 'response', None), 'status_code', None)
            if status_code in [401, 602] and retry_on_401:
                if self.login():
                    return self.request(method, url, params, json_data, timeout, retry_on_401=False)
            logger.error(f"网络请求错误: {e}")
            raise

marki_client = MarkiClient()

def get_api_url(api_name: str, preloaded_vars: dict = None) -> str:
    """从数据库中获取对应 api_name 的完整 URL"""
    db = SessionLocal()
    try:
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        if not service:
            raise ValueError("Marki service not found in database")
        api = db.query(ExternalApi).filter_by(service_id=service.id, name=api_name).first()
        if not api:
            raise ValueError(f"API {api_name} not found in database for Marki service")
        # 简单拼接 base_url 和 url_path
        base = service.base_url.rstrip("/")
        path = api.url_path.lstrip("/")
        
        # 解析路径中的变量
        from utils.variable_parser import resolve_variables
        path = resolve_variables(path, db, preloaded_vars=preloaded_vars)
        
        return f"{base}/{path}"
    finally:
        db.close()
