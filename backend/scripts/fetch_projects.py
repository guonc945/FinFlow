import os
import json
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import sys

# Add parent directory to sys.path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.marki_client import marki_client, get_api_url
from database import SessionLocal

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logger = logging.getLogger("project_sync")

# Build DATABASE_URL
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    if not all([DB_NAME, DB_USER, DB_PASSWORD]):
        raise RuntimeError('Database credentials are incomplete in .env')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

URL = get_api_url("getCommunityList")

def fetch_projects():
    """从马克联获取项目（园区）列表"""
    db = SessionLocal()
    try:
        logger.info(f"正在从马克联拉取项目列表...")
        
        # 动态获取 API 配置
        from models import ExternalApi, ExternalService
        from utils.variable_parser import resolve_dict_variables
        
        service = db.query(ExternalService).filter_by(service_name="marki").first()
        api = db.query(ExternalApi).filter_by(service_id=service.id, name="getCommunityList").first()
        
        if not service:
            logger.error("未找到 marki 集成服务配置")
            return []

        url = get_api_url("getCommunityList")
        method = api.method if api else "GET"
        
        # 准备变量
        system_id = os.getenv('MARKI_SYSTEM_ID', '')
        preloaded_vars = {"chargeSystemID": system_id}
        
        # 处理请求体/参数
        request_data = {}
        if api and api.request_body:
            request_data = resolve_dict_variables(json.loads(api.request_body), db, preloaded_vars=preloaded_vars)
        else:
            request_data = preloaded_vars if system_id else {}

        if method == "GET":
            result = marki_client.request("GET", url, params=request_data)
        else:
            result = marki_client.request(method, url, json_data=request_data)
        
        projects_data = []
        if isinstance(result, list):
            projects_data = result
        elif isinstance(result, dict):
            if "data" in result:
                data = result["data"]
                if isinstance(data, dict) and "list" in data:
                    projects_data = data["list"]
                elif isinstance(data, list):
                    projects_data = data
            elif "list" in result:
                projects_data = result["list"]
        
        logger.info(f"拉取成功，提取到 {len(projects_data)} 个项目。")
        return projects_data
    except Exception as e:
        logger.error(f"拉取项目列表失败: {e}")
        return []
    finally:
        db.close()

def upsert_projects(projects):
    """更新或插入项目数据到数据库"""
    if not projects:
        logger.warning("没有可同步的项目数据。")
        return
        
    insert_sql = text('''
        INSERT INTO projects_lists (proj_id, proj_name)
        VALUES (:proj_id, :proj_name)
        ON CONFLICT (proj_id) DO UPDATE SET proj_name = EXCLUDED.proj_name
    ''')
    
    success_count = 0
    with engine.begin() as conn:
        for proj in projects:
            # 兼容多种可能的字段名
            proj_id = proj.get('id') or proj.get('communityID') or proj.get('proj_id')
            proj_name = proj.get('name') or proj.get('communityName') or proj.get('proj_name')
            
            if proj_id is None or proj_name is None:
                continue
                
            try:
                # 数据库中 proj_id 是 INTEGER，强制转为 int
                safe_proj_id = int(proj_id)
                conn.execute(insert_sql, {'proj_id': safe_proj_id, 'proj_name': str(proj_name)})
                success_count += 1
            except (ValueError, TypeError):
                logger.error(f"跳过无效的项目 ID: {proj_id}")
            except SQLAlchemyError as db_err:
                logger.error(f"项目 {proj_id} 存入数据库失败: {db_err}")

    logger.info(f"项目同步完成: 成功处理 {success_count} 个项目。")

def main():
    projects = fetch_projects()
    upsert_projects(projects)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
