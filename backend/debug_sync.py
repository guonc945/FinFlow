import json
import requests
from database import SessionLocal
import models
from services.external_auth import ExternalAuthService

db_session = SessionLocal()
try:
    api_record = db_session.query(models.ExternalApi).filter(models.ExternalApi.name == "金蝶云星空查询客户").first()
    service_id = api_record.service_id
    service = db_session.query(models.ExternalService).filter(models.ExternalService.id == service_id).first()
    
    auth = ExternalAuthService(db=db_session, service_record=service)
    auth.get_token()
    
    full_url = api_record.url_path
    if not full_url.startswith("http"):
        full_url = (service.base_url or "") + full_url
    
    body = {"data": {}, "pageSize": 2000, "pageNo": 1}

    headers = auth.get_auth_headers()
    user_headers = api_record.request_headers
    if isinstance(user_headers, str):
        user_headers = json.loads(user_headers)
    for k, v in user_headers.items():
        if isinstance(v, str) and "{access_token}" in v and service.access_token:
            v = v.replace("{access_token}", service.access_token)
        headers[k] = str(v)

    resp = requests.request("POST", full_url, headers=headers, json=body)
    
    data = resp.json()
    with open("kingdee_debug.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

except Exception as e:
    import traceback
    traceback.print_exc()
finally:
    db_session.close()
