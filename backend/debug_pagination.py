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
    
    headers = auth.get_auth_headers()
    user_headers = api_record.request_headers
    if isinstance(user_headers, str):
        try: user_headers = json.loads(user_headers)
        except: user_headers = {}
    if isinstance(user_headers, dict):
        for k, v in user_headers.items():
            if isinstance(v, str) and "{access_token}" in v and service.access_token:
                v = v.replace("{access_token}", service.access_token)
            headers[k] = str(v)

    body_500 = {"data": {}, "pageSize": 100, "pageNo": 500}
    body_501 = {"data": {}, "pageSize": 100, "pageNo": 501}

    resp_500 = requests.request("POST", full_url, headers=headers, json=body_500)
    resp_501 = requests.request("POST", full_url, headers=headers, json=body_501)
    
    ids_500 = [r.get("id") for r in resp_500.json().get("data", {}).get("rows", [])]
    ids_501 = [r.get("id") for r in resp_501.json().get("data", {}).get("rows", [])]

    print(f"Len 500: {len(ids_500)}")
    print(f"Len 501: {len(ids_501)}")
    print(f"Are they exactly identical?: {ids_500 == ids_501}")
    
except Exception as e:
    import traceback
    traceback.print_exc()
finally:
    db_session.close()
