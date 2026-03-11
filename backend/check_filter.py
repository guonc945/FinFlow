import requests, json
from database import SessionLocal
import models
from services.external_auth import ExternalAuthService

db = SessionLocal()
api = db.query(models.ExternalApi).filter(models.ExternalApi.name == '辅助资料查询').first()
svc = db.query(models.ExternalService).filter(models.ExternalService.id == api.service_id).first()
auth = ExternalAuthService(db=db, service_record=svc)
auth.get_token()

headers = auth.get_auth_headers()
headers.update(json.loads(api.request_headers))
for k, v in headers.items():
    if '{access_token}' in str(v):
        headers[k] = str(v).replace('{access_token}', svc.access_token)

body1 = {'data': {'filter': "group_number in ('companytrade', 'itemmodel')"}, 'pageSize': 100}
r1 = requests.post(api.url_path, headers=headers, json=body1)
d1 = r1.json()
print('Total rows for group_number in (...):', len(d1.get('data', {}).get('rows', [])))

body2 = {'data': {'filter': "FGroup.FNumber in ('companytrade', 'itemmodel')"}, 'pageSize': 100}
r2 = requests.post(api.url_path, headers=headers, json=body2)
d2 = r2.json()
print('Total rows for FGroup.FNumber in (...):', len(d2.get('data', {}).get('rows', [])))
