import sys
sys.path.append(r"D:\FinFlow\backend")
from fastapi.testclient import TestClient
import main
import utils.auth as auth

token = auth.create_access_token({'sub': 3})
client = TestClient(main.app)
headers = {'Authorization': f'Bearer {token}', 'Origin': 'http://localhost:5273'}
for path in ['/api/projects?skip=0&limit=2000&current_account_book_only=true','/api/deposit-records?skip=0&limit=25','/api/receipt-bills?skip=0&limit=25','/api/prepayment-records?skip=0&limit=25']:
    r = client.get(path, headers=headers)
    print(path, r.status_code, r.headers.get('access-control-allow-origin'))
