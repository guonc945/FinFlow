import sys, requests
sys.path.append(r"D:\FinFlow\backend")
import utils.auth as auth

token = auth.create_access_token({'sub': 1})
headers={'Authorization': f'Bearer {token}', 'Origin':'http://localhost:5273'}
base='http://localhost:8110'
for path in ['/api/users/me','/api/projects?skip=0&limit=2000&current_account_book_only=true','/api/deposit-records?skip=0&limit=25','/api/receipt-bills?skip=0&limit=25']:
    r=requests.get(base+path,headers=headers,timeout=10)
    print(path, '->', r.status_code, 'ACAO=', r.headers.get('Access-Control-Allow-Origin'))
    print(r.text[:400].replace('\n',' '))
