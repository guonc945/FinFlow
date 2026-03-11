
import sys
import os
import logging

# Add parent dir to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.kingdee_auth import KingdeeAuthService
from database import SessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_auth():
    print("Testing Kingdee Auth...")
    
    # Check env vars
    app_id = os.getenv("KINGDEE_APP_ID")
    app_secret = os.getenv("KINGDEE_APP_SECRET")
    
    if not app_id or not app_secret:
        print("ERROR: KINGDEE_APP_ID or KINGDEE_APP_SECRET not found in environment.")
        print("Please add them to backend/.env")
        return

    db = SessionLocal()
    try:
        service = KingdeeAuthService(db)
        token = service.get_token()
        print(f"Successfully retrieved token: {token[:10]}... (truncated)")
        print("Token management is working.")
        
        # Test headers
        headers = service.get_auth_headers()
        print(f"Auth Headers: {headers}")
        
    except Exception as e:
        print(f"Auth failed: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    # Load .env from backend/
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(env_path)
    
    test_auth()
