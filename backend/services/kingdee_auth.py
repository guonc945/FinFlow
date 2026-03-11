import os
import requests
import json
import logging
import sys
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# Ensure we can import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import ExternalService
from database import SessionLocal

logger = logging.getLogger("kingdee_auth")
logging.basicConfig(level=logging.INFO)

class KingdeeAuthService:
    def __init__(self, db: Session = None):
        self._db_external = db is None
        self.db = db or SessionLocal()
        self.service_name = "kingdee_oauth"
        
        # Load service config from DB first
        self.service_record = self.db.query(ExternalService).filter_by(service_name=self.service_name).first()
        
        # Fallback to env if not in DB specific fields
        env_app_id = os.getenv("KINGDEE_APP_ID", "")
        env_app_secret = os.getenv("KINGDEE_APP_SECRET", "")
        env_auth_url = os.getenv("KINGDEE_AUTH_URL", "https://api.kingdee.com/jdy/oauth2/token")
        
        if self.service_record:
            self.app_id = self.service_record.app_id or env_app_id
            self.app_secret = self.service_record.app_secret or env_app_secret
            self.auth_url = self.service_record.auth_url or env_auth_url
        else:
            self.app_id = env_app_id
            self.app_secret = env_app_secret
            self.auth_url = env_auth_url
            
            # If completely missing record but we have env, create it to start tracking
            if self.app_id:
                self._init_service_record()

        # Validation
        if not self.app_id or not self.app_secret:
            logger.warning("Kingdee App ID or Secret not configured (DB or Env).")

    def __del__(self):
        if self._db_external:
            self.db.close()

    def _init_service_record(self):
        try:
            new_service = ExternalService(
                service_name=self.service_name,
                display_name="Kingdee K3/Cloud",
                app_id=self.app_id,
                app_secret=self.app_secret,
                auth_url=self.auth_url,
                is_active=True
            )
            self.db.add(new_service)
            self.db.commit()
            self.db.refresh(new_service)
            self.service_record = new_service
            logger.info("Initialized ExternalService record for Kingdee from environment variables.")
        except Exception as e:
            logger.error(f"Failed to init service record: {e}")

    def get_token(self) -> str:
        """
        Get a valid access token.
        Returns cached token if valid, otherwise refreshes/logins.
        """
        # Refresh record
        if not self.service_record:
             self.service_record = self.db.query(ExternalService).filter_by(service_name=self.service_name).first()

        if not self.service_record:
             # Try init again if failed before? Or just try login and it will fail if no creds
             logger.info("No service record, attempting login with env vars if present...")
             return self._login_and_save()
        
        token_record = self.service_record
        now = datetime.now()
        
        # 1. Check if token exists and is valid (with 5 min buffer)
        if token_record.access_token and token_record.expires_at:
            if token_record.expires_at > now + timedelta(minutes=5):
                return token_record.access_token
            
            # Token expired or expiring soon
            logger.info("Token expired or expiring soon, refreshing...")
            return self._refresh_token(token_record)
        
        if token_record.access_token:
             # Exists but no expiry? Refresh.
             logger.info("Token record exists but unsure of expiry, refreshing...")
             return self._refresh_token(token_record)

        # 2. No token record, initial login
        logger.info("No token record found, performing initial login...")
        return self._login_and_save()

    def _login_and_save(self) -> str:
        """
        Perform Client Credentials Grant to get new token and save to DB.
        """
        try:
            # Standard OAuth2 Client Credentials
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.app_id,
                "client_secret": self.app_secret
            }
            
            response = requests.post(self.auth_url, data=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Expected format: { "access_token": "...", "expires_in": 7200, ... }
            if "access_token" not in data:
                raise ValueError(f"Invalid response from Kingdee: {data}")
                
            access_token = data["access_token"]
            expires_in = data.get("expires_in", 7200) # Default 2 hours
            refresh_token = data.get("refresh_token")
            
            # Calculate expiry time
            expires_at = datetime.now() + timedelta(seconds=int(expires_in))
            
            # Save to DB
            self._save_token(access_token, refresh_token, expires_at, data)
            
            return access_token
            
        except Exception as e:
            logger.error(f"Failed to login to Kingdee: {e}")
            raise

    def _refresh_token(self, token_record: ExternalService) -> str:
        """
        Refresh token. 
        If refresh_token is present and supported, use it.
        Otherwise, fall back to re-login (client_credentials).
        """
        # For client_credentials, "refresh" is often just requesting a new token.
        # But if there is a refresh_token, we can try using it.
        if token_record.refresh_token:
            try:
                payload = {
                    "grant_type": "refresh_token",
                    "client_id": self.app_id,
                    "client_secret": self.app_secret,
                    "refresh_token": token_record.refresh_token
                }
                response = requests.post(self.auth_url, data=payload, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if "access_token" in data:
                        access_token = data["access_token"]
                        expires_in = data.get("expires_in", 7200)
                        refresh_token = data.get("refresh_token") or token_record.refresh_token
                        expires_at = datetime.now() + timedelta(seconds=int(expires_in))
                        
                        self._save_token(access_token, refresh_token, expires_at, data)
                        return access_token
            except Exception as e:
                logger.warning(f"Failed to refresh using refresh_token, falling back to full login: {e}")

        # Fallback to fresh login
        return self._login_and_save()

    def _save_token(self, access_token, refresh_token, expires_at, extra_data):
        try:
            # Re-query to attach to session if needed
            token_record = self.db.query(ExternalService).filter_by(service_name=self.service_name).first()
            if not token_record:
                # Should have been inited, but if not
                token_record = ExternalService(
                    service_name=self.service_name,
                    app_id=self.app_id,
                    app_secret=self.app_secret,
                    auth_url=self.auth_url
                )
                self.db.add(token_record)
            
            token_record.access_token = access_token
            token_record.refresh_token = refresh_token
            token_record.expires_at = expires_at
            token_record.token_type = extra_data.get("token_type", "Bearer")
            token_record.scope = extra_data.get("scope", "")
            token_record.extra_info = json.dumps(extra_data)
            token_record.updated_at = datetime.now()
            
            self.db.commit()
            logger.info("Kingdee token saved/updated successfully.")
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Database error saving token: {e}")
            raise

    def get_auth_headers(self):
        """
        Helper to get ready-to-use headers for API calls.
        """
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
