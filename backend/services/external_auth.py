# -*- coding: utf-8 -*-
import os
import requests
import json
import logging
import sys
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict, Any

# Ensure we can import from parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import ExternalService
from database import SessionLocal
from utils.crypto import decrypt_value, encrypt_value
from utils.variable_parser import resolve_variables


logger = logging.getLogger("external_auth")
logging.basicConfig(level=logging.INFO)

class ExternalAuthService:
    """
    Generic service to manage OAuth2 Client Credential tokens for any defined ExternalService.
    Usage:
        auth = ExternalAuthService(service_name="kingdee_main")
        token = auth.get_token()
    """
    def __init__(self, service_name: str = None, db: Session = None, service_record: ExternalService = None, user_context: Optional[Dict[str, str]] = None):
        self._db_external = db is None
        self.db = db or SessionLocal()
        self.service_name = service_name
        self.service_record = service_record
        self.user_context = user_context
        
        # Sync service_name if only record is provided
        if self.service_record and not self.service_name:
            self.service_name = self.service_record.service_name or self.service_record.display_name

        
        # Load service config if not provided but name is
        if not self.service_record and self.service_name:
            self._load_service_record()


    def __del__(self):
        if self._db_external:
            self.db.close()

    def _load_service_record(self):
        self.service_record = self.db.query(ExternalService).filter_by(service_name=self.service_name).first()
        if not self.service_record:
            logger.warning(f"External service '{self.service_name}' not found in database.")

    def get_token(self) -> str:
        """
        Get a valid access token.
        Returns cached token if valid, otherwise refreshes/logins.
        NOTE: This is primarily for OAuth2 flows. For other types, it might just return the secret.
        """
        if not self.service_record:
            self._load_service_record()
            if not self.service_record:
                 raise ValueError(f"Service '{self.service_name}' does not exist.")
        
        # If not oauth2, we might not need to do the expiry check dance
        if self.service_record.auth_type and self.service_record.auth_type != 'oauth2':
            return decrypt_value(self.service_record.app_secret)

        now = datetime.now()
        
        # 1. Check if token exists and is valid (with 5 min buffer)
        # 1. Check if token exists and is valid (with 5 min buffer)
        if self.service_record.access_token and self.service_record.expires_at:
            if self.service_record.expires_at > now + timedelta(minutes=5):
                return decrypt_value(self.service_record.access_token)
            
            # Token expired or expiring soon
            logger.info(f"[{self.service_name}] Token expired or expiring soon (Expires: {self.service_record.expires_at}), refreshing...")
            try:
                return self._refresh_token()
            except Exception as e:
                logger.warning(f"Refresh failed, trying full login: {e}")
                return self._login_and_save()
        
        if self.service_record.access_token:
             # Exists but no expiry? Refresh to be safe.
             logger.info(f"[{self.service_name}] Token record exists but unsure of expiry, refreshing...")
             try:
                 return self._refresh_token()
             except:
                 return self._login_and_save()

        # 2. No token record, initial login
        logger.info(f"[{self.service_name}] No token record found, performing initial login...")
        return self._login_and_save()

    def _login_and_save(self) -> str:
        """
        Perform Client Credentials Grant to get new token and save to DB.
        Only applicable for OAuth2.
        """
        if not self.service_record:
             raise ValueError(f"Service {self.service_name} not configured.")
             
        if self.service_record.auth_type and self.service_record.auth_type != 'oauth2':
            # For non-oauth2, 'login' might just be validating? 
            # Or we just assume the secret is the token.
            return decrypt_value(self.service_record.app_secret)

        if not self.service_record.auth_url:
            # If no Auth URL provided, we assume the App Secret is a static token
            logger.info(f"[{self.service_name}] No Auth URL provided for OAuth2. Using App Secret as static token.")
            return decrypt_value(self.service_record.app_secret)

        missing = []
        if not self.service_record.app_id: missing.append("App ID")
        if not self.service_record.app_secret: missing.append("App Secret")
        
        if missing:
            raise ValueError(f"Service '{self.service_name}' missing required fields for OAuth2: {', '.join(missing)}")



        try:
            # Resolve variables in key fields before preparing payload
            auth_url = resolve_variables(self.service_record.auth_url, self.db, user_context=self.user_context)
            client_id = resolve_variables(self.service_record.app_id, self.db, user_context=self.user_context)
            client_secret = resolve_variables(decrypt_value(self.service_record.app_secret), self.db, user_context=self.user_context)
            
            # Prepare payload and headers
            payload = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
            
            # Merge custom body if present
            if self.service_record.auth_body:
                try:
                    resolved_body_str = resolve_variables(self.service_record.auth_body, self.db, user_context=self.user_context)
                    custom_body = json.loads(resolved_body_str)
                    payload.update(custom_body)
                except Exception as e:
                    logger.error(f"Failed to parse auth_body JSON: {e}")

            headers = {"Content-Type": "application/x-www-form-urlencoded"} # Default
            if self.service_record.auth_headers:
                try:
                    resolved_headers_str = resolve_variables(self.service_record.auth_headers, self.db, user_context=self.user_context)
                    custom_headers = json.loads(resolved_headers_str)
                    headers.update(custom_headers)
                except Exception as e:
                    logger.error(f"Failed to parse auth_headers JSON: {e}")



            method = (self.service_record.auth_method or "POST").upper()
            logger.info(f"[{self.service_name}] Sending {method} auth request to: {auth_url}")
            
            if method == "GET":
                response = requests.get(auth_url, params=payload, headers=headers, timeout=30)
            else:
                if headers.get("Content-Type") == "application/json":
                    response = requests.post(auth_url, json=payload, headers=headers, timeout=30)
                else:
                    response = requests.post(auth_url, data=payload, headers=headers, timeout=30)
            


            response.raise_for_status()



            data = response.json()
            
            # Handle nested structures (data/result) commonly used in enterprise APIs
            token_data = data
            if "access_token" not in data:
                if isinstance(data.get("data"), dict) and "access_token" in data["data"]:
                    token_data = data["data"]
                elif isinstance(data.get("result"), dict) and "access_token" in data["result"]:
                    token_data = data["result"]
                
            access_token = token_data.get("access_token")
            if not access_token:
                raise ValueError(f"Invalid response format from {self.service_name}: Missing 'access_token'. Response: {data}")
                
            expires_in = token_data.get("expires_in", 7200)
            refresh_token = token_data.get("refresh_token")
            
            # Calculate expiry time
            try:
                expires_at = datetime.now() + timedelta(seconds=int(expires_in))
            except ValueError:
                expires_at = datetime.now() + timedelta(seconds=7200)
            
            # Save to DB (using the original data for extra_info to preserve full context)
            self._save_token(access_token, refresh_token, expires_at, data)

            
            return access_token
            
        except Exception as e:
            logger.error(f"[{self.service_name}] Failed to login: {e}")
            raise

    def _refresh_token(self) -> str:
        """
        Refresh token using refresh_token if available, else re-login.
        Only applicable for OAuth2.
        """
        if self.service_record.auth_type and self.service_record.auth_type != 'oauth2':
            return decrypt_value(self.service_record.app_secret)

        if self.service_record.refresh_token:
            try:
                payload = {
                    "grant_type": "refresh_token",
                    "client_id": self.service_record.app_id,
                    "client_secret": decrypt_value(self.service_record.app_secret),
                    "refresh_token": decrypt_value(self.service_record.refresh_token)
                }
                response = requests.post(self.service_record.auth_url, data=payload, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if "access_token" in data:
                        access_token = data["access_token"]
                        expires_in = data.get("expires_in", 7200)
                        refresh_token = data.get("refresh_token") or self.service_record.refresh_token
                        try:
                            expires_at = datetime.now() + timedelta(seconds=int(expires_in))
                        except ValueError:
                            expires_at = datetime.now() + timedelta(seconds=7200)
                        
                        self._save_token(access_token, refresh_token, expires_at, data)
                        return access_token
            except Exception as e:
                logger.warning(f"[{self.service_name}] Failed to refresh using refresh_token, falling back to full login: {e}")

        # Fallback to fresh login
        return self._login_and_save()

    def _save_token(self, access_token, refresh_token, expires_at, extra_data):
        try:
            # If this is a test (no ID), just update the object in memory
            if not self.service_record.id:
                self.service_record.access_token = encrypt_value(access_token) if access_token else None
                self.service_record.refresh_token = encrypt_value(refresh_token) if refresh_token else None
                self.service_record.expires_at = expires_at
                self.service_record.token_type = extra_data.get("token_type", "Bearer")
                self.service_record.extra_info = encrypt_value(json.dumps(extra_data))
                self.service_record.updated_at = datetime.now()
                return

            # Refresh object from DB to ensure attached
            self.service_record = self.db.query(ExternalService).filter_by(id=self.service_record.id).first()

            
            self.service_record.access_token = encrypt_value(access_token) if access_token else None
            self.service_record.refresh_token = encrypt_value(refresh_token) if refresh_token else None
            self.service_record.expires_at = expires_at
            self.service_record.token_type = extra_data.get("token_type", "Bearer")
            self.service_record.scope = extra_data.get("scope", "")
            self.service_record.extra_info = encrypt_value(json.dumps(extra_data))
            self.service_record.updated_at = datetime.now()
            
            # Also update status to active if it wasn't
            self.service_record.is_active = True
            
            self.db.commit()
            logger.info(f"[{self.service_name}] Token saved/updated successfully.")
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Database error saving token: {e}")
            raise

    def invalidate_token(self):
        """Force clear the token in DB to trigger re-login on next request"""
        if not self.service_record or not self.service_record.id:
            return

        try:
            # Refresh object from DB
            self.service_record = self.db.query(ExternalService).filter_by(id=self.service_record.id).first()
            if self.service_record:
                self.service_record.access_token = None
                self.service_record.expires_at = datetime.now() - timedelta(minutes=1) # Set to past
                self.db.commit()
                logger.info(f"[{self.service_name}] Token invalidated.")
        except SQLAlchemyError as e:
            self.db.rollback()
            logger.error(f"Database error invalidating token: {e}")

    def get_auth_headers(self) -> Dict[str, str]:
        """
        Helper to get ready-to-use headers for API calls.
        Supports: oauth2, basic, api_key, bearer
        """
        if not self.service_record:
            self._load_service_record()
            
        auth_type = self.service_record.auth_type or 'oauth2'

        
        if auth_type == 'oauth2':
            token = self.get_token()
            return {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
        elif auth_type == 'basic':
            import base64
            # For Basic Auth: app_id is username, app_secret is password
            secret = decrypt_value(self.service_record.app_secret)
            credentials = f"{self.service_record.app_id}:{secret}"
            encoded = base64.b64encode(credentials.encode()).decode()
            return {
                "Authorization": f"Basic {encoded}",
                "Content-Type": "application/json"
            }
            
        elif auth_type == 'api_key':
            header_name = self.service_record.app_id or 'X-API-Key'
            return {
                header_name: decrypt_value(self.service_record.app_secret),
                "Content-Type": "application/json"
            }
            
        elif auth_type == 'bearer':
            return {
                "Authorization": f"Bearer {decrypt_value(self.service_record.app_secret)}",
                "Content-Type": "application/json"
            }
            
        return {}

