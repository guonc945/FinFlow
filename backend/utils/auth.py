import base64
import hmac
import hashlib
import json
import time

SECRET_KEY = "finflow-secret-super-key"

def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('utf-8')

def _base64url_decode(b64_string: str) -> bytes:
    padding = '=' * (4 - (len(b64_string) % 4))
    return base64.urlsafe_b64decode(b64_string + padding)

def create_access_token(data: dict, expires_delta_seconds: int = 86400) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = data.copy()
    payload["exp"] = int(time.time()) + expires_delta_seconds

    header_b64 = _base64url_encode(json.dumps(header).encode('utf-8'))
    payload_b64 = _base64url_encode(json.dumps(payload).encode('utf-8'))

    msg = f"{header_b64}.{payload_b64}"
    signature = hmac.new(SECRET_KEY.encode('utf-8'), msg.encode('utf-8'), hashlib.sha256).digest()
    signature_b64 = _base64url_encode(signature)

    return f"{msg}.{signature_b64}"

def verify_access_token(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header_b64, payload_b64, signature_b64 = parts
        
        msg = f"{header_b64}.{payload_b64}"
        expected_signature = hmac.new(SECRET_KEY.encode('utf-8'), msg.encode('utf-8'), hashlib.sha256).digest()
        
        if _base64url_encode(expected_signature) != signature_b64:
            return None
            
        payload = json.loads(_base64url_decode(payload_b64).decode('utf-8'))
        
        if payload.get("exp", 0) < time.time():
            return None
            
        return payload
    except Exception:
        return None
