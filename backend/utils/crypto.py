# -*- coding: utf-8 -*-
import os
import base64
from cryptography.fernet import Fernet
import logging
from pathlib import Path

logger = logging.getLogger("crypto")

# Try to load key from file, or generate new one
BASE_DIR = Path(__file__).resolve().parents[1]
KEY_FILE = Path(os.getenv("ENCRYPTION_KEY_FILE", str(BASE_DIR / ".encryption.key")))

def _get_key():
    key = os.getenv("ENCRYPTION_KEY")
    if key:
        return key.encode() if isinstance(key, str) else key
    
    if KEY_FILE.exists():
        with open(KEY_FILE, "rb") as f:
            return f.read().strip()
            
    # Generate new key
    key = Fernet.generate_key()
    KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(KEY_FILE, "wb") as f:
        f.write(key)
    logger.warning(f"Generated new encryption key and saved to {KEY_FILE}. Keep this safe!")
    return key

_fernet = Fernet(_get_key())

def encrypt_value(value: str) -> str:
    """Encrypt a string value."""
    if not value:
        return value
    return _fernet.encrypt(value.encode()).decode()

def decrypt_value(token: str) -> str:
    """Decrypt a string value. Returns original if decryption fails (graceful degradation for legacy plain data)."""
    if not token:
        return token
    try:
        return _fernet.decrypt(token.encode()).decode()
    except Exception:
        # Assuming legacy plain text
        return token
