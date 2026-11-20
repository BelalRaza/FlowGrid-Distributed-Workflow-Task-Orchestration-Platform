"""API key authentication.

Keys look like ``fg_<43 random url-safe chars>`` (256 bits of entropy). Only
their SHA-256 hash is stored. A slow password hash (bcrypt/argon2) is not
needed: those exist to make guessing *low-entropy human passwords* expensive,
and a 256-bit random key cannot be guessed whatever the hash speed.
"""

from __future__ import annotations

import hashlib
import secrets

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models import ApiKey, utcnow

KEY_PREFIX = "fg_"
DISPLAY_PREFIX_LENGTH = 11  # "fg_" + 8 chars: enough to identify, useless to attack


def generate_api_key() -> str:
    return KEY_PREFIX + secrets.token_urlsafe(32)


def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def create_api_key(db: Session, name: str) -> tuple[ApiKey, str]:
    """Create a key. The raw key is returned once and never stored."""
    raw = generate_api_key()
    api_key = ApiKey(name=name, prefix=raw[:DISPLAY_PREFIX_LENGTH], key_hash=hash_api_key(raw))
    db.add(api_key)
    db.commit()
    return api_key, raw


def authenticate(db: Session, raw_key: str) -> ApiKey | None:
    if not raw_key.startswith(KEY_PREFIX):
        return None
    api_key = db.scalar(select(ApiKey).where(ApiKey.key_hash == hash_api_key(raw_key)))
    if api_key is None or api_key.revoked_at is not None:
        return None
    return api_key


def revoke_api_key(db: Session, prefix: str) -> int:
    result = db.execute(
        update(ApiKey).where(ApiKey.prefix == prefix, ApiKey.revoked_at.is_(None)).values(revoked_at=utcnow())
    )
    db.commit()
    return result.rowcount


def list_api_keys(db: Session) -> list[ApiKey]:
    return list(db.scalars(select(ApiKey).order_by(ApiKey.created_at)))
