"""API key format: hop_live_<prefix>_<secret>.

The prefix (8 random lowercase letters and digits) is public and is how a key is looked up.
The secret is 32 random bytes in base64url. Only HMAC-SHA256(pepper, secret) is stored: the
secret is random and long, so a fast keyed hash is enough (bcrypt and Argon2 are for
low-entropy passwords), and without the pepper a leaked table cannot be checked against.
"""

import hashlib
import hmac
import re
import secrets
import string
from dataclasses import dataclass, field

PREFIX_LENGTH = 8
_PREFIX_ALPHABET = string.ascii_lowercase + string.digits
_KEY = re.compile(r"hop_live_([a-z0-9]{8})_([A-Za-z0-9_-]{43})")


@dataclass(frozen=True, slots=True)
class NewKey:
    prefix: str
    key_hash: bytes
    plaintext: str = field(repr=False)  # shown to the caller once, never stored


def hash_secret(pepper: bytes, secret: str) -> bytes:
    return hmac.new(pepper, secret.encode(), hashlib.sha256).digest()


def generate(pepper: bytes) -> NewKey:
    prefix = "".join(secrets.choice(_PREFIX_ALPHABET) for _ in range(PREFIX_LENGTH))
    secret = secrets.token_urlsafe(32)  # 32 bytes -> 43 base64url characters
    return NewKey(prefix, hash_secret(pepper, secret), f"hop_live_{prefix}_{secret}")


def parse(presented: str) -> tuple[str, str] | None:
    """(prefix, secret), or None if the string is not shaped like a Hopper key."""
    match = _KEY.fullmatch(presented)
    return (match[1], match[2]) if match else None


def matches(pepper: bytes, secret: str, key_hash: bytes) -> bool:
    """Constant-time comparison, so response timing reveals nothing about the hash."""
    return hmac.compare_digest(hash_secret(pepper, secret), key_hash)
