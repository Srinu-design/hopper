import time
import uuid

import jwt
import pytest

from hopper.auth import keys, passwords, tokens

PEPPER = b"unit-test-pepper"
SECRET = "unit-test-jwt-secret-0123456789abcdef"
ISS, AUD = "hopper", "hopper-admin"


def test_generated_key_has_the_documented_format_and_only_its_hash_is_kept() -> None:
    new = keys.generate(PEPPER)
    assert new.plaintext.startswith(f"hop_live_{new.prefix}_")
    parsed = keys.parse(new.plaintext)
    assert parsed is not None
    prefix, secret = parsed
    assert prefix == new.prefix and len(prefix) == 8
    assert len(secret) == 43  # 32 random bytes in base64url
    assert new.key_hash == keys.hash_secret(PEPPER, secret)
    assert secret not in repr(new)  # never printed by accident


def test_keys_differ_and_need_the_right_pepper() -> None:
    a, b = keys.generate(PEPPER), keys.generate(PEPPER)
    assert a.prefix != b.prefix and a.plaintext != b.plaintext
    _, secret = keys.parse(a.plaintext) or ("", "")
    assert keys.matches(PEPPER, secret, a.key_hash)
    assert not keys.matches(b"another-pepper", secret, a.key_hash)
    tampered = secret[:-1] + ("B" if secret.endswith("A") else "A")  # always a different secret
    assert not keys.matches(PEPPER, tampered, a.key_hash)


@pytest.mark.parametrize(
    "presented",
    [
        "",
        "hop_live_abc",
        "hop_test_abcdefgh_" + "A" * 43,  # wrong environment marker
        "hop_live_ABCDEFGH_" + "A" * 43,  # prefix is lowercase only
        "hop_live_abcdefgh_" + "A" * 42,  # secret too short
        "hop_live_abcdefgh_" + "A" * 43 + " ",
    ],
)
def test_malformed_keys_do_not_parse(presented: str) -> None:
    assert keys.parse(presented) is None


def admin(role: tokens.Role = "owner") -> tokens.Admin:
    return tokens.Admin(uuid.uuid4(), role, uuid.uuid4() if role == "owner" else None)


def decode(token: str) -> tokens.Admin:
    return tokens.decode(token, secret=SECRET, issuer=ISS, audience=AUD)


def test_token_round_trips() -> None:
    for who in (admin("owner"), admin("platform_admin")):
        token = tokens.issue(who, secret=SECRET, issuer=ISS, audience=AUD, ttl_seconds=900)
        assert decode(token) == who


def raw(claims: dict[str, object], key: str = SECRET, alg: str = "HS256") -> str:
    base: dict[str, object] = {
        "sub": str(uuid.uuid4()),
        "role": "platform_admin",
        "tenant_id": None,
        "iat": int(time.time()),
        "exp": int(time.time()) + 900,
        "iss": ISS,
        "aud": AUD,
    }
    return jwt.encode(base | claims, key, algorithm=alg)


@pytest.mark.parametrize(
    "token",
    [
        raw({"exp": int(time.time()) - 1}),  # expired
        raw({"aud": "another-service"}),  # minted for someone else
        raw({"iss": "someone-else"}),
        raw({}, key="a-different-secret-0123456789abcdef"),  # forged
        raw({"role": "superuser"}),
        raw({"role": "owner"}),  # an owner without a tenant
        raw({"tenant_id": str(uuid.uuid4())}),  # a platform admin with one
        raw({"sub": "not-a-uuid"}),
        "not.a.jwt",
    ],
)
def test_bad_tokens_are_rejected(token: str) -> None:
    with pytest.raises(tokens.InvalidToken):
        decode(token)


@pytest.mark.filterwarnings("ignore::jwt.warnings.InsecureKeyLengthWarning")
def test_a_token_signed_with_another_algorithm_is_rejected() -> None:
    """Same secret, HS512: rejected because only HS256 is on the allowed list."""
    with pytest.raises(tokens.InvalidToken):
        decode(raw({}, alg="HS512"))


def test_alg_none_token_is_rejected() -> None:
    """The classic attack: an unsigned token that claims to need no signature."""
    unsigned = jwt.encode(
        {
            "sub": str(uuid.uuid4()),
            "role": "platform_admin",
            "tenant_id": None,
            "iat": int(time.time()),
            "exp": int(time.time()) + 900,
            "iss": ISS,
            "aud": AUD,
        },
        key=None,
        algorithm="none",
    )
    with pytest.raises(tokens.InvalidToken):
        decode(unsigned)


def test_token_missing_a_required_claim_is_rejected() -> None:
    claims = {"sub": str(uuid.uuid4()), "role": "platform_admin", "iss": ISS, "aud": AUD}
    token = jwt.encode(claims, SECRET, algorithm="HS256")  # no exp, no iat
    with pytest.raises(tokens.InvalidToken):
        decode(token)


async def test_passwords_hash_with_argon2id_and_verify() -> None:
    hashed = await passwords.hash_password("correct horse battery")
    assert hashed.startswith("$argon2id$")
    assert await passwords.verify_password(hashed, "correct horse battery")
    assert not await passwords.verify_password(hashed, "wrong horse battery")
    assert not await passwords.verify_password(None, "anything")  # unknown user
    assert not await passwords.verify_password("not-a-hash", "anything")
