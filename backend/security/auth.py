import base64
import json
from datetime import timedelta
from json import dumps
from typing import Dict, Tuple, Union

import jwt
from app.core.config import settings
from app.core.db import collection
from app.core.redis import redis
from app.logger import logger
from app.schemas.rbac import UserRole
from app.utils import get_current_time_utc, validate_object_id
from bson import ObjectId
from Crypto.Cipher import AES
from Crypto.Hash import SHA256
from Crypto.Util.Padding import pad, unpad
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext

# Initialize OAuth2 scheme for JWT authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/login", scheme_name="JWT")

# Initialize password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Get users collection from MongoDB
users = collection("users")
auth_keys = collection("auth_keys")


async def verify_token_payload(payload: Dict) -> Dict[str, Union[str, int]]:
    """
    Verify the token payload and validate user exists with correct role.

    Args:
        payload (Dict): Decoded JWT token payload

    Returns:
        Dict[str, Union[str, int]]: Validated subject claim containing user info

    Raises:
        HTTPException(401): If validation fails for any reason
    """
    # Extract and validate required claims from token payload
    sub: Dict[str, str] = json.loads(payload.get("sub", {}))
    exp: int = payload.get("exp")

    # Extract user_id and role from sub claim
    user_id: str = sub.get("user_id")
    role: str = sub.get("role")
    email: str = sub.get("email")
    company_id: str = sub.get("company_id")
    company_name: str = sub.get("company_name")
    # Validate user_id format
    for claim in [user_id, company_id]:
        validate_object_id(claim, error_msg="Malformed Token Subject Contents")

    # Verify all required claims are present
    if not all([user_id, role, exp, email, company_id, company_name]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload - missing required claims",
        )

    cache_key = f"user_details:{user_id}"

    # Try to get user from cache first
    cached_user = redis.get_json_(cache_key)

    if cached_user:
        user = cached_user
    else:
        # Query database if not in cache
        user = await users.find_one(
            {"_id": ObjectId(user_id), "company_id": ObjectId(company_id)},
        )
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

        # Cache the user details with expiry
        redis.set_json_(
            cache_key,
            {
                "name": f"{user.get('firstname')} {user.get('lastname')}",
                "email": user.get("email"),
                "role": user.get("role"),
                "company_id": str(user.get("company_id")),
                "company_name": sub["company_name"],
                "user_id": user_id,
                "credits_used": user.get("credits_used", 0),
                "credits_limit": user.get("credits_limit", 0),
                "profile_pic_url": user.get("profile_pic_url", ""),
            },
            set_expiry=False,
        )

    # Verify user's role in database matches role claim in token
    if user.get("role") != role:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token role claim does not match user role",
        )

    # Verify user's email in database matches role email claim in token
    if user.get("email") != email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token email claim does not match user email",
        )

    return sub


async def decode_token(token: str) -> Dict:
    """
    Decode and validate JWT token.

    Args:
        token (str): JWT token to decode and validate

    Returns:
        Dict: Decoded and validated token payload

    Raises:
        HTTPException(401): If token is expired or invalid
    """
    try:
        # Decode and validate JWT token with configured secret and algorithm
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.AUTHENTICATION_ALGORITHM],
        )
        return payload

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials - invalid token",
        )


async def decode_and_verify_token(token: str = Depends(oauth2_scheme)) -> dict[str, str]:
    # Decode and validate the token
    payload = await decode_token(token)

    # Verify the payload and get validated subject
    sub = await verify_token_payload(payload)

    return sub


async def get_current_user_role(
    token: str = Depends(oauth2_scheme),
) -> Tuple[Dict[str, Union[str, int]], UserRole]:
    """
    Validate JWT token and retrieve current user's role and ID.

    This function performs several validation steps:
    1. Decodes and validates the JWT token
    2. Extracts user information from token payload
    3. Validates the token subject (user ID)
    4. Verifies user exists in database
    5. Validates user role matches token claim

    Args:
        token (str): JWT token from Authorization header, obtained via OAuth2 dependency

    Returns:
        Tuple[Dict[str, Union[str, int]], UserRole]: A tuple containing the user info dict and the authenticated user's role

    Raises:
        HTTPException(401): In the following cases:
            - Token is expired
            - Token is invalid or malformed
            - Token subject (user ID) is invalid
            - Token payload is missing required claims
            - User not found in database
            - User's role doesn't match token claim
    """
    # Decode and verify the token payload
    sub = await decode_and_verify_token(token)

    # Return validated user role as enum along with user info dict
    return sub, UserRole(sub["role"])


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against a hashed password"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generate password hash from plain password"""
    return pwd_context.hash(password)


async def create_access_token(subject: dict) -> str:
    generated_at = get_current_time_utc()
    expires_delta = generated_at + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {"exp": int(expires_delta.timestamp()), "sub": dumps(subject)}

    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET_KEY, settings.AUTHENTICATION_ALGORITHM)

    logger.info(f"User {subject.get('email')} logged in {generated_at.strftime('%m-%d-%Y %H:%M:%S')}")

    return encoded_jwt


def deterministic_encrypt(raw_key: str) -> str:
    """
    Deterministically encrypt a string using AES-CBC mode with a derived IV.

    The initialization vector (IV) is deterministically derived from the input key
    using SHA-256 to ensure the same input always produces the same output.

    Args:
        raw_key (str): The plaintext string to encrypt

    Returns:
        str: Base64 encoded encrypted string

    Raises:
        ValueError: If raw_key is empty or EXTERNAL_SECRET_KEY is invalid length
        UnicodeEncodeError: If raw_key contains invalid characters
    """
    if not raw_key:
        raise ValueError("raw_key cannot be empty")

    # Validate key length - AES requires 16, 24, or 32 bytes
    key = settings.EXTERNAL_SECRET_KEY.encode()
    if len(key) not in (16, 24, 32):
        raise ValueError("EXTERNAL_SECRET_KEY must be 16, 24, or 32 bytes (AES-128, AES-192, or AES-256)")

    # Generate deterministic IV from SHA-256 hash of the key
    hasher = SHA256.new()
    hasher.update(raw_key.encode())
    iv = hasher.digest()[:16]  # Use first 16 bytes as IV

    # Create AES cipher in CBC mode with derived IV
    cipher = AES.new(key, AES.MODE_CBC, iv)

    # Encrypt and pad the data
    padded_data = pad(raw_key.encode(), AES.block_size)
    encrypted = cipher.encrypt(padded_data)

    # Return base64 encoded result
    return base64.b64encode(encrypted).decode()


def deterministic_decrypt(enc_key: str, raw_key: str) -> str:
    """
    Decrypt a deterministically encrypted string using AES-CBC mode.

    Uses the same IV derivation as the encryption function to ensure
    deterministic decryption of values encrypted with deterministic_encrypt().

    Args:
        enc_key (str): Base64 encoded encrypted string to decrypt
        raw_key (str): Original key used during encryption for IV derivation

    Returns:
        str: Decrypted plaintext string

    Raises:
        ValueError: If inputs are empty or padding is invalid
        binascii.Error: If enc_key is not valid base64
        UnicodeDecodeError: If decrypted data is not valid UTF-8
    """
    if not enc_key or not raw_key:
        raise ValueError("enc_key and raw_key cannot be empty")

    # Generate the same deterministic IV
    iv = SHA256.new(raw_key.encode()).digest()[:16]

    # Create AES cipher in CBC mode with derived IV
    cipher = AES.new(settings.EXTERNAL_SECRET_KEY.encode(), AES.MODE_CBC, iv)

    # Decrypt and unpad
    encrypted_data = base64.b64decode(enc_key)
    decrypted = unpad(cipher.decrypt(encrypted_data), AES.block_size)

    return decrypted.decode()
