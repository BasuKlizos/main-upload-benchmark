from typing import Optional

from backend.common.exceptions import InvalidAPIKeyException
from backend.db_config.db import collection
from backend.logging_config.logger import logger
from backend.security.auth import deterministic_encrypt
from backend.service.external import caller_api_client
from backend.utils import get_current_time_utc
from bson import ObjectId

# Collections
auth_tokens = collection("auth_tokens")
auth_keys = collection("auth_keys")


async def secure_token_ops(**kwargs) -> Optional[str]:
    """
    Handle secure token operations (set/get/delete) in MongoDB.

    Args:
        mode (str): Operation mode - either "set", "get" or "delete"
        **kwargs: Additional arguments
            For set mode:
                key (str): Token key
                value (str): Token value
                set_expiry (bool): Whether to set expiry
                expiry_time (timedelta): Expiry duration
            For get mode:
                filter parameters for find_one query
            For delete mode:
                filter parameters for delete_one query

    Returns:
        Optional[str]: Found token document for get mode, None for set/delete modes

    Raises:
        ValueError: If invalid mode provided
    """
    mode = kwargs.pop("mode")

    if mode == "set":
        # Prepare token document
        payload = {"key": kwargs["key"], "value": kwargs["value"], "created_at": get_current_time_utc()}

        # Add expiry if specified
        if kwargs.get("set_expiry"):
            payload["expireAt"] = get_current_time_utc() + kwargs.get("expiry_time")

        # Insert token document
        await auth_tokens.insert_one(payload)
        return None

    elif mode == "get":
        # Find and return token document
        token = await auth_tokens.find_one(kwargs)
        return token.get("value") if token else None

    elif mode == "delete":
        # Delete token document
        await auth_tokens.delete_one(kwargs)
        return None

    else:
        raise ValueError("Invalid mode. Must be either 'set', 'get' or 'delete'")


async def api_key_ops(**kwargs) -> Optional[dict]:
    """
    Handle API key operations (set/get) in MongoDB.

    Args:
        mode (str): Operation mode - either "set" or "get"
        **kwargs: Additional arguments
            For set mode:
                company_id (str): Company ID to generate key for
                name (str): Company name
            For get mode:
                x_api_key (str): API key to validate

    Returns:
        Optional[dict]: Authentication data for get mode, None for set mode

    Raises:
        InvalidAPIKeyException: If API key is invalid in get mode
        ValueError: If invalid mode provided
    """
    mode = kwargs.pop("mode")

    if mode == "set":
        # Generate new API key from external service
        api_key_data = await caller_api_client.generate_company_api_key(kwargs)
        api_key = api_key_data.get("data", {}).get("key")

        if not api_key:
            raise ValueError(f"Failed to generate API key for company: {kwargs['company_id']}")

        # Encrypt the API key before storing
        encrypted_key = deterministic_encrypt(api_key)

        # Store encrypted key with metadata
        current_time = get_current_time_utc()
        await auth_keys.insert_one(
            {
                "api_key_encrypted": encrypted_key,
                "company_id": ObjectId(kwargs["company_id"]),
                "created_at": current_time,
                "updated_at": current_time,
            }
        )
        logger.info(f"API key generated for company: {kwargs['company_id']}")
        return None

    elif mode == "get":
        # Validate and lookup API key
        if not kwargs.get("x_api_key"):
            raise InvalidAPIKeyException("API key is required")

        encrypted_key = deterministic_encrypt(kwargs["x_api_key"])
        auth_data = await auth_keys.find_one({"api_key_encrypted": encrypted_key}, {"_id": 0, "created_at": 0, "updated_at": 0})

        if not auth_data:
            raise InvalidAPIKeyException("Invalid API key")

        return auth_data

    else:
        raise ValueError(f"Invalid mode '{mode}'. Must be either 'set' or 'get'")