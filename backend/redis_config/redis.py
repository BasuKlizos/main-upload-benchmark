from typing import Optional

from backend.common.exceptions import EmptyValueForCachingError, MissingKeyError
from backend.config import settings
from backend.logging_config.logger import logger
from redis.commands.json.path import Path
from redis.exceptions import ConnectionError

from redis import Redis

redis_client = Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    db=settings.REDIS_DB,
)


class RedisHelper:
    """
    Redis Helper class for managing Redis operations including JSON handling.
    Provides methods for setting, getting, and deleting both standard and JSON values.
    """

    def __init__(self) -> None:
        try:
            self.redis = redis_client
            # Test connection during initialization
            self.redis.ping()
        except ConnectionError as e:
            logger.error(f"Could not establish connection to redis host: {str(e)}")
            raise

    def set_(
        self,
        key: str,
        val: str,
        set_expiry: bool = True,
        expiry_time: int = settings.REDIS_CACHE_EXPIRY_TIME,
    ) -> bool:
        """Set a string value in Redis with optional expiration."""
        if not val or not key:
            raise EmptyValueForCachingError("Value and key are required")

        try:
            result = self.redis.set(key, val)
            if set_expiry:
                self.redis.expire(name=key, time=expiry_time)
            return bool(result)
        except Exception as e:
            logger.error(f"Error setting Redis key {key}: {str(e)}")
            raise

    def get_(self, key: str) -> Optional[str]:
        """Get a string value from Redis."""
        if not key:
            raise MissingKeyError("Key is required")

        try:
            value = self.redis.get(key)
            return value.decode("utf-8") if value else None
        except Exception as e:
            logger.error(f"Error getting Redis key {key}: {str(e)}")
            raise

    def set_json_(
        self,
        key: str,
        data: dict,
        set_expiry: bool = True,
        expiry_time: int = settings.REDIS_CACHE_EXPIRY_TIME,
    ) -> bool:
        """Set a JSON value in Redis with optional expiration."""
        if not key or data is None:
            raise EmptyValueForCachingError("Key and data are required")

        try:
            result = self.redis.json().set(name=key, path=Path.root_path(), obj=data)
            if set_expiry:
                self.redis.expire(name=key, time=expiry_time)
            return bool(result)
        except Exception as e:
            logger.error(f"Error setting JSON for key {key}: {str(e)}")
            raise

    def get_json_(self, key: str, path: str = Path.root_path()) -> Optional[dict]:
        """Get a JSON value from Redis with optional path filter."""
        if not key:
            raise MissingKeyError("Key is required")

        try:
            return self.redis.json().get(key, path)
        except Exception as e:
            logger.error(f"Error getting JSON for key {key}: {str(e)}")
            raise

    def delete_(self, *keys: str) -> int:
        """Delete one or more keys from Redis. Returns number of keys deleted."""
        if not keys:
            raise MissingKeyError("At least one key is required")

        try:
            return self.redis.delete(*keys)
        except Exception as e:
            logger.error(f"Error deleting keys {keys}: {str(e)}")
            raise


redis = RedisHelper()
