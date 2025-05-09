import time
from threading import Lock
from typing import Optional

from app.core.config import settings
from app.logger import logger
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from pymongo import ASCENDING
from pymongo.errors import OperationFailure

_lock = Lock()
_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None


def get_client() -> AsyncIOMotorClient:
    """
    Get or initialize the MongoDB client
    """
    global _client, _db

    if _client is None:
        with _lock:
            if _client is None:  # Double-check pattern
                try:
                    _client = AsyncIOMotorClient(settings.MONGO_DSN)
                    _db = _client[settings.MONGO_DB]
                    logger.info("Connected to MongoDB.")
                except Exception as e:
                    logger.error(f"Could not connect to MongoDB: {e}")
                    raise
    return _client


def collection(name: str) -> AsyncIOMotorCollection:
    """
    Get a collection by name
    """
    if _db is None:
        get_client()
    return _db[name]


async def create_ttl_index():
    """Create TTL index for jobs, questions and auth_tokens collections"""
    retries = 3
    jobs = collection("jobs")
    questions = collection("questions")
    auth_tokens = collection("auth_tokens")

    while retries:
        try:
            # Get the index information for collections
            jobs_indexes = await jobs.index_information()
            questions_indexes = await questions.index_information()
            auth_tokens_indexes = await auth_tokens.index_information()

            index_key = f"expireAt_{ASCENDING}"
            ttl_seconds = 15 * 60  # 15 minutes = 900 seconds

            # Handle jobs collection
            if index_key in jobs_indexes:
                if jobs_indexes.get(index_key, {}).get("expireAfterSeconds") == ttl_seconds:
                    logger.info("TTL index already exists for jobs collection.")
                else:
                    logger.info("Dropping existing TTL index for jobs collection.")
                    await jobs.drop_index(index_key)
                    await jobs.create_index(
                        [("expireAt", ASCENDING)],
                        name="expireAt_1",
                        expireAfterSeconds=ttl_seconds,
                    )
            else:
                await jobs.create_index(
                    [("expireAt", ASCENDING)],
                    name="expireAt_1",
                    expireAfterSeconds=ttl_seconds,
                )

            # Handle questions collection
            if index_key in questions_indexes:
                if questions_indexes.get(index_key, {}).get("expireAfterSeconds") == ttl_seconds:
                    logger.info("TTL index already exists for questions collection.")
                else:
                    logger.info("Dropping existing TTL index for questions collection.")
                    await questions.drop_index(index_key)
                    await questions.create_index(
                        [("expireAt", ASCENDING)],
                        name="expireAt_1",
                        expireAfterSeconds=ttl_seconds,
                    )
            else:
                await questions.create_index(
                    [("expireAt", ASCENDING)],
                    name="expireAt_1",
                    expireAfterSeconds=ttl_seconds,
                )

            # Handle auth_tokens collection
            if index_key in auth_tokens_indexes:
                if auth_tokens_indexes.get(index_key, {}).get("expireAfterSeconds") == ttl_seconds:
                    logger.info("TTL index already exists for auth_tokens collection.")
                else:
                    logger.info("Dropping existing TTL index for auth_tokens collection.")
                    await auth_tokens.drop_index(index_key)
                    await auth_tokens.create_index(
                        [("expireAt", ASCENDING)],
                        name="expireAt_1",
                        expireAfterSeconds=ttl_seconds,
                    )
            else:
                await auth_tokens.create_index(
                    [("expireAt", ASCENDING)],
                    name="expireAt_1",
                    expireAfterSeconds=ttl_seconds,
                )

            return

        except OperationFailure as e:
            retries -= 1
            logger.error(f"Error while creating TTL indexes!! Retrying {3-retries+1}")
            time.sleep(2)


_client = get_client()
