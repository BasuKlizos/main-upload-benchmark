import dramatiq
import asyncio
import uuid
import redis

from dramatiq.brokers.redis import RedisBroker
from backend.upload.utils import (
    process_zip_extracted_files,
    send_processing_completion_email,
    _process_file_chunk
)
from backend.config import settings
from backend.logging_config.logger import logger

# Connect to Redis broker
broker = RedisBroker("redis://localhost:6379/0")
dramatiq.set_broker(broker)

# for redis 
r = redis.Redis.from_url("redis://localhost:6379/0")
SEMAPHORE_KEY = "semaphore:file_chunks"
MAX_CONCURRENCY = settings.MAX_CONCURRENCY or 8


# Safe atomic acquire using Lua script
SEMAPHORE_LUA = """
local current = redis.call('get', KEYS[1])
if not current then current = 0 end
current = tonumber(current)
if current < tonumber(ARGV[1]) then
  redis.call('incr', KEYS[1])
  return 1
else
  return 0
end
"""


@dramatiq.actor(
    actor_name="process_zip_file_task",
    max_retries=5,
    time_limit=600_000,  # in milliseconds | 10 minutes
    retry_backoff=300_000,  # in milliseconds | 5 minutes
)
def process_zip_task(
    batch_directory, batch_id, job_id, user_id, company_id, send_invitations
):
    async def process_zip_extracted():
        logger.info(f"Started processing batch {batch_id}...")

        await process_zip_extracted_files(
            extracted_dir=batch_directory,
            batch_id=batch_id,
            job_id=job_id,
            user_id=user_id,
            company_id=company_id,
            send_invitations=send_invitations,
        )

        await send_processing_completion_email(batch_id, job_id, user_id)
        logger.info(f"Completed processing batch {batch_id}.")

    try:
        asyncio.run(process_zip_extracted())
    except Exception as e:
        logger.error(f"Error in 'process_zip_file_task': {e}")
        raise dramatiq.RetryLater(delay=300_000)  # retry after 5 minutes


@dramatiq.actor(
    actor_name="process_file_chunk_task",
    max_retries=5,
    time_limit=600_000,  # 10 minutes
    retry_backoff=300_000,
)
def process_file_chunk_task(chunk, extracted_dir, batch_id, job_id, job_data, user_id, company_id):
    async def process():
        if not acquire_semaphore():
            raise dramatiq.RetryLater(delay=10_000)  # Retry after 10 sec

        try:
            await _process_file_chunk(chunk, extracted_dir, uuid.UUID(batch_id), job_id, job_data, user_id, company_id)
        finally:
            release_semaphore()

    try:
        asyncio.run(process())
    except Exception as e:
        logger.error(f"Error processing file chunk: {e}", exc_info=True)
        raise dramatiq.RetryLater(delay=60_000)


def acquire_semaphore():
    result = r.eval(SEMAPHORE_LUA, 1, SEMAPHORE_KEY, MAX_CONCURRENCY)
    return result == 1

def release_semaphore():
    with r.pipeline() as pipe:
        while True:
            try:
                pipe.watch(SEMAPHORE_KEY)
                current = int(pipe.get(SEMAPHORE_KEY) or 0)
                new_value = max(0, current - 1)
                pipe.multi()
                pipe.set(SEMAPHORE_KEY, new_value)
                pipe.execute()
                break
            except redis.WatchError:
                continue