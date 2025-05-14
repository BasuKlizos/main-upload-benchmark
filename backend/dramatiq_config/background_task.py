import dramatiq
import asyncio
import uuid
import redis

from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import Retries
# from backend.upload.utils import (
#     # process_zip_extracted_files,
#     # send_processing_completion_email,
#     # _process_file_chunk,
#     _process_single_file
# )
from backend.config import settings
from backend.logging_config.logger import logger

# Connect to Redis broker
broker = RedisBroker(url="redis://localhost:6379/0")
broker.add_middleware(Retries())
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
    actor_name="process_zip_file_task"
)
async def process_zip_task(
    batch_directory, batch_id, job_id, user_id, company_id, send_invitations
):
    try:
        from backend.upload.utils import process_zip_extracted_files, send_processing_completion_email
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
    except Exception as e:
        logger.error(f"Error in 'process_zip_file_task': {e}")
        raise dramatiq.RetryLater(delay=300_000)  # retry after 5 minutes


@dramatiq.actor(
    actor_name="process_file_chunk_task"
)
async def process_file_chunk_task(chunk, extracted_dir, batch_id, job_id, job_data, user_id, company_id):
    from backend.upload.utils import _process_file_chunk
    if not acquire_semaphore():
        raise dramatiq.RetryLater(delay=10_000)  # Retry after 10 sec

    try:
        await _process_file_chunk(chunk, extracted_dir, uuid.UUID(batch_id), job_id, job_data, user_id, company_id)
    finally:
        release_semaphore()


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


@dramatiq.actor(
    actor_name="process_single_file_task"
)
async def process_single_file_task(file_path: str, job_id: str, user_id: str, task_id: str):
    from backend.upload.utils import _process_single_file
    try:
        # Process the file (your existing logic)
        result = await _process_single_file(file_path, job_id, user_id)

        # Store the result in Redis (or another database)
        result_key = f"task_result:{task_id}"
        r.set(result_key, str(result))  # or you can store it as a JSON or dict if needed

        return result
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        
        # Store failure status in Redis
        result_key = f"task_result:{task_id}"
        r.set(result_key, "failed")
        
        raise dramatiq.RetryLater(delay=10_000)  # Retry after 1 minute