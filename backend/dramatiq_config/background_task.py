import dramatiq
import os
import asyncio
import uuid
import time
import redis
import shutil
# import threading


from typing import List
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import Retries
# from prometheus_client import start_http_server

from backend.config import settings
from backend.logging_config.logger import logger
from backend.monitor.metrices import (
    PROCESS_DURATION, 
    EMAIL_SENT,
    push_to_gateway,
    registry
)

# Redis Broker Setup
broker = RedisBroker(url="redis://redis:6379/0")
broker.add_middleware(Retries())
dramatiq.set_broker(broker)

# Start Prometheus metrics server (e.g., on port 8002)
# def run_prometheus_server():
#     start_http_server(8002)

# threading.Thread(target=run_prometheus_server, daemon=True).start()

# Redis Connection
r = redis.Redis.from_url("redis://redis:6379/0")

# Semaphore Config
SEMAPHORE_KEY = "semaphore:file_chunks"
MAX_CONCURRENCY = settings.MAX_CONCURRENCY or 8

# Lua Script for Atomic Acquire
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


def get_uuid(batch_id: str):
    if isinstance(batch_id, str):
        try:
            return uuid.UUID(batch_id)
        except ValueError:
            print(f"Invalid UUID string: {batch_id}")
    return None


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


@dramatiq.actor(actor_name="process_zip_file_task", max_retries=3, max_backoff=5000)
def process_zip_task(
    batch_directory: str,
    batch_id: str,
    job_id: str,
    user_id: str,
    company_id: str,
    send_invitations: bool = False,
    # request: Request = None,
    origin: str = None,
):
    from backend.upload.utils import (
        process_zip_extracted_files,
        # send_processing_completion_email,
    )

    batch_uuid = get_uuid(batch_id)

    logger.info(f"[Batch {batch_id}] Starting ZIP processing task...")
    logger.debug(
        f"[Batch {batch_id}] Params - job_id: {job_id}, user_id: {user_id}, send_invitations: {send_invitations}"
    )

    process_start_time = time.time()
    try:
        # async_to_sync(process_zip_extracted_files)(
        #     extracted_dir=batch_directory,
        #     batch_id=batch_uuid,
        #     job_id=job_id,
        #     user_id=user_id,
        #     company_id=company_id,
        #     send_invitations=send_invitations,
        # )

        asyncio.run(
            process_zip_extracted_files(
                extracted_dir=batch_directory,
                batch_id=batch_uuid,
                job_id=job_id,
                user_id=user_id,
                company_id=company_id,
                send_invitations=send_invitations,
            )
        )
        logger.info(f"[Batch {batch_id}] ZIP processing completed successfully.")

        # async_to_sync(send_processing_completion_email)(
        #     batch_uuid, job_id, user_id, origin
        # )

        # asyncio.run(send_processing_completion_email(
        #     batch_uuid, job_id, user_id, origin
        # ))

        # send_email_task.send(batch_id, job_id, user_id, origin)
        # EMAIL_SENT.inc()
        # logger.info(f"Completed processing batch {batch_id}.")

    except Exception as e:
        logger.exception(f"Error in 'process_zip_file_task': {e}")
        raise RuntimeError("process_zip_file_task execution failed.") from e

    finally:
        process_duration = time.time() - process_start_time
        PROCESS_DURATION.observe(process_duration)
        # print(f"[Batch {batch_id}] PROCESS_DURATION observed: {process_duration:.2f}s")
        logger.info(
            f"[Batch {batch_id}] PROCESS_DURATION observed: {process_duration:.2f}s"
        )
        try:
            push_to_gateway("http://pushgateway:9091", job="fastapi_upload_route", registry=registry)
        except Exception as e:
            logger.warning(f"Could not push metrics to Prometheus PushGateway: {e}")


@dramatiq.actor(actor_name="process_file_chunk_task", max_retries=3, max_backoff=5000)
def process_file_chunk_task(
    chunk: List[str],
    extracted_dir: str,
    batch_id: str,
    job_id: str,
    job_data: dict,
    user_id: str,
    company_id: str,
):
    batch_uuid = get_uuid(batch_id)
    from backend.upload.utils import _process_file_chunk
    from backend.monitor.metrices import (
        CHUNK_PROCESS_DURATION,
        CHUNKS_PROCESSED,
        CHUNKS_FAILED,
    )

    logger.info(
        f"[Batch {batch_id}] Starting chunk processing with {len(chunk)} files."
    )  # -> log
    
    chunk_start_time = time.time()
    chunk_counter_key = f"chunk_counter:{batch_id}"

    if not acquire_semaphore():
        logger.warning("Semaphore limit reached, retrying chunk...")
        raise Exception("Semaphore limit reached, try to retry.")

    try:
        # async_to_sync(_process_file_chunk)(
        #     chunk,
        #     extracted_dir,
        #     batch_uuid,
        #     job_id,
        #     job_data,
        #     user_id,
        #     company_id,
        # )

        asyncio.run(
            _process_file_chunk(
                chunk,
                extracted_dir,
                batch_uuid,
                job_id,
                job_data,
                user_id,
                company_id,
            )
        )

        CHUNKS_PROCESSED.inc()
        print(f"----------metrics of CHUNKS_PROCESSED: {CHUNKS_PROCESSED}")
        logger.info(
            f"[Batch {batch_id}] Successfully processed chunk of {len(chunk)} files."
        )
    except Exception as e:
        CHUNKS_FAILED.inc()
        print(f"----------metrics of CHUNKS_FAILED: {CHUNKS_FAILED}")
        logger.error(f"Error processing chunk: {e}")
        raise Exception(f"processing chunk execution failed.")
    finally:

        chunk_duration = time.time() - chunk_start_time
        CHUNK_PROCESS_DURATION.observe(chunk_duration)
        print(f"----------metrics of CHUNK_PROCESS_DURATION: {CHUNK_PROCESS_DURATION}")
            
        release_semaphore()

        # Decrement the Redis counter and delete directory if last
        remaining = r.decr(chunk_counter_key)
        if remaining == 0:
            try:
                shutil.rmtree(os.path.dirname(extracted_dir))
                logger.info(
                    f"Cleaned up directory after last chunk: {extracted_dir}"
                )
            except Exception as e:
                logger.error(f"Failed to clean directory {extracted_dir}: {e}")


@dramatiq.actor(actor_name="process_single_file_task", max_retries=3, max_backoff=5000)
def process_single_file_task(file_path: str, job_id: str, user_id: str, task_id: str):
    from backend.upload.utils import _process_single_file
    from backend.monitor.metrices import (
        FILE_PROCESS_DURATION,
        FILES_PROCESSED,
        FILES_FAILED,
    )

    result_key = f"task_result:{task_id}"
    start = time.time()

    logger.info(f"[Task {task_id}] Processing single file: {file_path}")

    try:
        result = asyncio.run(_process_single_file(file_path, job_id, user_id))
        # result = async_to_sync(_process_single_file)(file_path, job_id, user_id)
        r.set(result_key, str(result))
        FILES_PROCESSED.inc()
        print(f"----------metrics of FILES_PROCESSED: {FILES_PROCESSED}")
        logger.info(
            f"[Task {task_id}] File processed successfully in {time.perf_counter() - start:.2f}s"
        )
    except Exception as e:
        r.set(result_key, "failed")
        FILES_FAILED.inc()
        print(f"----------metrics of FILES_FAILED: {FILES_FAILED}")
        logger.exception(f"Error processing file {file_path}: {e}")
        raise Exception("process_single_file_task execution failed.")
    finally:
        file_duration = time.time() - start
        FILE_PROCESS_DURATION.observe(file_duration)
        print(f"----------metrics of FILE_PROCESS_DURATION: {FILE_PROCESS_DURATION}")


# @dramatiq.actor(actor_name="send_email_task", max_retries=3, max_backoff=5000)
# def send_email_task(batch_id: str, job_id: str, user_id: str, origin: str = None):
#     from backend.upload.utils import send_processing_completion_email

#     batch_uuid = get_uuid(batch_id)

#     try:
#         asyncio.run(send_processing_completion_email(batch_uuid, job_id, user_id, origin))
#     except Exception as e:
#         logger.exception(f"Error sending email in send_email_task: {e}")
#         raise
