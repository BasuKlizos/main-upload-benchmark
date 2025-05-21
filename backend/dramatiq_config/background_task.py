import dramatiq
import os
import io
import asyncio
import uuid
import time
import redis
import shutil
import zipfile
import base64

from typing import List
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import Retries, AsyncIO
from bson import Binary, ObjectId

from backend.upload.utils import (
    get_job_data,
)
from backend.db_config.db import collection
from backend.utils import (
    get_current_time_utc,
    get_temp_path,
)
from backend.config import settings
from backend.logging_config.logger import logger
from backend.monitor.metrices import (
    PROCESS_DURATION,
    EMAIL_SENT,
    FILE_COUNT,
    UNSUPPORTED_FILES,
    ZIP_FILES,
    CREATED_FILES,
    push_to_gateway,
    registry,
)


# Redis Broker Setup
broker = RedisBroker(url="redis://redis:6379/0")
broker.add_middleware(AsyncIO())
broker.add_middleware(Retries())
dramatiq.set_broker(broker)

# Redis Connection
r = redis.Redis.from_url("redis://redis:6379/0")

# Collections
jobs = collection("jobs")
batches = collection("batches")

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


@dramatiq.actor(actor_name="zip_extraction_actor")
async def zip_extract_and_prepare_actor(
    job_id: str,
    batch_name: str,
    files_data: list,  # List of dicts: {filename, content_type, content}
    user_details: dict,
    batch_id: str,
    send_invitations: bool,
    origin: str,
):
    logger.info("zip_extract_and_prepare_actor triggered")
    # batch_id = None

    try:
        print(f"----------user_details-----------:{user_details}")
        logger.debug(f"Received batch_name: {batch_name} for job_id: {job_id}")
        if await batches.find_one({"batch_name": batch_name}):
            logger.warning(f"Batch name already taken: {batch_name}")
            return

        job = await get_job_data(job_id)
        logger.debug(f"Fetched job data: {job}")

        # batch_id = create_batch_id()
        batch_directory = os.path.join(get_temp_path(), str(batch_id))
        os.makedirs(batch_directory, exist_ok=True)
        logger.info(f"Batch directory created: {batch_directory}")

        for file_data in files_data:
            filename = file_data["filename"]
            content_type = file_data["content_type"]
            content = base64.b64decode(file_data["content"])

            logger.info(f"Processing file: {filename}")
            FILE_COUNT.inc()

            if content_type not in [
                "application/zip",
                "application/x-zip-compressed",
                "application/pdf",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ]:
                UNSUPPORTED_FILES.inc()
                logger.warning(f"Unsupported file skipped: {filename}")
                continue

            if content_type in ["application/zip", "application/x-zip-compressed"]:
                ZIP_FILES.inc()
                temp_extract_dir = os.path.join(batch_directory, "_temp_extract")
                os.makedirs(temp_extract_dir, exist_ok=True)

                with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                    zip_file.extractall(temp_extract_dir)

                for root, _, files in os.walk(temp_extract_dir):
                    for fname in files:
                        if fname.lower().endswith((".pdf", ".docx")):
                            src = os.path.join(root, fname)
                            base, ext = os.path.splitext(fname)
                            dest = os.path.join(
                                batch_directory,
                                f"{base}_{get_current_time_utc().timestamp()}{ext}",
                            )
                            shutil.move(src, dest)
                            logger.debug(f"Moved {src} → {dest}")
                shutil.rmtree(temp_extract_dir)

            else:
                CREATED_FILES.inc()
                base, ext = os.path.splitext(filename)
                dest = os.path.join(
                    batch_directory, f"{base}_{get_current_time_utc().timestamp()}{ext}"
                )
                with open(dest, "wb") as f:
                    f.write(content)
                logger.debug(f"Saved file: {dest}")

        file_count = len(
            [
                f
                for f in os.listdir(batch_directory)
                if f.lower().endswith((".pdf", ".docx"))
            ]
        )

        batch_uuid = get_uuid(batch_id)

        await batches.insert_one(
            {
                "uploaded_by": ObjectId(user_details.get("company_id")),
                "company_id": ObjectId(user_details.get("company_id")),
                "batch_id": Binary.from_uuid(batch_uuid),
                "batch_name": batch_name,
                "upload_count": file_count,
                "job_id": ObjectId(job_id),
                "status": "processing",
                "start_time": get_current_time_utc(),
            }
        )
        logger.debug("Inserted batch metadata to database")

        await jobs.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {"updated_at": get_current_time_utc()},
                "$inc": {"selection_progress.total_candidate_count": file_count},
            },
        )
        logger.debug("Updated job record with candidate count")

        logger.info(f"Batch {batch_name} processed with {file_count} files")

        logger.debug(f"Sending background task with origin: {origin}")

        process_zip_task.send(
            batch_directory=batch_directory,
            # batch_id=str(batch_id),
            batch_id=batch_id,
            job_id=job_id,
            user_id=user_details.get("user_id"),
            company_id=user_details.get("company_id"),
            send_invitations=send_invitations,
            origin=origin,
        )

    except Exception as e:
        logger.exception(f"Error in zip_extract_and_prepare_actor: {e}")

        # Cleanup on error
        try:
            if batch_directory and os.path.exists(batch_directory):
                shutil.rmtree(batch_directory)
                logger.debug(
                    f"Cleaned up temp directory after error: {batch_directory}"
                )
        except Exception as cleanup_error:
            logger.error(
                f"Failed to cleanup temp directory: {cleanup_error}", exc_info=True
            )


@dramatiq.actor(actor_name="process_zip_file_actor", max_retries=3, max_backoff=5000)
async def process_zip_task(
    batch_directory: str,
    batch_id: str,
    job_id: str,
    user_id: str,
    company_id: str,
    send_invitations: bool = False,
    origin: str = None,
):
    from backend.upload.utils import (
        process_zip_extracted_files,
        send_processing_completion_email,
    )

    batch_uuid = get_uuid(batch_id)

    logger.info(f"[Batch {batch_id}] Starting ZIP processing task...")
    logger.debug(
        f"[Batch {batch_id}] Params - job_id: {job_id}, user_id: {user_id}, send_invitations: {send_invitations}"
    )

    process_start_time = time.time()
    try:
        await process_zip_extracted_files(
            extracted_dir=batch_directory,
            batch_id=batch_uuid,
            job_id=job_id,
            user_id=user_id,
            company_id=company_id,
            send_invitations=send_invitations,
        )
        logger.info(f"[Batch {batch_id}] ZIP processing completed successfully.")

        # await send_processing_completion_email(
        #     batch_uuid, job_id, user_id, origin
        # )
        # EMAIL_SENT.inc()
        # logger.info(f"Completed processing batch {batch_id}.")

    except Exception as e:
        logger.exception(f"Error in 'process_zip_file_task': {e}")
        raise RuntimeError("process_zip_file_task execution failed.") from e

    finally:
        process_duration = time.time() - process_start_time
        PROCESS_DURATION.observe(process_duration)
        logger.info(
            f"[Batch {batch_id}] PROCESS_DURATION observed: {process_duration:.2f}s"
        )
        try:
            push_to_gateway(
                "http://pushgateway:9091",
                job="background_process_zip_task",
                registry=registry,
            )
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
                logger.info(f"Cleaned up directory after last chunk: {extracted_dir}")
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
