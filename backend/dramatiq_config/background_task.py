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
import json
import contextlib

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
from backend.upload.upload_validation import is_supported_file_type
from backend.monitor.metrices import (
    PROCESS_DURATION,
    EMAIL_SENT,
    FILE_COUNT,
    UNSUPPORTED_FILES,
    ZIP_FILES,
    CREATED_FILES,
    CHUNK_PROCESS_DURATION,
    CHUNKS_FAILED,
    CHUNKS_PROCESSED,
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


def get_uuid(batch_id: str):
    if isinstance(batch_id, str):
        try:
            return uuid.UUID(batch_id)
        except ValueError:
            print(f"Invalid UUID string: {batch_id}")
    return None



# In your resume/recovery logic on startup or resume

async def resume_processing_from_backup(job_id: str, batch_id: str, job_data: dict, user_id: str, company_id: str):
    from backend.dramatiq_config.background_task import process_single_file_task

    queue = f"single_file_backup_queue:{job_id}"
    pending_files = r.lrange(queue, 0, -1)

    if pending_files:
        logger.info(f"[Recovery] Found {len(pending_files)} unprocessed files in {queue}. Resuming...")
        for file_path in pending_files:
            process_single_file_task.send(
                file_path.decode(),
                batch_id,
                job_id,
                job_data,
                user_id,
                company_id
            )
    else:
        logger.info("[Recovery] No pending files in backup queue. Proceed with fresh batch.")


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

    try:
        logger.info(f"----------user_details-----------:{user_details}")
        # logger.debug(f"Received batch_name: {batch_name} for job_id: {job_id}")
        # if await batches.find_one({"batch_name": batch_name}):
        #     logger.warning(f"Batch name already taken: {batch_name}")
        #     return

        job = await get_job_data(job_id)
        logger.debug(f"Fetched job data: {job}")

        batch_directory = os.path.join(get_temp_path(), str(batch_id))
        os.makedirs(batch_directory, exist_ok=True)
        logger.info(f"Batch directory created: {batch_directory}")

        for file_data in files_data:
            filename = file_data["filename"]
            content_type = file_data["content_type"]
            content = base64.b64decode(file_data["content"])

            logger.info(f"Processing file: {filename}")
            FILE_COUNT.inc()

            # if content_type not in [
            #     "application/zip",
            #     "application/x-zip-compressed",
            #     "application/pdf",
            #     "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            # ]:
            if not is_supported_file_type(content_type):
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
    finally:
        try:
            push_to_gateway(
                "http://pushgateway:9091",
                job="background_process_zip_task",
                registry=registry,
            )
        except Exception as e:
            logger.warning(f"Could not push metrics to Prometheus PushGateway: {e}")


@dramatiq.actor(actor_name="process_zip_file_actor", max_retries=0)
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
        process_duration = (time.time() - process_start_time) * 1000
        PROCESS_DURATION.observe(process_duration)
        logger.info(
            f"[Batch {batch_id}] PROCESS_DURATION observed: {process_duration:.2f}s"
        )


@dramatiq.actor(actor_name="process_file_chunk_actor_main_queue", max_retries=0)
async def process_file_chunk_task(chunk_data: dict):
    from backend.upload.utils import _process_file_chunk

    start_time = time.time()
    job_id = chunk_data.get("job_id")
    job_key = f"job:{job_id}"
    chunk_id = chunk_data.get("chunk_id")

    try:
        logger.info(f"Processing chunk: {chunk_id}")
        CHUNKS_PROCESSED.inc()

        job_data = chunk_data.get("job_data", {})

        await _process_file_chunk(
            chunk_data.get("chunk_files"),
            chunk_data.get("extracted_dir"),
            uuid.UUID(chunk_data.get("batch_id")),
            chunk_data.get("job_id"),
            job_data,
            chunk_data.get("user_id"),
            chunk_data.get("company_id"),
        )

        r.incr(f"{job_key}:chunks_processed")

    except Exception as e:
        logger.error(f"Failed chunk {chunk_data.get('chunk_id')}: {e}", exc_info=True)
        CHUNKS_FAILED.inc()
        r.lpush(f"death_chunk_queue:{chunk_data.get('job_id')}", json.dumps(chunk_data))

    finally:
        duration = time.time() - start_time
        CHUNK_PROCESS_DURATION.observe(duration)


# === 3. Retry Failed Chunks Actor ===
@dramatiq.actor(actor_name="retry_failed_chunks_actor")
def retry_failed_chunks_actor(job_id: str):

    death_queue_key = f"death_chunk_queue:{job_id}"
    retry_counter_key = f"chunk_retry_counter:{job_id}"

    failed_chunks = r.lrange(death_queue_key, 0, -1)
    if not failed_chunks:
        logger.info(f"No failed chunks left for job {job_id}. Finalizing job.")
        job_metadata = r.get_json_(f"job:{job_id}")
        if job_metadata:
            finalize_job_task.send_with_options(
                args=(
                    job_metadata["batch_id"],
                    job_id,
                    job_metadata["extracted_dir"],
                    job_metadata["user_id"],
                    job_metadata["company_id"],
                    job_metadata.get("send_invitations", False),
                )
            )
        return

    logger.info(f"Retrying {len(failed_chunks)} failed chunks for job {job_id}")

    retryable_chunks_remaining = []

    for chunk_bytes in failed_chunks:
        chunk_data = json.loads(chunk_bytes.decode("utf-8"))
        chunk_id = chunk_data["chunk_id"]

        retry_count = int(r.hget(retry_counter_key, chunk_id) or 0)

        if retry_count >= 3:
            logger.warning(
                f"Chunk {chunk_id} failed {retry_count} times. Skipping further retries."
            )
            continue

        logger.info(f"Retrying chunk {chunk_id} (Attempt {retry_count + 1})")
        process_file_chunk_task.send_with_options(args=(chunk_data,))
        r.hset(retry_counter_key, chunk_id, retry_count + 1)
        retryable_chunks_remaining.append(chunk_data)

    r.delete(death_queue_key)

    if not retryable_chunks_remaining:
        logger.info(f"No more retryable chunks left for job {job_id}. Finalizing job.")
        job_metadata = r.get_json_(f"job:{job_id}")
        if job_metadata:
            finalize_job_task.send_with_options(
                args=(
                    job_metadata["batch_id"],
                    job_id,
                    job_metadata["extracted_dir"],
                    job_metadata["user_id"],
                    job_metadata["company_id"],
                    job_metadata.get("send_invitations", False),
                )
            )


# final cleanup after process_chunk background actor
@dramatiq.actor(actor_name="finalize_job_actor")
async def finalize_job_task(
    batch_id: str,
    job_id: str,
    extracted_dir: str,
    user_id: str,
    company_id: str,
    send_invitations: bool,
):
    from backend.upload.utils import _process_and_vectorize_candidates_batch

    logger.info(f"Finalizing job: {job_id}")

    try:
        await _process_and_vectorize_candidates_batch(
            uuid.UUID(batch_id), job_id, company_id, user_id, send_invitations
        )

    except Exception as e:
        logger.error(f"Error in finalizing job {job_id}: {e}", exc_info=True)

    finally:
        try:
            shutil.rmtree(extracted_dir)
            logger.info(f"Cleaned up directory: {extracted_dir}")
        except FileNotFoundError:
            logger.warning(f"Tried to delete missing directory: {extracted_dir}")
        except Exception as e:
            logger.error(
                f"Failed to cleanup directory {extracted_dir}: {e}", exc_info=True
            )

        try:
            push_to_gateway(
                "http://pushgateway:9091",
                job="file_chunk_processor_metrics",
                registry=registry,
            )
        except Exception as e:
            logger.warning(f"Could not push metrics to Prometheus PushGateway: {e}")


@dramatiq.actor(actor_name="process_single_file_actor", max_retries=0)
async def process_single_file_task(
    file_path: str,
    batch_id_str: str,
    job_id_str: str,
    job_data: dict,
    user_id: str,
    company_id: str,
):

    from backend.upload.utils import (
        _process_single_file,
        _insert_candidates,
        _insert_candidate_errors,
    )
    from backend.schemas.candidate import CVParseResponse

    batch_id = get_uuid(batch_id_str)
    job_id = ObjectId(job_id_str)
    company_id = ObjectId(company_id)

    logger.info(f"[SingleFileActor] Processing file: {file_path}")

    try:
        result = await _process_single_file(file_path, job_data.get("job_id"), user_id)

        valid_results, invalid_results, error_count = [], [], 0
        current_time = get_current_time_utc()

        if isinstance(result, Exception):
            error_count += 1
            logger.error(f"[SingleFileActor] Task failed: {str(result)}", exc_info=True)
        elif isinstance(result, dict) and result.get("error"):
            invalid_results.append(
                {
                    **result,
                    "batch_id": Binary.from_uuid(batch_id),
                    "job_id": job_id,
                    "company_id": company_id,
                    "created_at": current_time,
                    "updated_at": current_time,
                }
            )
        elif isinstance(result, CVParseResponse):
            valid_results.append(result)

        logger.info(
            f"[SingleFileActor] File processed: {len(valid_results)} success, {error_count} errors"
        )

        await _insert_candidates(
            valid_results,
            batch_id,
            str(job_id),
            str(company_id),
            user_id,
            job_data,
            current_time,
        )
        await _insert_candidate_errors(invalid_results)

    except Exception as e:
        logger.error(
            f"[SingleFileActor] Failed to process file {file_path}: {str(e)}",
            exc_info=True,
        )
        # `single_file_death_queue` here
        r.lpush(f"single_file_death_queue:{job_id}", file_path)

    finally:
        # Decrement files_to_process count
        r.decr(f"files_to_process:{job_id_str}")
        
        # Remove from backup queue
        r.lrem(f"single_file_backup_queue:{job_id}", 0, file_path)


@dramatiq.actor(actor_name="single_file_retry_actor")
def single_file_retry_actor(
    job_id: str, batch_id: str, job_data: dict, user_id: str, company_id: str
):

    MAX_RETRY_ATTEMPTS = 3
    queue_name = f"single_file_death_queue:{job_id}"
    retry_prefix = f"file_retry_count:{job_id}:"

    failed_files = r.lrange(queue_name, 0, -1)

    if not failed_files:
        logger.info(f"[RetryActor] No failed files found in {queue_name}")
        return

    logger.info(f"[RetryActor] Retrying {len(failed_files)} files from {queue_name}")

    for file_path_bytes in failed_files:
        file_path = file_path_bytes.decode()
        retry_key = retry_prefix + file_path

        try:
            attempt_count = int(r.get(retry_key) or 0)

            if attempt_count >= MAX_RETRY_ATTEMPTS:
                logger.warning(
                    f"[RetryActor] Max attempts reached for {file_path}. Skipping."
                )
                continue

            # Re-dispatch file to single_file_processor_actor
            process_single_file_task.send(
                file_path, batch_id, job_id, job_data, user_id, company_id
            )

            # Increment attempt count
            r.set(retry_key, attempt_count + 1)

        except Exception as e:
            logger.error(
                f"[RetryActor] Failed to retry {file_path}: {str(e)}", exc_info=True
            )

    # Clean up the death queue after processing
    r.delete(queue_name)
    logger.info(f"[RetryActor] Completed retry cycle for {queue_name}")
