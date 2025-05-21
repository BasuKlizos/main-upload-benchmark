import asyncio
import time
import os.path
import random
import contextlib
import shutil
import string
import uuid
from datetime import timedelta
from typing import Any, List, Optional
from urllib.parse import quote

from backend.upload.parse import extract
from backend.common.circuit_breaker import CircuitBreaker
from backend.common.exceptions import TextExtractionFailedException
from backend.aws_config.aws import email_operation, s3_operation
from backend.config import settings
from backend.db_config.db import collection
from backend.redis_config.redis import redis
from backend.logging_config.logger import logger
from backend.schemas.candidate import CVParseResponse
from backend.service.ai import ai_services
from backend.service.external import caller_api_client
from backend.service.vectorize import vector_service
from backend.utils import EmbeddingUtils, get_current_time_utc, validate_object_id
from bson import Binary, ObjectId
from fastapi import HTTPException, Request, status
from pymongo import ReplaceOne
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from backend.monitor.metrices import (
    CREATED_FILES, 
    CHUNK_PROCESS_DURATION, 
    CHUNKS_FAILED,
    CHUNKS_PROCESSED,
    FILES_PROCESSED,
    FILE_PROCESS_DURATION,
    FILES_FAILED,
    push_to_gateway,
    registry
)

# Collections
candidates = collection("candidates")
candidates_errors = collection("candidates_errors")
batches = collection("batches")
jobs = collection("jobs")
candidate_questions_col = collection("candidate_questions")

# Circuit Breaker instances
question_cb = CircuitBreaker(max_failures=3, reset_timeout=90)
embed_cb = CircuitBreaker(max_failures=3, reset_timeout=90)

# Retry configuration
RETRY_CONFIG = dict(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(Exception))


async def get_job_data(job_id: str) -> dict:
    """
    Get job data from Redis cache or database.

    Args:
        job_id: ID of the job to fetch

    Returns:
        dict: Job data

    Raises:
        HTTPException: If job is not found
    """
    redis_key = f"job:{job_id}"
    if redis.get_json_(redis_key):
        job = redis.get_json_(redis_key)
    else:
        validate_object_id(job_id)

        # Get job and candidate data from database
        job = await jobs.find_one({"_id": ObjectId(job_id)}, {"_id": 0, "created_at": 0, "updated_at": 0, "company_id": 0})
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID {job_id} not found",
            )

        # Load the job in redis if it does't exists
        redis.set_json_(redis_key, job, set_expiry=False)

    return job


@retry(**RETRY_CONFIG)
def upload_file_to_s3(file_path: str, job_name: str, unq_id: Optional[str] = None) -> str:
    fname, ext = os.path.splitext(os.path.basename(file_path))
    path = f"CVs/{unq_id}"
    object_name = f"{path}/{job_name}_{fname}_{get_current_time_utc().strftime('%d_%m_%Y')}{ext}".replace(" ", "_")
    s3_operation.upload_file(file_path, object_name)
    return path


def _generate_unique_candidate_id(length: int = 8) -> str:
    unique_part = uuid.uuid4().hex[:length]
    random_part = "".join(random.choices(string.ascii_letters + string.digits, k=length))
    return f"{unique_part}{random_part}"[:length]


async def _process_single_file(file_path: str, job_name: str, user_id: str) -> CVParseResponse | dict:
    logger.info(f"Starting to process file: {file_path}")
    unq_id = _generate_unique_candidate_id()

    start_time = time.time()

    try:
        text, is_image_pdf, metadata = await extract.extract_text(file_path, user_id)
        if file_path.lower().endswith((".docx", ".doc")) and not text:
            raise TextExtractionFailedException(f"Text extraction failed for {file_path}")

        # Parse word_count, frequent_words, reading_level, vocabulary_level, section_presence
        # TODO -> Find logic for image pdfs
        if isinstance(text, str):
            heuristics = await ai_services.cv_parser.extract_heuristics(text)
            metadata.update(heuristics)
        else:
            metadata.update(
                {
                    "word_count": 0,
                    "personal_pronoun_count": 0,
                    "reading_level": 0,
                    "vocabulary_level": 0,
                    "frequent_words": [],
                }
            )

        parsed_cv = await ai_services.cv_parser.parse_text(text, user_id) if not is_image_pdf else text

        if parsed_cv.email:
            existing_unique_id = await candidates.find_one({"email": parsed_cv.email}, {"_id": 0, "unique_id": 1})
            if existing_unique_id and existing_unique_id.get("unique_id"):
                unq_id = existing_unique_id.get("unique_id")

        # Add metadata, unique_id, and cv_directory_link to parsed_cv
        parsed_cv.metadata = metadata
        parsed_cv.unique_id = unq_id
        parsed_cv.cv_directory_link = upload_file_to_s3(file_path, job_name, unq_id)

        # Cache parsed text for each candidate
        # TODO -> Find logic for image pdfs
        if not isinstance(text, str):
            text = ""
        cache_key = f"candidate_text:{unq_id}"
        redis.set_json_(cache_key, text, set_expiry=True, expiry_time=timedelta(days=1))

        FILES_PROCESSED.inc()
        duration = time.time() - start_time
        FILE_PROCESS_DURATION.observe(duration)

        logger.info(f"Successfully parsed file: {file_path}")
        return parsed_cv

    except Exception as e:
        FILES_FAILED.inc()
        duration = time.time() - start_time
        FILE_PROCESS_DURATION.observe(duration)

        logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
        return {"error": str(e), "cv_directory_link": upload_file_to_s3(file_path, job_name, unq_id)}


def _get_pdf_text(unq_id: str) -> str:
    return redis.get_json_(f"candidate_text:{unq_id}")


async def _insert_candidates(valid_results, batch_id, job_id, company_id, user_id, job_data, current_time):
    if not valid_results:
        return

    candidates_data = [
        {
            **result.model_dump(),
            "batch_id": Binary.from_uuid(batch_id),
            "job_id": ObjectId(job_id),
            "company_id": ObjectId(company_id),
            "status": "New",
            "created_at": current_time,
            "updated_at": current_time,
            "compatibility_analysis": await ai_services.compatibility_analyzer.get_analysis(
                job=job_data, candidate=result.model_dump(), user_id=user_id
            ),
            "ats_analysis": await ai_services.ats_analyzer.analyze(
                pdf_text=_get_pdf_text(result.unique_id), candidate_data=result.model_dump(), user_id=user_id, role=job_data.get("title")
            ),
        }
        for result in valid_results
    ]

    processed_candidates = set()
    operations = []

    for candidate in candidates_data:
        email = candidate.get("email")
        name = candidate.get("name")
        candidate_key = f"{name}:{email}" if name and email else None

        if candidate_key and candidate_key not in processed_candidates:
            operations.append(
                ReplaceOne(
                    {"email": email, "name": name, "job_id": candidate.get("job_id"), "company_id": candidate.get("company_id")},
                    candidate,
                    upsert=True,
                )
            )
            processed_candidates.add(candidate_key)

    if operations:
        await candidates.bulk_write(operations, ordered=False)
        logger.info(f"Inserted {len(operations)} candidates into DB")


async def _insert_candidate_errors(invalid_results):
    if invalid_results:
        await candidates_errors.insert_many(invalid_results, ordered=False)
        logger.info(f"Inserted {len(invalid_results)} error records into DB")


async def _process_file_chunk(chunk: List[str], extracted_dir: str, batch_id: uuid.UUID, job_id: str, job_data: dict, user_id: str, company_id: str):
    logger.info(f"Processing chunk of {len(chunk)} files from {extracted_dir}")

    tasks = [_process_single_file(os.path.join(extracted_dir, file), job_data.get("job_id"), user_id) for file in chunk]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid_results, invalid_results, error_count = [], [], 0
    current_time = get_current_time_utc()

    for result in results:
        if isinstance(result, Exception):
            error_count += 1
            logger.error(f"Task failed: {str(result)}", exc_info=True)
        elif isinstance(result, dict) and result.get("error"):
            invalid_results.append(
                {
                    **result,
                    "batch_id": Binary.from_uuid(batch_id),
                    "job_id": ObjectId(job_id),
                    "company_id": ObjectId(company_id),
                    "created_at": current_time,
                    "updated_at": current_time,
                }
            )
        elif isinstance(result, CVParseResponse):
            valid_results.append(result)

    logger.info(f"Chunk processed: {len(valid_results)} successes, {error_count} errors")

    await _insert_candidates(valid_results, batch_id, job_id, company_id, user_id, job_data, current_time)
    await _insert_candidate_errors(invalid_results)


async def _send_interview_invitations(candidates: list[dict], company_name: str, job_id: str, job_title: str, company_id: str) -> None:
    """
    Send interview invitations to qualified candidates
    """
    ## Get Interview Links

    # Set Expiration Time for Interview Links 3 days from now
    expiration_delta = get_current_time_utc() + timedelta(days=3)

    # Create Payload for Interview Links
    payload = [
        {
            "name": candidate.get("name"),
            "email": candidate.get("email"),
            "candidate_id": candidate.get("candidate_id"),
            "job_id": job_id,
            "company_id": company_id,
            "expiration_time": expiration_delta.strftime("%Y-%m-%dT%H:%M:%S.000+00:00"),
            "unique_id": candidate.get("unique_id"),
        }
        for candidate in candidates
    ]

    # Create Interview Links
    interview_links = await caller_api_client.create_interview_link(payload)
    interview_links = interview_links.get("data")

    # Create recipients data for bulk email
    recipients_data = [
        {
            "email": candidate.get("email"),
            "template_data": {
                "job_title": job_title,
                "candidate_name": candidate.get("name"),
                "company_name": company_name,
                "expiration_time": expiration_delta.strftime("%d/%m/%Y %I:%M:%S %p UTC"),
                "interview_link": interview_links.get(candidate.get("candidate_id")),
            },
        }
        for candidate in candidates
    ]

    email_operation.send_bulk_email_with_ses_template(
        template_name="InterviewInviteTemplate", recipients_data=recipients_data, default_data={"company_data": company_name, "job_title": job_title}
    )

    logger.info(f"Successfully sent interview invitations to {len(candidates)} candidates")


async def _fetch_job_data(job_id: str) -> dict:
    job_cache_key = f"job:{job_id}"
    job_data = redis.get_json_(job_cache_key)

    if not job_data:
        job_data = await jobs.find_one({"_id": ObjectId(job_id)}, {"_id": 0})
        if job_data:
            redis.set_json_(job_cache_key, job_data)

    if not job_data:
        raise ValueError(f"Job data not found for job_id {job_id}")

    return job_data


async def _fetch_batch_candidates(batch_id: uuid.UUID, company_id: str) -> list:
    return await candidates.find(
        {"batch_id": Binary.from_uuid(batch_id), "company_id": ObjectId(company_id)},
        {
            "_id": 1,
            "name": 1,
            "email": 1,
            "location": 1,
            "unique_id": 1,
            "languages": 1,
            "designation": 1,
            "total_experience": 1,
            "compatibility_analysis": 1,
            "technical_skills": 1,
            "soft_skills": 1,
            "work_experience": 1,
            "education": 1,
            "certifications": 1,
            "created_at": 1,
            "primary_phone": 1,
            "status": 1,
        },
    ).to_list(length=None)


async def _process_candidates(batch_candidates: list, job_data: dict, user_id: str, company_id: str) -> tuple[Any]:
    embedding_tasks = []
    qualified_candidates = []
    question_operations = []
    disqualified_candidates = []

    min_score_threshold = job_data.get("selection_criteria", {}).get("resume_shortlist_criteria", 0)
    job_title = job_data.get("title")

    for candidate in batch_candidates:
        candidate_id = str(candidate["_id"])
        candidate["_id"] = candidate_id
        candidate["company_id"] = company_id

        created_at = candidate.pop("created_at", None)
        candidate["date"] = created_at.strftime("%Y-%m-%d") if created_at else None

        embedding_text = EmbeddingUtils.build_embedding_text(candidate)

        embedding_tasks.append(asyncio.create_task(embed_cb(vector_service.embed_and_store, candidate, embedding_text, job_title)))

        score = candidate.get("compatibility_analysis", {}).get("overall_score", 0)

        if score >= min_score_threshold:
            try:
                questions = await question_cb(ai_services.candidate_questions.generate, job=job_data, user_id=user_id, embedding_text=embedding_text)
            except Exception as e:
                logger.warning(f"Skipping questions for candidate {candidate_id}: {str(e)}")
                continue

            question_operations.append(
                {"candidate_id": candidate_id, "questions": questions, "created_at": get_current_time_utc(), "updated_at": get_current_time_utc()}
            )

            qualified_candidates.append(
                {"email": candidate["email"], "name": candidate["name"], "candidate_id": candidate_id, "unique_id": candidate["unique_id"]}
            )
        else:
            disqualified_candidates.append({"candidate_id": candidate_id})

    return embedding_tasks, question_operations, qualified_candidates, disqualified_candidates


async def _store_embeddings(embedding_tasks: list, batch_id: uuid.UUID):
    if not embedding_tasks:
        return

    results = await asyncio.gather(*embedding_tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Embedding failed during batch {batch_id}: {r}")

    logger.info(f"Embeddings stored for batch {batch_id}")


async def _store_candidate_questions(question_operations: list):
    if question_operations:
        await candidate_questions_col.insert_many(question_operations)


async def _notify_candidates_and_admin(qualified_candidates: list, user_id: str, company_id: str, job_id: str, job_title: str):
    user_details = redis.get_json_(f"user_details:{user_id}") or {}
    company_name = user_details.get("company_name", "Your Company")

    try:
        await _send_interview_invitations(qualified_candidates, company_name, job_id, job_title, company_id)
        logger.info(f"Sent invitations to {len(qualified_candidates)} candidates")
    except Exception as e:
        logger.warning(f"Failed to send invitations: {str(e)}")


async def _update_job_shortlist_count(job_id: str, count: int):
    if count > 0:
        await jobs.update_one({"_id": ObjectId(job_id)}, {"$inc": {"selection_progress.shortlisted_candidate_count": count}})
        logger.info(f"Updated shortlisted count by {count} for job {job_id}")


async def _update_candidate_shortlist_status(candidates_list: list[dict], shortlisted: bool) -> None:
    candidate_ids = [candidate.get("candidate_id") for candidate in candidates_list]

    update_query = {"updated_at": get_current_time_utc(), "progress": {"round_1": shortlisted}}
    if shortlisted:
        update_query["status"] = "Shortlisted"

    await candidates.update_many(
        {"_id": {"$in": [ObjectId(cid) for cid in candidate_ids]}},
        {"$set": update_query},
    )


async def _process_and_vectorize_candidates_batch(
    batch_id: uuid.UUID, job_id: str, company_id: str, user_id: str, should_send_invitations: bool = False
):
    """
    Orchestrates candidate processing: fetching, vectorizing, question generating, notifying, and batch finalizing.
    """
    logger.info(f"Starting processing for batch {batch_id}")

    try:
        job_data = await _fetch_job_data(job_id)
        batch_candidates = await _fetch_batch_candidates(batch_id, company_id)

        embeddings, question_operations, qualified_candidates, disqualified_candidates = await _process_candidates(
            batch_candidates, job_data, user_id, company_id
        )

        await _store_embeddings(embeddings, batch_id)
        await _store_candidate_questions(question_operations)

        if should_send_invitations and qualified_candidates:
            await _notify_candidates_and_admin(qualified_candidates, user_id, company_id, job_id, job_data.get("title"))
        else:
            logger.info(f"Skipping sending invitations for batch {batch_id}")

        if qualified_candidates:
            await _update_candidate_shortlist_status(qualified_candidates, True)
        if disqualified_candidates:
            await _update_candidate_shortlist_status(disqualified_candidates, False)

        await _update_job_shortlist_count(job_id, len(qualified_candidates))

        logger.info(f"Successfully processed batch {batch_id}")

    except Exception as e:
        logger.exception(f"Error processing batch {batch_id}: {str(e)}")


async def process_zip_extracted_files(
    extracted_dir: str, batch_id: uuid.UUID, job_id: str, user_id: str, company_id: str, send_invitations: bool = False
):
    from backend.dramatiq_config.background_task import (
        process_file_chunk_task,
        retry_failed_chunks_actor,
        r,
    )
    logger.info(f"Starting to process files from {extracted_dir}")

    try:
        files = [f for f in os.listdir(extracted_dir) if f.endswith((".pdf", ".docx"))]
        logger.info(f"Found {len(files)} files to process")

        CREATED_FILES.inc(len(files))

        chunks = [files[i : i + settings.CHUNK_SIZE] for i in range(0, len(files), settings.CHUNK_SIZE)]
        job_data = redis.get_json_(f"job:{job_id}")

        total_chunks_key = f"job:{job_id}:total_chunks"
        r.set(total_chunks_key, len(chunks))

        for idx, chunk in enumerate(chunks):
            chunk_data = {
                "chunk_id": f"{job_id}_chunk_{idx}",
                "chunk_files": chunk,
                "extracted_dir": extracted_dir,
                "batch_id": str(batch_id),
                "job_id": job_id,
                "job_data": job_data,
                "user_id": user_id,
                "company_id": company_id,
                "send_invitations": send_invitations,
            }
            process_file_chunk_task.send_with_options(args=(chunk_data,))


        # semaphore = asyncio.Semaphore(settings.MAX_CONCURRENCY)

        # async def process_chunk_with_semaphore(chunk):
        #     async with semaphore:
        #         start_time = time.time()

        #         try:
        #             await _process_file_chunk(chunk, extracted_dir, batch_id, job_id, job_data, user_id, company_id)
        #             CHUNKS_PROCESSED.inc()
        #         except Exception as e:
        #             CHUNKS_FAILED.inc()
        #             logger.error(f"Chunk failed: {chunk} - Error: {e}", exc_info=True)
        #         finally:
        #             duration = time.time() - start_time
        #             CHUNK_PROCESS_DURATION.observe(duration)

        # await asyncio.gather(*(process_chunk_with_semaphore(chunk) for chunk in chunks))

        # logger.info(f"Completed processing all chunks")

    #     asyncio.create_task(_process_and_vectorize_candidates_batch(batch_id, job_id, company_id, user_id, send_invitations))

    # finally:
    #     try:
    #         shutil.rmtree(os.path.dirname(extracted_dir))
    #         logger.info(f"Successfully cleaned up directory: {extracted_dir}")
    #     except Exception as e:
    #         logger.error(f"Failed to cleanup directory {extracted_dir}: {e}", exc_info=True)
    # Wait for background task to finish, then cleanup
    
    except Exception as e:
        logger.error(f"Error during processing files from {extracted_dir}: {e}", exc_info=True)
        raise

    else:   
        retry_failed_chunks_actor.send_with_options(args=(job_id,), delay=30000)
        
        # async def run_and_cleanup():
        #     try:
        #         await _process_and_vectorize_candidates_batch(batch_id, job_id, company_id, user_id, send_invitations)
        #     finally:
        #         with contextlib.suppress(FileNotFoundError):
        #             shutil.rmtree(extracted_dir)
        #             # shutil.rmtree(os.path.dirname(extracted_dir))
        #             logger.info(f"Successfully cleaned up directory: {extracted_dir}")
                
        #         # Push Prometheus metrics to PushGateway
        #         try:
        #             push_to_gateway("http://pushgateway:9091", job="file_chunk_processor_metrics", registry=registry)
        #         except Exception as e:
        #             logger.warning(f"Could not push metrics to Prometheus PushGateway: {e}")

        # asyncio.create_task(run_and_cleanup())

async def _update_batch_status(batch_id: uuid.UUID):
    batch = await batches.find_one_and_update(
        {"batch_id": Binary.from_uuid(batch_id)}, {"$set": {"status": "completed", "end_time": get_current_time_utc()}}, return_document=True
    )
    return batch


async def send_processing_completion_email(batch_id: uuid.UUID, user_details: dict, job_title: str, request: Request):
    batch = await _update_batch_status(batch_id)

    # Get batch Name and upload count
    batch_name = batch.get("batch_name")
    upload_count = batch.get("upload_count")

    # Calculate total time taken
    total_time_taken = batch.get("end_time") - batch.get("start_time")
    hours = total_time_taken.seconds // 3600
    minutes = (total_time_taken.seconds % 3600) // 60
    seconds = total_time_taken.seconds % 60

    if hours > 0:
        total_time_taken = (
            f"{hours} hour{'s' if hours > 1 else ''} {minutes} min{'s' if minutes != 1 else ''} and {seconds} second{'s' if seconds != 1 else ''}"
        )
    else:
        total_time_taken = f"{minutes} minute{'s' if minutes != 1 else ''} and {seconds} second{'s' if seconds != 1 else ''}"

    # Get User Details
    user_name = user_details.get("name")
    user_email = user_details.get("email")

    # Email template details
    subject = f"{upload_count} candidates processed - {job_title}"
    body_html = email_operation.render_template(
        "candidate_upload_progress.html",
        {
            "user_name": user_name,
            "batch_name": batch_name,
            "upload_count": upload_count,
            "total_duration": total_time_taken,
            "batch_link": f"{request.headers.get('origin')}/candidates/list?type=batch&batch_id={batch_id}&batch_name={quote(batch_name)}",
        },
    )

    # Send Email
    email_operation.send_email(subject, body_html, user_email)

    logger.info(f"Processing completion email sent for batch {batch_id}")
