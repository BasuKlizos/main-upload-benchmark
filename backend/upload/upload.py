import asyncio
import io
import time
import os.path
import shutil
import zipfile
from typing import List

from backend.upload.utils import (
    get_job_data,
)
from backend.db_config.db import collection
from backend.logging_config.logger import logger
from backend.security.perms import Permission
from backend.dramatiq_config.background_task import process_zip_task
from backend.api.deps import get_user_details_factory
from backend.types_ import UserData
from backend.utils import (
    create_batch_id,
    get_current_time_utc,
    get_temp_path,
)
from bson import Binary, ObjectId
from fastapi import (
    APIRouter,
    Depends,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse
from backend.monitor.metrices import (
    UPLOAD_REQUESTS,
    UPLOAD_SUCCESS,
    UPLOAD_FAILURE,
    UPLOAD_DURATION,
    FILE_COUNT,
    ZIP_FILES,
    CREATED_FILES,
    UNSUPPORTED_FILES,
    push_to_gateway,
    registry
)

router = APIRouter()

# Collections
jobs = collection("jobs")
batches = collection("batches")


@router.post(
    "/bulk",
    summary="Upload multiple candidate files",
    response_description="Return HTTP 201 Created",
    status_code=status.HTTP_201_CREATED,
    response_class=JSONResponse,
)
async def upload_candidates(
    request: Request,
    job_id: str = Form(..., description="ID of the job to upload candidates for"),
    batch_name: str = Form(..., description="Name of the batch"),
    files: List[UploadFile] = File(..., description="Multiple candidate files (PDF, DOCX or ZIP)"),
    send_invitations: bool = Form(False, description="Send interview invitations to qualified candidates"),
    # user_data: UserData = get_user_details_factory(PermissionChecker([Permission.MANAGE_CANDIDATES])),
    user_data: UserData = get_user_details_factory([Permission.MANAGE_CANDIDATES]),
) -> JSONResponse:
    
    start_time = time.time()
    logger.info("Upload endpoint triggered")
    UPLOAD_REQUESTS.inc()

    batch_id = None 

    try:
        logger.debug(f"Received batch_name: {batch_name} for job_id: {job_id}")
        # Check if batch_name is already taken
        if await batches.find_one({"batch_name": batch_name}):
            logger.warning(f"Batch name already taken: {batch_name}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch name already taken",
            )

        # Get job data
        job = await get_job_data(job_id)
        logger.debug(f"Fetched job data: {job}")

        # Parse Role and User Details form UserData
        details, _ = user_data
        logger.debug(f"User details: {details}")

        batch_id = create_batch_id()
        logger.info(f"Starting new upload batch: {batch_id}")

        batch_directory = os.path.join(get_temp_path(), str(batch_id))
        logger.info(f"Creating temp directory: {batch_directory}")

        os.makedirs(batch_directory, exist_ok=True)

        for file in files:
            logger.info(f"Processing uploaded file: {file.filename}")
            FILE_COUNT.inc()
            if file.content_type not in [
                "application/zip",
                "application/x-zip-compressed",
                "application/pdf",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ]:
                logger.error(f"Unsupported file type: {file.filename} -> {file.content_type}")

                UNSUPPORTED_FILES.inc()
                
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File {file.filename} is not supported. Only ZIP files are allowed",
                )

            # Check file type and process accordingly
            if file.content_type in ["application/zip", "application/x-zip-compressed"]:
                logger.debug(f"ZIP file detected: {file.filename}")
                ZIP_FILES.inc()

                contents = await file.read()
                temp_extract_dir = os.path.join(batch_directory, "_temp_extract")
                os.makedirs(temp_extract_dir, exist_ok=True)

                with zipfile.ZipFile(io.BytesIO(contents)) as zip_file:
                    zip_file.extractall(temp_extract_dir)

                # Find all files recursively and move them directly to batch directory
                for root, _, files in os.walk(temp_extract_dir):
                    for filename in files:
                        if filename.lower().endswith((".pdf", ".docx")):
                            src_path = os.path.join(root, filename)
                            # Generate unique name to avoid conflicts
                            base, ext = os.path.splitext(filename)
                            # Use timestamp to ensure uniqueness
                            dest_filename = f"{base}_{get_current_time_utc().timestamp()}{ext}"
                            dest_path = os.path.join(batch_directory, dest_filename)
                            shutil.move(src_path, dest_path)
                            logger.debug(f"Moved file from {src_path} to {dest_path}")

                # Clean up temporary extraction directory
                shutil.rmtree(temp_extract_dir)
                logger.debug(f"Cleaned up temporary ZIP extraction directory: {temp_extract_dir}")

            elif file.content_type in ["application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
                logger.debug(f"Non-ZIP file detected: {file.filename}")
                CREATED_FILES.inc()

                base, ext = os.path.splitext(file.filename)
                # Use timestamp to ensure uniqueness
                dest_filename = f"{base}_{get_current_time_utc().timestamp()}{ext}"
                file_path = os.path.join(batch_directory, dest_filename)
                with open(file_path, "wb") as f:
                    f.write(await file.read())
                logger.debug(f"Saved file to: {file_path}")
            else:
                UNSUPPORTED_FILES.inc()

                logger.error(f"Invalid file type for {file.filename}: {file.content_type}, Skipping file")
                continue

        # Get the count of files in the batch directory
        file_count = len([f for f in os.listdir(batch_directory) if f.lower().endswith((".pdf", ".docx"))])
        logger.info(f"Total valid files to process: {file_count}")

        # Insert batch record and update job count before background processing
        await batches.insert_one(
            {
                "uploaded_by": ObjectId(details.get("user_id")),
                "company_id": ObjectId(details.get("company_id")),
                "batch_id": Binary.from_uuid(batch_id),
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
            {"$set": {"updated_at": get_current_time_utc()}, "$inc": {"selection_progress.total_candidate_count": file_count}},
        )
        logger.debug("Updated job record with candidate count")

        UPLOAD_SUCCESS.inc()

        # Create response before background processing
        response = JSONResponse(
            content={
                "msg": f"Processing of {file_count} candidates started. You will receive an email when complete",
                "batch_id": str(batch_id),
                "status": True,
            },
            status_code=status.HTTP_201_CREATED,
        )

        # Process all extracted directories in background
        # async def background_processing():
        #     process_start_time = time.time()
        #     try:
        #         if os.path.exists(batch_directory):
        #             logger.info(f"Starting background processing for directory: {batch_directory}")
        #             await process_zip_extracted_files(
        #                 extracted_dir=batch_directory,
        #                 batch_id=batch_id,
        #                 job_id=job_id,
        #                 user_id=details.get("user_id"),
        #                 company_id=details.get("company_id"),
        #                 send_invitations=send_invitations,
        #             )

        #             # Send Processing completion email to user
        #             await send_processing_completion_email(batch_id, details, job.get("title"), request)
        #             EMAIL_SENT.inc()
        #         else:
        #             logger.error(f"Extracted directory does not exist: {batch_directory}")

        #     except Exception as e:
        #         logger.exception(f"Error in upload batch {batch_id}: {str(e)}")
        #         try:
        #             shutil.rmtree(batch_directory)
        #             logger.debug(f"Cleaned up temp directory after error: {batch_directory}")
        #         except Exception as cleanup_error:
        #             logger.error(f"Failed to cleanup temp directory: {cleanup_error}", exc_info=True)

        #     # Log process duration
        #     process_duration = time.time() - process_start_time
        #     PROCESS_DURATION.observe(process_duration)

        # asyncio.create_task(background_processing())
        
        
        origin = request.headers.get("origin") if request else None
        logger.debug(f"Sending background task with origin: {origin}") 

        process_zip_task.send(
            batch_directory=batch_directory,
            batch_id=str(batch_id),
            job_id=job_id,
            user_id=details.get("user_id"),
            company_id=details.get("company_id"),
            send_invitations=send_invitations,
            origin=origin
        )
        
        return response

    except Exception as e:
        logger.exception(f"Error in upload batch {batch_id}: {str(e)}")
        # Cleanup on error
        try:
            shutil.rmtree(batch_directory)
            logger.debug(f"Cleaned up temp directory after error: {batch_directory}")
        except Exception as cleanup_error:
            logger.error(f"Failed to cleanup temp directory: {cleanup_error}", exc_info=True)
        
        UPLOAD_FAILURE.inc()
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing upload: {str(e)}",
        )
    
    finally:
        duration = time.time() - start_time
        UPLOAD_DURATION.observe(duration)
        try:
            push_to_gateway("http://pushgateway:9091", job="fastapi_upload_route", registry=registry)
        except Exception as e:
            logger.warning(f"Could not push metrics to Prometheus PushGateway: {e}")