import time
import base64

from typing import List
from fastapi import (
    APIRouter,
    File,
    Form,
    HTTPException,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import JSONResponse

from backend.db_config.db import collection
from backend.logging_config.logger import logger
from backend.security.perms import Permission
from backend.dramatiq_config.background_task import (
    zip_extract_and_prepare_actor,
)
from backend.api.deps import get_user_details_factory
from backend.types_ import UserData
from backend.utils import (
    create_batch_id,
)
from backend.upload.upload_validation import(
    batch_name_exists,
    is_supported_file_type,
    serialize_upload_file
)
from backend.monitor.metrices import (
    UPLOAD_REQUESTS,
    UPLOAD_SUCCESS,
    UPLOAD_FAILURE,
    UPLOAD_DURATION,
    push_to_gateway,
    registry,
)

router = APIRouter()

# Collections
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
    files: List[UploadFile] = File(
        ..., description="Multiple candidate files (PDF, DOCX or ZIP)"
    ),
    send_invitations: bool = Form(
        False, description="Send interview invitations to qualified candidates"
    ),
    # user_data: UserData = get_user_details_factory(PermissionChecker([Permission.MANAGE_CANDIDATES])),
    user_data: UserData = get_user_details_factory([Permission.MANAGE_CANDIDATES]),
) -> JSONResponse:

    start_time = time.time()
    logger.info("Upload endpoint triggered")
    UPLOAD_REQUESTS.inc()

    batch_id = None

    try:
        origin = request.headers.get("origin") if request else None
        
        # if await batches.find_one({"batch_name": batch_name}):
        #     raise HTTPException(
        #         status_code=status.HTTP_400_BAD_REQUEST,
        #         detail="Batch name already taken."
        #     )
        if await batch_name_exists(batch_name, batches):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Batch name already taken."
            )

        for file in files:
            logger.info(f"Processing file: {file.filename}")
            if not is_supported_file_type(file.content_type):
                logger.error(f"Invalid file type for {file.filename}: {file.content_type}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported file type: {file.filename}"
                )

        # Parse Role and User Details form UserData
        details, _ = user_data
        logger.debug(f"User details: {details}")

        # Read and serialize uploaded files
        files_data = [
            serialize_upload_file(file) 
            for file in files
        ]

        file_count = len(files_data)

        batch_id = create_batch_id()
        logger.info(f"Starting new upload batch: {batch_id}")

        logger.info(f"Send zip_file operations to background process: {batch_id}")

        # Send task to actor
        zip_extract_and_prepare_actor.send(
            job_id=job_id,
            batch_name=batch_name,
            files_data=files_data,
            user_details=details,
            batch_id=str(batch_id),
            send_invitations=send_invitations,
            origin=origin,
        )

        UPLOAD_SUCCESS.inc()

        # Create response before background processing
        return JSONResponse(
            content={
                "msg": f"Processing of {file_count} zip files started. You will receive an email when complete",
                "batch_id": str(batch_id),
                "status": True,
            },
            status_code=status.HTTP_201_CREATED,
        )

    except Exception as e:
        logger.exception(f"Error in upload batch {batch_id}: {str(e)}")
        UPLOAD_FAILURE.inc()

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing upload: {str(e)}",
        )

    finally:
        duration = (time.time() - start_time) * 1000
        UPLOAD_DURATION.observe(duration)
        try:
            push_to_gateway(
                "http://pushgateway:9091", job="fastapi_upload_route", registry=registry
            )
        except Exception as e:
            logger.warning(f"Could not push metrics to Prometheus PushGateway: {e}")
