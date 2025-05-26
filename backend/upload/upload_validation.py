import base64

from fastapi import UploadFile

ALLOWED_CONTENT_TYPES = [
    "application/zip",
    "application/x-zip-compressed",
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]

def is_supported_file_type(content_type: str) -> bool:
    return content_type in ALLOWED_CONTENT_TYPES

def serialize_upload_file(file: UploadFile) -> dict:
    content = file.file.read()
    return {
        "filename": file.filename,
        "content_type": file.content_type,
        "content": base64.b64encode(content).decode("utf-8"),
    }

async def batch_name_exists(batch_name: str, batches) -> bool:
    return await batches.find_one({"batch_name": batch_name}) is not None
