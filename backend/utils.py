import base64
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from backend.aws_config.aws import s3_operation
from backend.config import settings
from backend.logging_config.logger import logger
from bson import ObjectId
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorCollection


async def get_user(value: str, users: AsyncIOMotorCollection, field: str = "email"):
    """Get user from database by email"""
    if user := await users.find_one({field: value}):
        return user
    return None


def get_current_time_utc():
    return datetime.now(timezone.utc)


def validate_object_id(id_str: str, error_msg: Optional[str] = None) -> ObjectId:
    """
    Validates if the given string is a valid MongoDB ObjectId.
    Returns ObjectId if valid, raises HTTPException if invalid.
    """
    if id_str and ObjectId.is_valid(id_str):
        return ObjectId(id_str)
    else:
        raise HTTPException(
            status_code=400,
            detail=error_msg or "Invalid ID format. Must be a 24-character hex string.",
        )


def upload_objects_wrapper(
    paths: List[str],
    key: Optional[str] = None,
    subdir: Optional[str] = None,
) -> List[str]:
    try:
        uploaded_files_urls = []

        for path in paths:
            if not os.path.exists(path):
                continue

            # Construct S3 object name
            object_name = os.path.join(key or "", subdir or "", os.path.basename(path))
            object_name = object_name.replace("\\", "/").strip("/")

            # Upload and cleanup
            if s3_operation.upload_file(path, object_name):
                try:
                    os.remove(path)
                except OSError:
                    pass  # Log but don't fail if cleanup fails
                uploaded_files_urls.append(f"{settings.S3_OBJECT_BASE_PATH}/{object_name}")

        return uploaded_files_urls
    except Exception as e:
        logger.error(f"Failed to upload objects: {str(e)}")
        return []


def custom_pic_io(
    path: str,
    col: AsyncIOMotorCollection,
    object_id: str,
    subdir: str,
    update_param: str,
    key: str = "profile_pictures",
) -> None:
    try:
        if not os.path.exists(path):
            logger.error(f"File not found: {path}")
            return

        profile_pic_urls = upload_objects_wrapper(paths=[path], key=key, subdir=subdir)

        if not profile_pic_urls:
            logger.error("Failed to upload profile picture")
            return

        col.update_one({"_id": ObjectId(object_id)}, {"$set": {update_param: profile_pic_urls[0]}})
    except Exception as e:
        logger.error(f"Error in custom_pic_io: {str(e)}")


def save_base64_as_file(encoded_string: str, output_path: str) -> bool:
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Handle both with and without metadata prefix
        if "," in encoded_string:
            img_data = encoded_string.split(",", 1)[1]
        else:
            img_data = encoded_string

        # Decode and save
        with open(output_path, "wb") as f:
            f.write(base64.b64decode(img_data))
        return True
    except Exception as e:
        logger.error(f"Failed to save base64 file: {str(e)}")
        return False


def get_temp_path() -> str:
    temp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tmp")
    os.makedirs(temp_path, exist_ok=True)
    return temp_path


def generate_id(title: str, _type: str = "JOB") -> str:
    """
    Generate Timestamp based ID for the given type and title.
    """
    initials = "".join([word[0].upper() for word in title.split()[:2]])  # Get first two initials
    timestamp = time.strftime("%y%m%d")  # YYMMDD format

    return f"{_type}-{initials}-{timestamp}"


def create_batch_id() -> uuid.UUID:
    namespace = uuid.NAMESPACE_URL
    current_time = datetime.now(timezone.utc).isoformat()
    return uuid.uuid5(namespace, current_time)


def pdftotext_exe_path() -> str:
    # Define executable paths for different platforms
    default_paths = {
        "linux": "/usr/bin/pdftotext",
        "win32": r"C:\Program Files\xpdf-tools\bin64\pdftotext.exe",
    }

    # Get the executable path based on the system platform
    exe = default_paths.get(sys.platform, "")

    # Check if the file exists at the default path
    if not os.path.isfile(exe):
        raise FileNotFoundError("pdftotext executable/binary not found at the default path. Please install it or configure the correct path.")

    return exe


def is_valid_uuid(uuid_str):
    # Define a regular expression pattern for UUID
    uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

    # Check if the provided string matches the UUID pattern
    return bool(uuid_pattern.match(uuid_str))


def get_temp_path() -> str:
    # Get the directory containing utils.py
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Combine with 'temp' to get the absolute path of temp directory
    temp_path = os.path.join(current_directory, "tmp")

    return temp_path    


def generate_token() -> str:
    """
    Generate an atomically unique 128-bit GUID token.
    Uses combination of timestamp and random UUID to ensure uniqueness.
    Thread-safe implementation using system monotonic counter.

    Returns:
        str: A unique 128-bit GUID token as a hex string
    """
    # Use monotonic counter for thread-safe timestamp (8 bytes)
    timestamp = int(time.monotonic_ns() / 1_000_000).to_bytes(8, "big")
    # UUID4 is thread-safe by design (use full 16 bytes)
    random_bytes = uuid.uuid4().bytes
    token = base64.b64encode(timestamp + random_bytes).decode("utf-8")
    return token.replace("+", "-").replace("/", "_").rstrip("=")


class EmbeddingUtils(object):
    @staticmethod
    def extract_experience_years(text: str) -> float:
        """Extract years of experience from text"""
        if not isinstance(text, str):
            return 0
        years = months = 0
        y = re.search(r"(\d+)\s+years?", text)
        m = re.search(r"(\d+)\s+months?", text)
        if y:
            years = int(y.group(1))
        if m:
            months = int(m.group(1))
        return round(years + months / 12, 2)

    @staticmethod
    def build_embedding_text(doc: dict) -> str:
        """Build a minimal text representation used solely for embedding and semantic search"""
        text_parts = []

        # Matching-relevant fields
        if doc.get("name"):
            text_parts.append(doc["name"])
        if doc.get("location"):
            text_parts.append(f"from {doc['location']}")

        languages = [f"{lang.get('name', '')} ({lang.get('proficiency', '')})" for lang in doc.get("languages", [])]
        if languages:
            text_parts.append(f"Speaks: {', '.join(languages)}")

        if doc.get("designation"):
            text_parts.append(f"is a {doc['designation']}")

        if doc.get("total_experience"):
            exp_years = EmbeddingUtils.extract_experience_years(doc["total_experience"])
            text_parts.append(f"has {doc['total_experience']} (~{exp_years} years) of experience")

        overall_score = doc.get("compatibility_analysis", {}).get("overall_score", 0)
        if overall_score:
            text_parts.append(f"with overall score of {overall_score}")

        if doc.get("technical_skills"):
            text_parts.append(f"Technical Skills: {', '.join(doc['technical_skills'])}")
        if doc.get("soft_skills"):
            text_parts.append(f"Soft Skills: {', '.join(doc['soft_skills'])}")

        work_exp = []
        for exp in doc.get("work_experience", []):
            summary = exp.get("responsibility_summary", "")
            details = " | ".join(exp.get("responsibilities", []))
            line = (
                f"{exp.get('position', '')} at {exp.get('company_name', '')} for {exp.get('duration', '')}. {summary} Key responsibilities: {details}"
            )
            work_exp.append(line)
        if work_exp:
            text_parts.append(f"Work Experience: {'. '.join(work_exp)}")

        return ". ".join(text_parts)
