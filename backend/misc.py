import base64
import json
import os
import random
import secrets
import string
import subprocess
import tempfile
from datetime import datetime
from typing import Literal

from backend.logging_config.logger import logger


class Toolkit:
    @staticmethod
    def generate_concatenated_random_strings(length1: int = 4, length2: int = 4) -> str:
        """Generates two random alphanumeric strings, capitalizes the first letter, and concatenates them.
        Inserts the current timestamp as numeric digits at random positions excluding index 0.
        If the timestamp is longer than the available positions, it slices the timestamp accordingly.
        """

        def insert_timestamp(generated_string, timestamp, random_indexes):
            new_string = []
            ts_idx = 0

            for i, char in enumerate(generated_string):
                new_string.append(char)
                if ts_idx < len(timestamp) and i == random_indexes[ts_idx]:
                    new_string.append(timestamp[ts_idx])
                    ts_idx += 1

            return "".join(new_string)

        # Define the alphabet (letters + digits)
        alphabet = string.ascii_letters + string.digits

        # Ensure the first character is uppercase
        first_char = secrets.choice(string.ascii_uppercase)

        # Generate remaining random characters
        remaining_chars = "".join(secrets.choice(alphabet) for _ in range(length1 + length2 - 1))
        generated_string = list(first_char + remaining_chars)

        # Use a short, unique timestamp in milliseconds (hex encoded)
        timestamp = f"{int(datetime.utcnow().timestamp() * 1000):x}"

        # Ensure timestamp fits into the available positions
        max_insert_length = min(len(timestamp), len(generated_string) - 1)
        timestamp = timestamp[:max_insert_length]

        # Pick random indexes excluding the first position
        available_indexes = list(range(1, len(generated_string)))
        random_indexes = sorted(random.sample(available_indexes, max_insert_length))

        # Insert timestamp characters at random positions
        return insert_timestamp(generated_string, timestamp, random_indexes)

    @staticmethod
    def image_to_base64_and_delete(filepath):
        try:
            with open(filepath, "rb") as image_file:
                encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
            os.remove(filepath)
            return encoded_string
        except (FileNotFoundError, PermissionError) as e:
            logger.error(f"error: can't encode image, path: {filepath}\n {e}")
            return None

    @classmethod
    def ocr_pdf(cls, pdf_path: str) -> Literal["text_pdf", "scanned_pdf", "corrupt_pdf"]:
        """
        Runs OCR on a PDF using ocrmypdf.

        Returns:
            - "text_pdf": If PDF already contains text and --redo-ocr is suggested.
            - "scanned_pdf": If OCR was successfully performed.
            - "corrupt_pdf": If PDF is corrupt or another error occurs.
        """
        pdf_out_dir = os.path.join(tempfile.gettempdir(), f"{cls.generate_concatenated_random_strings()}.pdf")
        cmd = ["ocrmypdf", pdf_path, pdf_out_dir]

        try:
            subprocess.run(cmd, capture_output=True, text=True, check=True)  # Automatically raise CalledProcessError on non-zero exit
            return "scanned_pdf"

        except subprocess.CalledProcessError as e:
            stderr = e.stderr.lower()
            logger.error(f"Error in ocr_pdf: {e}")
            if "--redo-ocr" in stderr:
                return "text_pdf"
            else:
                return "corrupt_pdf"
        finally:
            if os.path.exists(pdf_out_dir):
                os.remove(pdf_out_dir)

    @staticmethod
    def extract_json(blob: str):
        try:
            return json.loads(blob.split("```json")[1].split("```")[0])
        except:
            try:
                return json.loads(blob)
            except:
                return
