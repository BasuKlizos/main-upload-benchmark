import asyncio
import os.path
import subprocess
import tempfile

from backend.common.exceptions import (
    CorruptFileException,
    EmptyFileException,
    FileSystemException,
    ImageToTextException,
    PDFConversionFailedException,
    PdfToImageException,
)
from backend.core.docx_to_text import DocxExtractor
from backend.core.imagepdf_to_text import ImageGrabber
from backend.logging_config.logger import logger
from backend.misc import Toolkit
from backend.service.ai import ai_services
from backend.utils import pdftotext_exe_path
from openai import APIConnectionError, APIError, APITimeoutError, RateLimitError


class _ExtractTextFromFile:
    """Handles extraction of text from PDF files"""

    def __init__(self):
        self.pdftotext_path = pdftotext_exe_path()

    async def _get_pdf_metadata(self, pdf_file_path: str) -> dict:
        try:
            process = await asyncio.create_subprocess_exec(
                "pdfinfo",
                "-enc",
                "UTF-8",
                pdf_file_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                logger.warning("pdfinfo failed: %s", stderr.decode("utf-8"))
                return {"pages": -1, "file_size_kb": -1}

            metadata = {"pages": -1, "file_size_kb": -1}
            for line in stdout.decode("utf-8").splitlines():
                if line.startswith("Pages:"):
                    metadata["pages"] = int(line.split(":")[1].strip())
                elif line.startswith("File size:"):
                    # Extract numeric value and convert to bytes
                    size_str = line.split(":")[1].strip().split()[0]
                    metadata["file_size_kb"] = int(size_str) * 1024

            return metadata

        except Exception as e:
            logger.error("Exception while running pdfinfo for %s: %s", pdf_file_path, str(e))

        return {"pages": -1, "file_size_kb": -1}

    async def _convert_pdf_to_text(self, pdf_file_path: str, output_text_file_name: str) -> tuple[str, dict]:
        """Convert PDF file to text using pdftotext utility and log number of pages."""
        if not os.path.isfile(pdf_file_path):
            logger.error("PDF file does not exist: %s", pdf_file_path)
            return "error: File not found"

        # Step 1: Get number of pages metadata using `pdfinfo`
        metadata = await self._get_pdf_metadata(pdf_file_path)
        metadata["file_type"] = "PDF"

        # Step 2: Convert PDF to text
        command = [
            self.pdftotext_path,
            "-layout",
            "-table",
            "-nopgbrk",
            "-enc",
            "UTF-8",
            pdf_file_path,
            output_text_file_name,
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            _, stderr_bytes = await process.communicate()

            if process.returncode != 0:
                e = subprocess.CalledProcessError(process.returncode, command)
                e.stderr = stderr_bytes.decode("utf-8")
                raise e

            return output_text_file_name, metadata

        except subprocess.CalledProcessError as e:
            error_message = e.stderr.lower()
            if "warning" in error_message:
                if "may not be a pdf file" in error_message:
                    return "error: Malformed pdf file"
                logger.warning("Warning in pdf_to_text conversion: %s", error_message)
            else:
                logger.error("Error converting pdf to text: %s", error_message)
                return "error_converting_pdf_to_text"

    async def extract_text(self, file_path: str, user_id: str) -> tuple[str, bool, dict]:
        """Extract text from Files (pdf or docx)"""
        txt_temp = None
        is_image_pdf = False
        try:
            if file_path.endswith(".pdf"):
                with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as txt_temp:
                    result, metadata = await self._convert_pdf_to_text(file_path, txt_temp.name)

                    if result.startswith("error:"):
                        raise PDFConversionFailedException(f"PDF conversion failed: {result}")

                    with open(txt_temp.name, "r", encoding="utf-8") as f:
                        text = f.read()

                    if not text:
                        # for pdfs we'll see if it is really scanned pdf or a empty file or a corrupt pdf.
                        pdf_signal = Toolkit.ocr_pdf(file_path)
                        if pdf_signal == "corrupt_pdf":
                            raise CorruptFileException(f"{os.path.basename(file_path)} is corrupted.")
                        elif pdf_signal == "text_pdf":
                            raise EmptyFileException(f"{os.path.basename(file_path)} is empty.")
                        else:
                            # For scanned pdfs 1st we'll grab the images
                            logger.info(f"Scanned pdf detected, file_name: {file_path}, initialing fallback...")
                            try:
                                base64_images_list = await ImageGrabber.extract_pages_parallel(file_path)
                            except Exception as e:
                                raise PdfToImageException(f"Can't pull images from {file_path}, \nerror: {e}")
                            # Initiate image to json
                            try:
                                text = await ai_services.image_translator._parse_img_text(base64_images_list, user_id)
                                is_image_pdf = True
                                logger.info(f"Extracted text from scanned pdf, candidate name: {text.name}")
                            except (RateLimitError, APIError, APIConnectionError, APITimeoutError, asyncio.TimeoutError):
                                raise
                            except Exception as e:
                                raise ImageToTextException(e)

            elif file_path.lower().endswith((".docx", ".doc")):
                text, metadata = await DocxExtractor.extract_docx_structure_async(file_path)
                metadata["file_type"] = "DOCX"

            return text, is_image_pdf, metadata

        except Exception as e:
            logger.error("Error in extract_text: %s", str(e))
            raise
        finally:
            if txt_temp:
                try:
                    os.unlink(txt_temp.name)
                except Exception as e:
                    logger.error(f"Can't delete file: {txt_temp.name}")
                    raise FileSystemException(e)


extract = _ExtractTextFromFile()
