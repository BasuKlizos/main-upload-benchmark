import asyncio
import gc
import os
import tempfile

import cv2
import numpy as np
import pymupdf
import pytesseract
from backend.logging_config.logger import logger
from backend.misc import Toolkit
from langdetect import DetectorFactory, detect
from PIL import Image
from rapidfuzz import fuzz

DetectorFactory.seed = 0  # Consistent language detection


class ImageGrabber:
    """
    ImageGrabber class intelligently extracts and processes images from PDF files,
    automatically detects language, removes overlapping content between pages,
    and returns a list of base64-encoded images.
    """

    @staticmethod
    def page_to_image(page, zoom=4):
        """
        Converts a PDF page to a high-resolution PIL image.

        Args:
            page (pymupdf.Page): PDF page object.
            zoom (int, optional): Resolution scaling factor. Defaults to 4.

        Returns:
            PIL.Image: Rendered image of the PDF page.
        """
        mat = pymupdf.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        return Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    @staticmethod
    def ocr_text(img_crop, lang="eng", min_confidence=70):
        """
        Extracts text from an image crop using OCR with confidence filtering.

        Args:
            img_crop (PIL.Image): Image crop to extract text from.
            lang (str, optional): Language code for OCR. Defaults to "eng".
            min_confidence (int, optional): Minimum OCR confidence for text inclusion. Defaults to 70.

        Returns:
            str: OCR-extracted text with high confidence.
        """
        data = pytesseract.image_to_data(img_crop, lang=lang, output_type=pytesseract.Output.DICT)
        confident_text = " ".join([word for word, conf in zip(data["text"], data["conf"]) if int(conf) > min_confidence]).strip()
        return confident_text

    @staticmethod
    def detect_language(img):
        """
        Automatically detects the primary language of text within an image.

        Args:
            img (PIL.Image): Image to detect language from.

        Returns:
            str: Tesseract-compatible language code.
        """
        langdetect_to_tesseract = {
            "en": "eng",  # English
            "vi": "vie",  # Vietnamese
            "es": "spa",  # Spanish
        }

        sample_text = pytesseract.image_to_string(img, config="--psm 6").strip()
        if sample_text:
            try:
                detected_lang = detect(sample_text)
                tesseract_lang = langdetect_to_tesseract.get(detected_lang, "eng")
                logger.info(f"[🌐] Detected language: {detected_lang} -> Tesseract lang: {tesseract_lang}")
                return tesseract_lang
            except Exception as e:
                logger.error(f"[⚠️w] Language detection failed ({e}), defaulting to English.")
                return "eng"
        else:
            logger.debug("[⚠️] No text detected, defaulting to English.")
            return "eng"

    @classmethod
    def adaptive_overlap_trim(
        cls, prev_img, curr_img, lang="eng", min_overlap=50, max_overlap=200, similarity_threshold=0.95, text_similarity_threshold=80
    ):
        """
        Trims overlapping regions between two consecutive page images using pixel similarity and OCR-based fuzzy matching.

        Args:
            prev_img (PIL.Image): Image of the previous page.
            curr_img (PIL.Image): Image of the current page.
            lang (str, optional): Language code for OCR. Defaults to "eng".
            min_overlap (int, optional): Minimum pixel height for overlap detection. Defaults to 50.
            max_overlap (int, optional): Maximum pixel height for overlap detection. Defaults to 200.
            similarity_threshold (float, optional): Pixel similarity threshold for initial overlap detection. Defaults to 0.95.
            text_similarity_threshold (int, optional): OCR text similarity threshold (percentage). Defaults to 80.

        Returns:
            PIL.Image: Trimmed current page image.
        """
        prev_np, curr_np = np.array(prev_img), np.array(curr_img)
        best_similarity, best_overlap = 0, 0

        # Adaptive overlap detection based on pixel similarity
        for overlap_height in range(min_overlap, max_overlap + 1, 10):
            prev_slice = prev_np[-overlap_height:, :, :]
            curr_slice = curr_np[:overlap_height, :, :]
            diff = cv2.absdiff(prev_slice, curr_slice)
            similarity = 1 - (np.mean(diff) / 255)

            if similarity > best_similarity:
                best_similarity, best_overlap = similarity, overlap_height

        # OCR verification (fuzzy matching)
        if best_similarity >= similarity_threshold:
            prev_crop = prev_img.crop((0, prev_img.height - best_overlap, prev_img.width, prev_img.height))
            curr_crop = curr_img.crop((0, 0, curr_img.width, best_overlap))

            prev_text = cls.ocr_text(prev_crop, lang)
            curr_text = cls.ocr_text(curr_crop, lang)

            text_similarity = fuzz.ratio(prev_text, curr_text) / 100

            if text_similarity >= 0.8:
                logger.info(f"[✅] Fuzzy OCR overlap confirmed ({text_similarity:.3f}, {best_overlap}px). Trimming.")
                return curr_img.crop((0, best_overlap, curr_img.width, curr_img.height))

        logger.info(f"[⚠️] No confirmed overlap (similarity={best_similarity:.3f}). Not trimming.")
        return curr_img

    @classmethod
    async def extract_pages_parallel(cls, pdf_path, zoom=4, min_overlap=50, max_overlap=200):
        """
        Processes and extracts images from a PDF file asynchronously, handles overlaps, and returns base64-encoded images.

        Args:
            pdf_path (str): Path to the input PDF file.
            zoom (int, optional): Resolution scaling factor. Defaults to 4.
            min_overlap (int, optional): Minimum overlap height in pixels. Defaults to 50.
            max_overlap (int, optional): Maximum overlap height in pixels. Defaults to 200.

        Returns:
            list: List of base64-encoded image strings.
        """

        pdf_file = pymupdf.open(pdf_path)
        total_pages = pdf_file.page_count
        images = {}
        base64_encoded_images = []

        # Detect language on first page
        first_img = await asyncio.to_thread(cls.page_to_image, pdf_file.load_page(0), zoom)
        lang = await asyncio.to_thread(cls.detect_language, first_img) or "eng"

        # Create tasks for rendering all pages
        tasks = []
        for i in range(total_pages):
            task = asyncio.create_task(asyncio.to_thread(cls.page_to_image, pdf_file.load_page(i), zoom))
            tasks.append((i, task))

        # Wait for tasks to complete
        for idx, task in tasks:
            images[idx] = await task
            logger.info(f"[+] Rendered page {idx+1}/{total_pages}")

        # Process each page sequentially (handling overlaps)
        for idx in range(total_pages):
            curr_img = images[idx]

            if idx > 0:
                prev_img = images[idx - 1]
                curr_img = await asyncio.to_thread(cls.adaptive_overlap_trim, prev_img, curr_img, lang, min_overlap, max_overlap)

            filename = os.path.join(tempfile.gettempdir(), f"{Toolkit.generate_concatenated_random_strings(length1=5, length2=5)}.png")
            await asyncio.to_thread(curr_img.save, filename, optimize=True)
            logger.info(f"[✅] Saved {filename}")

            images[idx] = curr_img
            base64_encoded_images.append(await asyncio.to_thread(Toolkit.image_to_base64_and_delete, filename))
            if os.path.exists(filename):
                os.remove(filename)
            if idx % 10 == 0:
                gc.collect()

        return base64_encoded_images


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("pdf_path", help="PDF file path")
    parser.add_argument("--zoom", type=int, default=4, help="Zoom factor")
    parser.add_argument("--min_overlap", type=int, default=50)
    parser.add_argument("--max_overlap", type=int, default=200)
    parser.add_argument("--similarity_threshold", type=float, default=0.95)
    args = parser.parse_args()

    asyncio.run(ImageGrabber.extract_pages_parallel(args.pdf_path, args.zoom, args.min_overlap, args.max_overlap))
