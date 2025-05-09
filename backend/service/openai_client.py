import asyncio
from typing import Optional

import tenacity
from backend.config import settings
from backend.logging_config.logger import logger
from openai import (
    APIConnectionError,
    APIError,
    APITimeoutError,
    AsyncOpenAI,
    RateLimitError,
)

_openai_client: Optional[AsyncOpenAI] = None  # Stores OpenAI client instance
_lock = asyncio.Lock()  # Lock for thread-safe client initialization


async def get_openai_client():
    """Returns a singleton instance of the OpenAI client"""

    # Using global to use the outer scope _openai_client variable
    global _openai_client

    if _openai_client is None:
        # Ensures that only one coroutine at a time enters this block, prevents race condition
        async with _lock:

            # Double-check race condition, if another coroutine has already initialized the client while waiting for the lock
            if _openai_client is None:
                _openai_client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

    return _openai_client


# Exceptions to trigger a retry
def should_retry_error(error: Exception) -> bool:
    """
    Determines if the given error should trigger a retry attempt.

    Args:
        error: The exception to check

    Returns:
        bool: True if the error type should trigger a retry, False otherwise
    """
    return isinstance(
        error,
        (
            RateLimitError,
            APIError,
            APIConnectionError,
            APITimeoutError,
            asyncio.TimeoutError,
        ),
    )


# Retries when specific errors occur
@tenacity.retry(
    retry=tenacity.retry_if_exception(should_retry_error),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=25),
    stop=tenacity.stop_after_attempt(5),
    before_sleep=lambda retry_state: logger.warning(
        f"API error: {retry_state.outcome.exception()}, retrying in {retry_state.next_action.sleep} seconds..."
    ),
)
async def _create_chat_completion(timeout=60.0, **kwargs):
    """
    Wrapper for OpenAI API call with retry logic for various error types.
    Accepts all parameters that client.chat.completions.create accepts.

    Args:
        timeout: Maximum time in seconds to wait for API response (default: 60s)
        **kwargs: All parameters to pass to the OpenAI API
    """
    try:
        # Set default model if not provided
        kwargs.setdefault("model", settings.OPENAI_MODEL)

        # Get a cached OpenAI client instance
        client = await get_openai_client()

        # Wrap the API call with a timeout
        return await asyncio.wait_for(client.chat.completions.create(**kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"OpenAI API call timed out after {timeout} seconds")
        raise  # Re-raise the TimeoutError to be handled by the retry decorator
    except Exception as e:
        logger.error(f"OpenAI API error ({type(e).__name__}): {str(e)}")
        raise


@tenacity.retry(
    retry=tenacity.retry_if_exception(should_retry_error),
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=25),
    stop=tenacity.stop_after_attempt(5),
    before_sleep=lambda retry_state: logger.warning(
        f"API error: {retry_state.outcome.exception()}, retrying in {retry_state.next_action.sleep} seconds..."
    ),
)
async def _create_embeddings(input_text: str, timeout=60.0, **kwargs):
    """
    Wrapper for OpenAI API call to create embeddings with retry logic.
    Takes a string input and returns embeddings using the OpenAI embeddings API.

    Args:
        input_text: The text to create embeddings for
        timeout: Maximum time in seconds to wait for API response (default: 60s)
        **kwargs: Additional parameters to pass to the OpenAI API
    """
    try:
        # Set default model for embeddings if not provided
        kwargs.setdefault("model", settings.OPENAI_EMBEDDING_MODEL)
        kwargs["input"] = input_text

        # Get a cached OpenAI client instance
        client = await get_openai_client()

        # Wrap the API call with a timeout
        response = await asyncio.wait_for(client.embeddings.create(**kwargs), timeout=timeout)
        return response.data[0].embedding
    except asyncio.TimeoutError:
        logger.error(f"OpenAI API call timed out after {timeout} seconds")
        raise  # Re-raise the TimeoutError to be handled by the retry decorator
    except Exception as e:
        logger.error(f"OpenAI API error ({type(e).__name__}): {str(e)}")
        raise


# @tenacity.retry(
#     retry=tenacity.retry_if_exception(should_retry_error),
#     wait=tenacity.wait_exponential(multiplier=1, min=4, max=25),
#     stop=tenacity.stop_after_attempt(5),
#     before_sleep=lambda retry_state: logger.warning(
#         f"API error: {retry_state.outcome.exception()}, retrying in {retry_state.next_action.sleep} seconds..."
#     ),
# )
# async def _create_transcription(timeout=60.0, **kwargs):
#     """
#     Wrapper for OpenAI API call with retry logic for audio transcription.
#     Uses Whisper model with optimized settings for best quality transcription.

#     Args:
#         audio_file: The audio file to transcribe
#         timeout: Maximum time in seconds to wait for API response (default: 60s)
#     """
#     try:
#         # Set default model if not provided
#         kwargs.setdefault("model", settings.OPENAI_STT_MODEL)

#         # Get a cached OpenAI client instance
#         client = await get_openai_client()

#         # Wrap the API call with a timeout
#         return await asyncio.wait_for(client.audio.transcriptions.create(**kwargs), timeout=timeout)

#     except asyncio.TimeoutError:
#         logger.error(f"OpenAI API call timed out after {timeout} seconds")
#         raise  # Re-raise the TimeoutError to be handled by the retry decorator
#     except Exception as e:
#         logger.error(f"OpenAI API error ({type(e).__name__}): {str(e)}")
#         raise
