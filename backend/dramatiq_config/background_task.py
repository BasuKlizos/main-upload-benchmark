import dramatiq
import asyncio

from dramatiq.brokers.redis import RedisBroker
from backend.upload.utils import (
    process_zip_extracted_files,
    send_processing_completion_email,
)
from backend.logging_config.logger import logger

# Connect to Redis broker
broker = RedisBroker("redis://localhost:6379/0")
dramatiq.set_broker(broker)


@dramatiq.actor(
    actor_name="process_zip_file_task",
    max_retries=5,
    time_limit=600_000,  # in milliseconds | 10 minutes
    retry_backoff=300_000,  # in milliseconds | 5 minutes
)
def process_zip_task(
    batch_directory, batch_id, job_id, user_id, company_id, send_invitations
):
    async def process_zip_extracted():
        logger.info(f"Started processing batch {batch_id}...")

        await process_zip_extracted_files(
            extracted_dir=batch_directory,
            batch_id=batch_id,
            job_id=job_id,
            user_id=user_id,
            company_id=company_id,
            send_invitations=send_invitations,
        )

        await send_processing_completion_email(batch_id, job_id, user_id)
        logger.info(f"Completed processing batch {batch_id}.")

    try:
        asyncio.run(process_zip_extracted())
    except Exception as e:
        logger.error(f"Error in 'process_zip_file_task': {e}")
        raise dramatiq.RetryLater(delay=300_000)  # retry after 5 minutes
