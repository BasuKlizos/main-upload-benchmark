import asyncio
from typing import Any, Optional

import httpx
from backend.config import settings
from backend.logging_config.logger import logger


class CallerAPIClient:
    def __init__(self):
        self.base_url = settings.CALLER_SERVER_URL
        self.timeout = settings.REQUEST_TIMEOUT if hasattr(settings, "REQUEST_TIMEOUT") else 30
        self.max_retries = 3
        self.retry_delay = 1  # seconds

    async def create_request(
        self,
        route: str,
        method: str = "GET",
        data: Optional[dict] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        retries = 0
        while retries < self.max_retries:
            try:
                async with httpx.AsyncClient(timeout=timeout or self.timeout) as client:
                    request_kwargs = {
                        "url": f"{self.base_url.rstrip('/')}/{route.lstrip('/')}",
                        "headers": {"accept": "application/json", "Content-Type": "application/json"},
                    }

                    if params is not None:
                        request_kwargs["params"] = params
                    if headers is not None:
                        request_kwargs["headers"].update(headers)

                    if data is not None:
                        if method.upper() in ["POST", "PUT", "PATCH"]:
                            request_kwargs["json"] = data
                        else:
                            request_kwargs["params"] = {**(params or {}), **data}

                    response = await getattr(client, method.lower())(**request_kwargs)
                    response.raise_for_status()
                    return response.json()

            except httpx.ConnectError as e:
                retries += 1
                if retries == self.max_retries:
                    logger.error(f"Failed to connect after {self.max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Connection attempt {retries} failed, retrying in {self.retry_delay}s...")
                await asyncio.sleep(self.retry_delay)

            except Exception as e:
                logger.error(f"Request failed: {str(e)}")
                raise

    async def create_interview_link(self, payload: dict) -> dict:
        """Create interview link with retry mechanism"""
        return await self.create_request(route="call/response/create", method="POST", data=payload)

    async def generate_company_api_key(self, payload: dict) -> dict:
        """Generate company API key"""
        return await self.create_request(route=f"api-key/generate", method="POST", data=payload)


caller_api_client = CallerAPIClient()
