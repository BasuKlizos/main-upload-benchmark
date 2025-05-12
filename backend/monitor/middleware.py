from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Summary
import time

REQUEST_LATENCY = Summary("request_latency_seconds", "Request latency", ["method", "endpoint"])

class PrometheusMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        REQUEST_LATENCY.labels(request.method, request.url.path).observe(process_time)
        return response
