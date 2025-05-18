# from starlette.middleware.base import BaseHTTPMiddleware
# from prometheus_client import Summary
# import time

# REQUEST_LATENCY = Summary("request_latency_seconds", "Request latency", ["method", "endpoint"])

# class PrometheusMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request, call_next):
#         start_time = time.time()
#         response = await call_next(request)
#         process_time = time.time() - start_time
#         REQUEST_LATENCY.labels(request.method, request.url.path).observe(process_time)
#         return response


from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import Summary, CollectorRegistry, push_to_gateway
import time

class PrometheusMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, gateway_url="http://localhost:9091", job_name="fastapi_app"):
        super().__init__(app)
        self.registry = CollectorRegistry()
        self.gateway_url = gateway_url
        self.job_name = job_name
        # Register the summary metric with the custom registry
        self.request_latency = Summary(
            "request_latency_seconds",
            "Request latency",
            ["method", "endpoint"],
            registry=self.registry
        )

    async def dispatch(self, request, call_next):
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        self.request_latency.labels(request.method, request.url.path).observe(process_time)

        try:
            push_to_gateway(self.gateway_url, job=self.job_name, registry=self.registry)
        except Exception as e:
            print(f"Failed to push request latency metrics: {e}")

        return response
