from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import make_asgi_app

from backend.monitor.middleware import PrometheusMiddleware
from backend.upload.upload import router

app = FastAPI()

app.add_middleware(PrometheusMiddleware)

# # Instrument and expose Prometheus metrics
# instrumentator = Instrumentator(
#     should_group_status_codes=True,
#     should_ignore_untemplated=True,
#     should_respect_env_var=True,
# )
# instrumentator.instrument(app).expose(app)

# # Mount /metrics endpoint
# metrics_app = make_asgi_app()
# app.mount("/metrics", metrics_app)

# Home route
@app.get("/")
async def home():
    return JSONResponse(
        content={"message": "I am Home", "status": True}, 
        status_code=status.HTTP_200_OK
    )

# Include your upload router
app.include_router(router)
