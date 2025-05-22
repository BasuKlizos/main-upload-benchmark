import uuid
import json

from fastapi import FastAPI, status
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import make_asgi_app

from backend.monitor.middleware import PrometheusMiddleware
from backend.upload.upload import router
from backend.dramatiq_config.background_task import(
    resume_processing_from_backup,
    r,
)

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


@app.on_event("startup")
async def startup_event():
    raw_context = r.get("last_job_context")
    if raw_context:
        try:
            context = json.loads(raw_context.decode('utf-8'))
            job_id = context["job_id"]
            batch_id = uuid.UUID(context["batch_id"])
            job_data = context["job_data"]
            user_id = context["user_id"]
            company_id = context["company_id"]

            await resume_processing_from_backup(job_id, batch_id, job_data, user_id, company_id)
        except Exception as e:
            print(f"[Startup] Failed to resume from backup: {str(e)}")
    else:
        print("[Startup] No job context found — skipping resume.")


# Home route
@app.get("/")
async def home():
    return JSONResponse(
        content={"message": "I am Home", "status": True}, 
        status_code=status.HTTP_200_OK
    )

# Include your upload router
app.include_router(router)
