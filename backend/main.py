from fastapi import FastAPI, status
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get("/")
async def home():
    return JSONResponse(
        content={"message": "I am Home", "status": True}, 
        status_code=status.HTTP_200_OK
    )
