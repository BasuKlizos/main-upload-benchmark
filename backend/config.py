from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings
from starlette.config import Config

from backend.logging_config.logger import logger

config = Config(env_file=".env")


class Settings(BaseSettings):
    """
    Application settings class that manages all configuration variables.
    Settings can be overridden by environment variables.
    """

    PROJECT_NAME: str = "TalentSync"
    PROJECT_DESCRIPTION: str = "TalentSync is an AI based platform for efficient talent management and recruitment."
    VERSION: str = "1.0.0"

    DEBUG: bool = config("DEBUG", cast=bool, default=False)

    ALLOWED_ORIGINS: List[AnyHttpUrl] = [
        "http://localhost:5173",
        "https://ai.recruiter.klizos.com",
    ]

    FRONTEND_URL: str = config("FRONTEND_URL", cast=str, default="http://localhost:5173")

    # Security Settings
    JWT_SECRET_KEY: str = config("JWT_SECRET_KEY", cast=str)
    EXTERNAL_SECRET_KEY: str = config("EXTERNAL_SECRET_KEY", cast=str)

    ACCESS_TOKEN_EXPIRE_MINUTES: int = config("ACCESS_TOKEN_EXPIRE_MINUTES", cast=int, default=1400)
    AUTHENTICATION_ALGORITHM: str = "HS256"

    # API Documentation Auth
    DOC_ROOT_USERNAME: str = config("DOC_ROOT_USERNAME", cast=str)
    DOC_ROOT_PASSWORD: str = config("DOC_ROOT_PASSWORD", cast=str)

    # MongoDB Settings
    # MONGO_USERNAME: Optional[str] = config("MONGO_USERNAME", cast=str, default=None)
    # MONGO_PASSWORD: Optional[str] = config("MONGO_PASSWORD", cast=str, default=None)
    # MONGO_HOST: str = config("MONGO_HOST", cast=str, default="localhost")
    MONGO_HOST: str = config("MONGO_HOST", cast=str, default="mongodb")
    MONGO_PORT: int = config("MONGO_PORT", cast=int, default=27017)
    MONGO_DB: str = config("MONGO_DB", cast=str, default="talentsync")
    MONGO_DSN: Optional[str] = None

    API_V1_STR: str = config("API_V1_STR", cast=str, default="/api/v1")

    @field_validator("MONGO_DSN", mode="before")
    @classmethod
    def assemble_mongo_dsn(cls, v: Optional[str], info: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v

        values = info.data
        if values.get("MONGO_USERNAME") and values.get("MONGO_PASSWORD"):
            return f"mongodb://{values.get('MONGO_USERNAME')}:{values.get('MONGO_PASSWORD')}@{values.get('MONGO_HOST')}:{values.get('MONGO_PORT')}/{values.get('MONGO_DB')}?authMechanism=DEFAULT&authSource=admin"
        return f"mongodb://{values.get('MONGO_HOST')}:{values.get('MONGO_PORT')}/{values.get('MONGO_DB')}"

    @field_validator("ALLOWED_ORIGINS", mode="before")
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            return [i.strip() for i in v.split(",")]
        return v

    # Redis Credentials
    REDIS_PORT: int = config("REDIS_PORT", cast=int, default=6379)
    REDIS_HOST: str = config("REDIS_HOST", cast=str, default="localhost")
    REDIS_DB: int = config("REDIS_DB", cast=int, default=0)
    REDIS_PASSWORD: Optional[str] = config("REDIS_PASSWORD", cast=str, default="")
    REDIS_CACHE_EXPIRY_TIME: int = config("REDIS_CACHE_EXPIRY_TIME", cast=str, default=120)

    # Dev Permission
    ENV:str = config("ENV", default="dev")

    # AWS Credentials
    AWS_ACCESS_KEY: str = config("AWS_ACCESS_KEY", cast=str)
    AWS_ACCESS_SECRET: str = config("AWS_ACCESS_SECRET", cast=str)
    AWS_BUCKET_NAME: str = config("AWS_BUCKET_NAME", cast=str)
    AWS_S3_ENVIRONMENT: str = config("AWS_S3_ENVIRONMENT", cast=str, default="prod")
    AWS_ACCESS_REGION: str = config("AWS_ACCESS_REGION", cast=str, default="eu-west-1")

    S3_OBJECT_BASE_PATH: str = f"https://{AWS_BUCKET_NAME}.s3.{AWS_ACCESS_REGION}.amazonaws.com"

    # Optional CV parsing settings
    MAX_CV_SIZE_MB: int = 10
    SUPPORTED_CV_FORMATS: List[str] = ["application/pdf"]

    # OpenAI API Key
    OPENAI_API_KEY: str = config("OPENAI_API_KEY", cast=str)
    OPENAI_MODEL: str = config("OPENAI_MODEL", cast=str, default="gpt-4o-mini")
    OPENAI_STT_MODEL: str = config("OPENAI_STT_MODEL", cast=str, default="whisper-1")
    OPENAI_EMBEDDING_MODEL: str = config("OPENAI_EMBEDDING_MODEL", cast=str, default="text-embedding-ada-002")
    logger.info(f"Openai api key: {OPENAI_API_KEY}")

    # # FreJun API Creds
    # FREJUN_API_KEY: str = config("FREJUN_API_KEY", cast=str)
    # FREJUN_API_URL: str = config("FREJUN_API_URL", cast=str, default="https://api.frejun.com/api/v1/integrations")
    # # Headers required for making requests to the FreJun API
    # FREJUN_REQUEST_HEADERS: Dict[str, str] = {
    #     "Authorization": f"Api-Key {FREJUN_API_KEY}",
    #     "Content-Type": "application/json",
    #     "Accept": "application/json",
    # }

    # Email Configs
    EMAIL_FROM: str = config("EMAIL_FROM", cast=str)

    # Vector DB Settings
    PINECONE_API_KEY: str = config("PINECONE_API_KEY", cast=str, default="")
    PINECONE_REGION: str = config("PINECONE_REGION", cast=str, default="us-east-1")

    PINECONE_INDEX_NAME: str = config("INDEX_NAME", cast=str, default="candidates")
    TRANSFORMER_MODEL: str = config("TRANSFORMER_MODEL", cast=str, default="all-MiniLM-L6-v2")

    # Google OAuth Settings
    GOOGLE_CLIENT_ID: str = config("GOOGLE_CLIENT_ID", cast=str)
    GOOGLE_CLIENT_SECRET: str = config("GOOGLE_CLIENT_SECRET", cast=str)

    # LinkedIn OAuth Settings
    LINKEDIN_CLIENT_ID: str = config("LINKEDIN_CLIENT_ID", cast=str)
    LINKEDIN_CLIENT_SECRET: str = config("LINKEDIN_CLIENT_SECRET", cast=str)

    # Caller Server  URL
    CALLER_SERVER_URL: str = config("CALLER_SERVER_URL", cast=str)
    REQUEST_TIMEOUT: int = config("REQUEST_TIMEOUT", cast=int, default=30)

    # Resume Upload Config
    CHUNK_SIZE: int = config("CHUNK_SIZE", cast=int, default=4)
    MAX_CONCURRENCY: int = config("MAX_CONCURRENCY", cast=int, default=8)
    BULK_INSERT_BATCH_SIZE: int = config("BULK_INSERT_BATCH_SIZE", cast=int, default=500)


settings = Settings()
