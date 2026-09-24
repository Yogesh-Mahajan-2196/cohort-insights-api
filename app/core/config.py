from pydantic.v1 import BaseSettings

class Settings(BaseSettings):
    APP_NAME: str = "Cohort Insights API"
    ENVIRONMENT: str = "development"

    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DATABASE: str = "cohort_insights"

    REDIS_URL: str = "redis://localhost:6379"

    REDIS_CACHE_TTL: int = 3600
    MAX_ACTIVE_JOBS_PER_USER: int = 3

    WORKER_CONCURRENCY: int = 4

    class Config:
        env_file = ".env"   


settings = Settings()