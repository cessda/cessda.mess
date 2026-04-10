from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str  # "ok" | "degraded"
    postgres: str  # "ok" | "error"
    sparql: str  # "ok" | "error"
