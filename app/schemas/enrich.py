from pydantic import BaseModel


class EnrichResponse(BaseModel):
    """Wrapper returned by /enrich — the data field contains SKG-IF JSON-LD."""
    pid: str
    cached: bool
    data: dict
