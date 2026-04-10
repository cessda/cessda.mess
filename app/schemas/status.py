from datetime import datetime

from pydantic import BaseModel


class StatusResponse(BaseModel):
    pid: str
    found: bool
    fresh: bool | None = None
    last_checked: datetime | None = None
    object_type: str | None = None
