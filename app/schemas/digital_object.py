from datetime import datetime

from pydantic import BaseModel


class DigitalObjectRead(BaseModel):
    id: int
    pids: list[dict]
    object_type: str
    title: str | None
    titles: dict | None
    creators: list | None
    keywords: list | None
    topics: list | None
    access: dict | None
    methods: list | None
    source_local_id: str | None
    external_ids: list
    citation_count: int | None
    fwci: float | None
    origin: str
    created_at: datetime
    last_checked: datetime

    model_config = {"from_attributes": True}
