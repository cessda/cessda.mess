from datetime import datetime

from pydantic import BaseModel


class RelationshipRead(BaseModel):
    id: int
    source_id: int
    target_id: int
    relation_type: str
    provenance: str
    created_at: datetime

    model_config = {"from_attributes": True}
