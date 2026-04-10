from pydantic import BaseModel


class StatsResponse(BaseModel):
    total_objects: int
    total_relationships: int
    fresh_objects: int
    stale_objects: int
    objects_by_type: dict[str, int]
    objects_by_origin: dict[str, int]
