"""
Pydantic models untuk validasi skema event dan response API.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
import uuid


class PublishRequest(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255, description="Nama topic/channel")
    event_id: str = Field(..., min_length=1, max_length=255, description="ID unik event (UUID disarankan)")
    timestamp: str = Field(..., description="Waktu event dalam format ISO8601")
    source: str = Field(..., min_length=1, max_length=255, description="Sumber event")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Data payload event")

    model_config = {
        "json_schema_extra": {
            "example": {
                "topic": "sensor-data",
                "event_id": "evt-550e8400-e29b-41d4-a716",
                "timestamp": "2026-06-16T10:00:00Z",
                "source": "iot-device-01",
                "payload": {"temperature": 30, "humidity": 75},
            }
        }
    }


class BatchPublishRequest(BaseModel):
    events: List[PublishRequest] = Field(..., min_length=1, max_length=500)


class PublishResponse(BaseModel):
    success: bool
    message: str
    event_id: str
    received_at: str


class BatchPublishResponse(BaseModel):
    success: bool
    total_received: int
    queued: int
    failed: int


class EventModel(BaseModel):
    topic: str
    event_id: str
    source: Optional[str] = None
    timestamp: Optional[str] = None
    payload: Dict[str, Any] = {}
    received_at: str
    processed_at: str


class EventsListResponse(BaseModel):
    success: bool
    topic: Optional[str] = None
    count: int
    events: List[EventModel]


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    topic_counts: Dict[str, int]
    uptime_seconds: float
    uptime_formatted: str
    workers_active: int
    queue_size: int


class HealthResponse(BaseModel):
    status: str
    database: str
    broker: str
    uptime_seconds: float
    version: str
    workers_active: int


class EventResponse(BaseModel):
    success: bool
    message: str
    event_id: str
    is_duplicate: bool
    received_at: str
