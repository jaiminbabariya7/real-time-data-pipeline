"""Avro-compatible event schemas for the streaming ETL pipeline."""
from __future__ import annotations
import json, uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class EventType(str, Enum):
    PAGE_VIEW   = "page_view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE    = "purchase"
    SEARCH      = "search"
    LOGIN       = "login"
    LOGOUT      = "logout"


@dataclass
class UserEvent:
    """Canonical event produced by the web application."""
    event_id:   str
    event_type: str
    user_id:    str
    session_id: str
    timestamp:  str
    properties: dict
    ip_address: str = ""
    user_agent: str = ""
    country:    str = ""
    platform:   str = "web"

    @classmethod
    def create(cls, event_type: EventType, user_id: str,
               session_id: str, properties: dict) -> "UserEvent":
        return cls(
            event_id=str(uuid.uuid4()), event_type=event_type.value,
            user_id=user_id, session_id=session_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            properties=properties,
        )

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_json(cls, data: bytes) -> "UserEvent":
        return cls(**json.loads(data.decode("utf-8")))

    def validate(self) -> None:
        if not self.event_id:   raise ValueError("event_id required")
        if self.event_type not in {e.value for e in EventType}:
            raise ValueError(f"Unknown event_type: {self.event_type}")
        if not self.user_id:    raise ValueError("user_id required")
        if not self.timestamp:  raise ValueError("timestamp required")


KAFKA_TOPICS = {
    "raw_events":       "streaming-etl.raw-events",
    "processed_events": "streaming-etl.processed-events",
    "dead_letter":      "streaming-etl.dead-letter",
}

BQ_EVENTS_SCHEMA = {"fields": [
    {"name": "event_id",     "type": "STRING",    "mode": "REQUIRED"},
    {"name": "event_type",   "type": "STRING",    "mode": "REQUIRED"},
    {"name": "user_id",      "type": "STRING",    "mode": "REQUIRED"},
    {"name": "session_id",   "type": "STRING",    "mode": "REQUIRED"},
    {"name": "event_ts",     "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "properties",   "type": "STRING",    "mode": "NULLABLE"},
    {"name": "ip_address",   "type": "STRING",    "mode": "NULLABLE"},
    {"name": "country",      "type": "STRING",    "mode": "NULLABLE"},
    {"name": "platform",     "type": "STRING",    "mode": "NULLABLE"},
    {"name": "is_valid",     "type": "BOOLEAN",   "mode": "REQUIRED"},
    {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "window_end",   "type": "TIMESTAMP", "mode": "NULLABLE"},
]}

BQ_DLQ_SCHEMA = {"fields": [
    {"name": "raw_message",  "type": "STRING",    "mode": "REQUIRED"},
    {"name": "error_reason", "type": "STRING",    "mode": "REQUIRED"},
    {"name": "topic",        "type": "STRING",    "mode": "NULLABLE"},
    {"name": "failed_at",    "type": "TIMESTAMP", "mode": "REQUIRED"},
]}
