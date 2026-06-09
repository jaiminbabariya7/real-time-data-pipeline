"""
Custom Apache Beam DoFns for the streaming ETL pipeline.

All business logic lives here — keeping pipeline.py clean and
these transforms independently testable.
"""
from __future__ import annotations
import json, logging
from datetime import datetime, timezone
from typing import Iterator
import apache_beam as beam
from kafka.schemas import UserEvent, EventType

logger = logging.getLogger(__name__)

VALID_TAG   = "valid"
INVALID_TAG = "invalid"


class ParseEvent(beam.DoFn):
    """Decode raw Pub/Sub bytes into a UserEvent.

    Outputs:
        valid   — successfully parsed UserEvent dict
        invalid — DLQ record for malformed messages
    """
    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam,
                *args, **kwargs) -> Iterator:
        raw_str = element.decode("utf-8", errors="replace")
        try:
            event = UserEvent.from_json(element)
            event.validate()
            record = {
                "event_id":     event.event_id,
                "event_type":   event.event_type,
                "user_id":      event.user_id,
                "session_id":   event.session_id,
                "event_ts":     event.timestamp,
                "properties":   json.dumps(event.properties),
                "ip_address":   event.ip_address,
                "country":      event.country,
                "platform":     event.platform,
                "is_valid":     True,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            yield beam.pvalue.TaggedOutput(VALID_TAG, record)
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            yield beam.pvalue.TaggedOutput(INVALID_TAG, {
                "raw_message":  raw_str[:4096],
                "error_reason": str(e),
                "topic":        "raw-events",
                "failed_at":    datetime.now(timezone.utc).isoformat(),
            })


class EnrichEvent(beam.DoFn):
    """Enrich a validated event with computed fields."""

    _PURCHASE_TIER = [
        (0,    50,   "micro"),
        (50,   200,  "small"),
        (200,  1000, "medium"),
        (1000, float("inf"), "large"),
    ]

    def process(self, record: dict, *args, **kwargs) -> Iterator[dict]:
        # Parse properties to add typed fields
        props = {}
        try:
            props = json.loads(record.get("properties") or "{}")
        except json.JSONDecodeError:
            pass

        # Add purchase tier for purchase events
        if record["event_type"] == EventType.PURCHASE.value:
            total = float(props.get("total_usd", 0))
            tier  = next((t for lo, hi, t in self._PURCHASE_TIER
                          if lo <= total < hi), "large")
            props["purchase_tier"] = tier

        # Add session start flag (first event is assumed login)
        props["is_session_start"] = record["event_type"] == EventType.LOGIN.value

        record["properties"] = json.dumps(props)
        yield record


class AddWindowTimestamps(beam.DoFn):
    """Stamp each record with its window's start and end times."""

    def process(self, record: dict,
                window=beam.DoFn.WindowParam,
                *args, **kwargs) -> Iterator[dict]:
        from apache_beam.utils.timestamp import Timestamp
        start = datetime.fromtimestamp(float(window.start), tz=timezone.utc)
        end   = datetime.fromtimestamp(float(window.end),   tz=timezone.utc)
        record["window_start"] = start.isoformat()
        record["window_end"]   = end.isoformat()
        yield record


class FormatGCSRecord(beam.DoFn):
    """Convert a record to newline-delimited JSON for GCS archive."""

    def process(self, record: dict, *args, **kwargs) -> Iterator[str]:
        yield json.dumps(record, default=str)
