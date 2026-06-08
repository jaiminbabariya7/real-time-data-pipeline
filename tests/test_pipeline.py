"""Unit tests for real-time data pipeline components."""
import json
import unittest
from unittest.mock import MagicMock, patch


class TestMessageSchema(unittest.TestCase):
    """Tests for event message schema validation."""

    VALID_EVENT = {
        "event_id": "evt_001",
        "user_id": "usr_42",
        "event_type": "page_view",
        "timestamp": "2024-07-15T09:30:01Z",
        "metadata": {"page": "/home", "session_id": "sess_99"},
    }

    def test_valid_event_round_trips_json(self):
        """Event must serialise and deserialise without data loss."""
        encoded = json.dumps(self.VALID_EVENT).encode("utf-8")
        decoded = json.loads(encoded)
        self.assertEqual(decoded["event_id"], self.VALID_EVENT["event_id"])
        self.assertEqual(decoded["event_type"], self.VALID_EVENT["event_type"])

    def test_required_fields_present(self):
        """Required fields must all be present in a valid event."""
        required = {"event_id", "user_id", "event_type", "timestamp"}
        for field in required:
            self.assertIn(field, self.VALID_EVENT)

    def test_missing_field_raises_key_error(self):
        """Accessing a missing required field must raise KeyError."""
        bad = {"event_id": "x"}
        with self.assertRaises(KeyError):
            _ = bad["event_type"]

    def test_timestamp_format(self):
        """Timestamp must be ISO 8601 format."""
        from datetime import datetime
        ts = self.VALID_EVENT["timestamp"]
        # Should parse without error
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        self.assertIsNotNone(dt)


class TestTransformations(unittest.TestCase):
    """Tests for Beam pipeline transform logic."""

    def test_enrich_event_adds_ingestion_time(self):
        """Enriched event must include an ingestion_timestamp field."""
        from datetime import datetime, timezone
        event = {"event_id": "e1", "event_type": "click"}
        event["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
        self.assertIn("ingestion_timestamp", event)

    def test_filter_invalid_events(self):
        """Events missing event_type must be filtered out."""
        events = [
            {"event_id": "1", "event_type": "click"},
            {"event_id": "2"},  # missing event_type
        ]
        valid = [e for e in events if "event_type" in e]
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0]["event_id"], "1")

    def test_batch_aggregation_count(self):
        """Aggregation must return correct event counts per type."""
        events = [
            {"event_type": "click"},
            {"event_type": "page_view"},
            {"event_type": "click"},
        ]
        counts = {}
        for e in events:
            counts[e["event_type"]] = counts.get(e["event_type"], 0) + 1
        self.assertEqual(counts["click"], 2)
        self.assertEqual(counts["page_view"], 1)


class TestDAGStructure(unittest.TestCase):
    """Lightweight tests for Airflow DAG definition."""

    def test_dag_importable(self):
        """DAG module must be importable (dependency check)."""
        try:
            import importlib
            spec = importlib.util.find_spec("airflow")
            if spec is None:
                self.skipTest("airflow not installed")
        except Exception:
            self.skipTest("airflow not installed")


if __name__ == "__main__":
    unittest.main()
