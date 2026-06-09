"""Unit tests for Beam pipeline transforms."""
import json, unittest
from datetime import datetime, timezone
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__),'..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__),'..','kafka'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__),'..','dataflow'))

from kafka.schemas import UserEvent, EventType
from dataflow.transforms import ParseEvent, EnrichEvent, VALID_TAG, INVALID_TAG


def _make_event(**overrides) -> bytes:
    e = UserEvent.create(EventType.PAGE_VIEW, "usr_00001", "sess_123",
                         {"page": "/home", "load_time_ms": 200})
    for k, v in overrides.items():
        setattr(e, k, v)
    return e.to_json()


class TestParseEvent(unittest.TestCase):
    def setUp(self):
        self.fn = ParseEvent()

    def _run(self, data: bytes):
        valid, invalid = [], []
        for out in self.fn.process(data):
            if hasattr(out, 'tag'):
                (valid if out.tag == VALID_TAG else invalid).append(out.value)
            else:
                valid.append(out)
        return valid, invalid

    def test_valid_event_parsed(self):
        valid, invalid = self._run(_make_event())
        self.assertEqual(len(valid), 1)
        self.assertEqual(len(invalid), 0)

    def test_invalid_json_goes_to_dlq(self):
        valid, invalid = self._run(b"not-json{{{")
        self.assertEqual(len(valid), 0)
        self.assertGreater(len(invalid), 0)

    def test_missing_user_id_goes_to_dlq(self):
        valid, invalid = self._run(_make_event(user_id=""))
        self.assertEqual(len(valid), 0)
        self.assertGreater(len(invalid), 0)

    def test_unknown_event_type_goes_to_dlq(self):
        valid, invalid = self._run(_make_event(event_type="unknown_type"))
        self.assertEqual(len(valid), 0)
        self.assertGreater(len(invalid), 0)

    def test_valid_record_has_required_fields(self):
        valid, _ = self._run(_make_event())
        rec = valid[0]
        for field in ["event_id","event_type","user_id","session_id","is_valid","processed_at"]:
            self.assertIn(field, rec)
        self.assertTrue(rec["is_valid"])

    def test_dlq_record_has_error_reason(self):
        _, invalid = self._run(b"bad{")
        self.assertIn("error_reason", invalid[0])
        self.assertIn("failed_at",    invalid[0])


class TestEnrichEvent(unittest.TestCase):
    def setUp(self):
        self.fn = EnrichEvent()

    def _parse_first(self, data: bytes) -> dict:
        p = ParseEvent()
        for out in p.process(data):
            if hasattr(out,'tag') and out.tag == VALID_TAG:
                return out.value
        return {}

    def test_purchase_tier_added(self):
        event = UserEvent.create(EventType.PURCHASE, "usr_1", "sess_1",
            {"total_usd": 250.0, "order_id": "ord_1", "items": 2, "payment_method": "card"})
        valid_rec = self._parse_first(event.to_json())
        enriched  = list(self.fn.process(valid_rec))[0]
        props = json.loads(enriched["properties"])
        self.assertIn("purchase_tier", props)
        self.assertEqual(props["purchase_tier"], "medium")

    def test_login_is_session_start(self):
        event = UserEvent.create(EventType.LOGIN, "usr_1", "sess_1", {"method": "email"})
        valid_rec = self._parse_first(event.to_json())
        enriched  = list(self.fn.process(valid_rec))[0]
        props = json.loads(enriched["properties"])
        self.assertTrue(props["is_session_start"])

    def test_non_login_not_session_start(self):
        valid_rec = self._parse_first(_make_event())
        enriched  = list(self.fn.process(valid_rec))[0]
        props = json.loads(enriched["properties"])
        self.assertFalse(props["is_session_start"])


class TestSchemas(unittest.TestCase):
    def test_user_event_roundtrip(self):
        e = UserEvent.create(EventType.SEARCH,"u1","s1",{"query":"test"})
        e2 = UserEvent.from_json(e.to_json())
        self.assertEqual(e.event_id, e2.event_id)
        self.assertEqual(e.event_type, e2.event_type)

    def test_validate_raises_for_empty_user_id(self):
        e = UserEvent.create(EventType.LOGIN,"","s1",{})
        with self.assertRaises(ValueError):
            e.validate()

    def test_purchase_tier_boundaries(self):
        from dataflow.transforms import EnrichEvent
        fn = EnrichEvent()
        tiers = [(25, "micro"), (75, "small"), (500, "medium"), (2000, "large")]
        for amount, expected in tiers:
            rec = {"event_type":"purchase","properties": json.dumps({"total_usd":amount})}
            result = list(fn.process(rec))[0]
            props = json.loads(result["properties"])
            self.assertEqual(props["purchase_tier"], expected, f"amount={amount}")


if __name__ == "__main__":
    unittest.main()
