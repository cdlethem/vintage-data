import json
import pathlib
import tempfile
import time
import unittest

from bots import usage


class UsageTest(unittest.TestCase):
    def test_openai_details(self):
        value = usage.normalize({"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120, "prompt_tokens_details": {"cached_tokens": 30}, "completion_tokens_details": {"reasoning_tokens": 5}}, format="openai")
        self.assertEqual(value["input_tokens"], 70)
        self.assertEqual(value["cached_input_tokens"], 30)
        self.assertEqual(value["reasoning_tokens"], 5)
        self.assertEqual(value["total_tokens"], 120)

    def test_anthropic_and_normalized(self):
        value = usage.normalize({"input_tokens": 10, "output_tokens": 4, "cache_read_input_tokens": 3, "cache_creation_input_tokens": 2}, format="anthropic")
        self.assertEqual(value["total_tokens"], 19)
        normalized = usage.normalize(value, format="normalized")
        self.assertEqual(normalized, value)

    def test_omp_session_merge_and_cost(self):
        value = usage.normalize([
            {"model": "gpt-5.6-luna", "usage": {"input": 103792, "output": 12375, "cacheRead": 1069056, "cacheWrite": 0, "totalTokens": 1185223, "cost": {"total": 0.057}}},
            {"model": "gpt-5.6-luna", "usage": {"input": 10, "output": 2, "cacheRead": 3, "totalTokens": 15, "cost": {"total": 0.001}}},
        ], format="omp_session")
        self.assertEqual(value["requests"], 2)
        self.assertEqual(value["cached_input_tokens"], 1069059)
        self.assertEqual(value["cost_micro_usd"], 58000)
        self.assertEqual(value["cost_source"], "provider_reported")

    def test_malformed_and_merge_rules(self):
        self.assertEqual(usage.normalize(None, format="openai")["total_tokens"], 0)
        one = usage.normalize({"input_tokens": 2, "output_tokens": 3}, format="anthropic")
        two = usage.normalize({"input_tokens": 4, "output_tokens": 5}, format="anthropic")
        self.assertIsNone(usage.merge([one, two])["cost_micro_usd"])
        self.assertEqual(usage.merge([one, two])["total_tokens"], 14)

    def test_price_precedence_and_rounding(self):
        book = {"id": "2026-09-08", "models": {"m": {"input": 0.202, "output": 1.195, "cached_input": 0.0199, "cache_write": 0.25}}}
        base = usage.normalize({"input_tokens": 1, "output_tokens": 1}, format="anthropic")
        priced = usage.price(base, model="m", alias_model="alias", book=book)
        self.assertEqual(priced["cost_source"], "price_book")
        self.assertEqual(priced["cost_micro_usd"], 1)
        provider = dict(base, cost_source="provider_reported", cost_micro_usd=9)
        self.assertEqual(usage.price(provider, model="m", alias_model=None, book=book)["cost_micro_usd"], 9)
        self.assertIsNone(usage.price(base, model="unknown", alias_model=None, book=book)["cost_micro_usd"])

    def test_capture_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            file = root / "u.json"
            file.write_text(json.dumps({"input_tokens": 2, "output_tokens": 3}))
            spec = {"source": "file", "format": "anthropic", "path": "{usage_file}"}
            self.assertEqual(usage.capture(spec, started_at=time.time() - 1, stdout="", usage_file=file)["total_tokens"], 5)
            trailer = usage.capture({"source": "stdout_trailer", "format": "anthropic", "marker": "USAGE:"}, started_at=time.time(), stdout='text\nUSAGE:{"input_tokens": 4, "output_tokens": 1}', usage_file=None)
            self.assertEqual(trailer["total_tokens"], 5)
            session = root / "sessions"
            session.mkdir()
            (session / "x.jsonl").write_text(json.dumps({"usage": {"input": 2, "output": 1, "totalTokens": 3}}) + "\n")
            session_value = usage.capture({"source": "session_dir", "format": "omp_session", "dir": str(session)}, started_at=time.time() - 2, stdout="", usage_file=None)
            self.assertEqual(session_value["total_tokens"], 3)

    def test_private_session_dir_reads_nested_message_usage(self):
        """A per-run directory needs no time threshold and may nest sessions."""
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            nested = root / "sub"
            nested.mkdir()
            (root / "a.jsonl").write_text(
                json.dumps({"type": "model_usage", "usage": {"input": 0, "output": 0, "totalTokens": 0, "cost": {"total": 0}}}) + "\n"
                + json.dumps({"type": "message", "message": {"model": "m", "usage": {"input": 33696, "output": 523, "cacheRead": 0, "totalTokens": 34219, "reasoningTokens": 467, "cost": {"total": 0.145244}}}}) + "\n"
            )
            (nested / "b.jsonl").write_text(
                json.dumps({"type": "message", "message": {"usage": {"input": 2770, "output": 57, "cacheRead": 33536, "totalTokens": 36363, "cost": {"total": 0.025634}}}}) + "\n"
            )
            spec = {"source": "session_dir", "format": "omp_session", "dir": "{usage_dir}"}
            value = usage.capture(spec, started_at=0, stdout="", usage_file=None, usage_dir=str(root))
            self.assertEqual(2, value["requests"])  # the all-zero aborted record is ignored
            self.assertEqual(36466, value["input_tokens"])
            self.assertEqual(33536, value["cached_input_tokens"])
            self.assertEqual(467, value["reasoning_tokens"])
            self.assertEqual(70582, value["total_tokens"])
            self.assertEqual("provider_reported", value["cost_source"])
            self.assertEqual(170878, value["cost_micro_usd"])
            self.assertEqual(0, usage.capture(spec, started_at=0, stdout="", usage_file=None)["total_tokens"])


if __name__ == "__main__":
    unittest.main()
