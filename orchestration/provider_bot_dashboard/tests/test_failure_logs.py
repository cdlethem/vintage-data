from __future__ import annotations

import itertools
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from airflow.providers.vintage.bot_dashboard import api
from airflow.utils.log.file_task_handler import StructuredLogMessage


class FailureLogTest(unittest.TestCase):
    def read(self, chunks, metadata=None):
        reader = Mock()
        reader.read_log_chunks.return_value = (chunks, metadata or {"end_of_log": True})
        with patch.object(api, "TaskLogReader", return_value=reader):
            result = api._failure_log_tail(SimpleNamespace(try_number=1))
        return result, reader

    def test_airflow_lazy_stream_retains_error_summary_and_redacts_it(self):
        messages = itertools.chain(
            (StructuredLogMessage(event=f"earlier line {i}") for i in range(40)),
            [StructuredLogMessage(event="Task failed with exception", error_detail=[{
                "exc_type": "ValueError", "exc_value": "invalid credential fixture-secret",
                "frames": [{"locals": {"sensitive": "not-included"}}],
            }])],
        )
        with patch.object(api, "redact", side_effect=lambda text, *a, **kw: text.replace("fixture-secret", "***")) as redact:
            (detail, code), reader = self.read(messages)
        self.assertEqual("task_failed", code)
        self.assertIn("ValueError: invalid credential ***", detail)
        self.assertNotIn("earlier line 0", detail)
        self.assertNotIn("itertools", detail)
        self.assertNotIn("not-included", detail)
        self.assertEqual(30, len(detail.splitlines()))
        self.assertEqual(("ValueError", "value_error"), api._failure_classification(detail))
        reader.read_log_chunks.assert_called_once()
        redact.assert_called_once()

    def test_scalar_logs_and_output_length_remain_supported(self):
        for chunk in ["RuntimeError: failed", StructuredLogMessage(event="RuntimeError: failed")]:
            (detail, code), _ = self.read(chunk)
            self.assertEqual(("RuntimeError: failed", "task_failed"), (detail, code))
        (detail, _), _ = self.read("x" * 10000)
        self.assertEqual(3500, len(detail))

    def test_empty_or_broken_stream_is_unavailable(self):
        self.assertEqual(("", "log_unavailable"), self.read(iter(()))[0])
        def broken():
            raise OSError("do not leak reader internals")
            yield
        self.assertEqual(("", "log_unavailable"), self.read(broken())[0])


if __name__ == "__main__":
    unittest.main()
