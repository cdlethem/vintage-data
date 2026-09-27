import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from bots.admitted_runner import _write_review_context
from bots.sandbox.worker import model_prompt


class ContextError(Exception):
    def __init__(self, code, retry_class):
        super().__init__(code)


class ReviewContextTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.patch = b'diff --git a/file.py b/file.py\n+fixed\n'
        self.report = json.dumps({'task_id': 'a'*36, 'summary': 'Fixed the defect.'}).encode()
        self.admission = {'task_id': 'a'*36, 'kind': 'pr_reviewer',
                          'patch_sha256': hashlib.sha256(self.patch).hexdigest(),
                          'executor_report_sha256': hashlib.sha256(self.report).hexdigest()}

    def test_verified_evidence_is_private_and_keeps_admission_unchanged(self):
        before = dict(self.admission)
        path = _write_review_context(self.root, self.admission, self.patch, self.report, ContextError)
        self.assertEqual(path.stat().st_mode & 0o777, 0o400)
        context = json.loads(path.read_text())
        self.assertEqual(context['patch_text'], self.patch.decode())
        self.assertEqual(context['executor_report']['summary'], 'Fixed the defect.')
        self.assertEqual(self.admission, before)
        self.assertIn('/review-context.json', model_prompt(self.admission))

    def test_rejects_changed_artifact_and_wrong_task_report(self):
        with self.assertRaisesRegex(ContextError, 'digest_invalid'):
            _write_review_context(self.root, self.admission, self.patch, self.report+b' ', ContextError)
        report = b'{"task_id":"wrong-task"}'
        self.admission['executor_report_sha256'] = hashlib.sha256(report).hexdigest()
        with self.assertRaisesRegex(ContextError, 'report_invalid'):
            _write_review_context(self.root, self.admission, self.patch, report, ContextError)
        self.assertFalse((self.root/'review-context.json').exists())

    def test_rejects_oversized_evidence_and_existing_symlink(self):
        with self.assertRaisesRegex(ContextError, 'outside_bounds'):
            _write_review_context(self.root, self.admission, b'x'*700_001, self.report, ContextError)
        (self.root/'review-context.json').symlink_to(self.root/'victim')
        with self.assertRaises(FileExistsError):
            _write_review_context(self.root, self.admission, self.patch, self.report, ContextError)
        self.assertFalse((self.root/'victim').exists())


if __name__ == '__main__':
    unittest.main()
