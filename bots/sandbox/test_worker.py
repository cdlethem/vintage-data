import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

from bots.sandbox import worker


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work = self.root / 'work'
        self.work.mkdir()
        self.sessions = self.root / 'sessions'
        self.sessions.mkdir()
        self.session = self.sessions / '2026-09-13T07-23-08_session-id.jsonl'
        self.write_session([{'type': 'text', 'text': 'Implemented fix.'}])
        self.admission = {'protocol_version': 2, 'kind': 'executor', 'model_role': '@task',
                          'task_id': 'a' * 36,
                          'deadline_at': (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
                          'task': {'verification_commands': [[sys.executable, '-c', 'print("passed")']]}}

    def write_session(self, final_blocks):
        rows = [
            {'type': 'session', 'version': 3, 'id': 'session-id'},
            {'type': 'message', 'id': 'first', 'message': {'role': 'assistant', 'content': [
                {'type': 'text', 'text': 'Working...'}, {'type': 'toolCall', 'name': 'read', 'arguments': {}}]}},
            {'type': 'message', 'id': 'tool', 'message': {'role': 'toolResult', 'content': [
                {'type': 'text', 'text': '{"untrusted":"tool JSON is not the answer"}'}]}},
            {'type': 'message', 'id': 'final', 'message': {'role': 'assistant', 'content': final_blocks}},
            {'type': 'custom', 'customType': 'session_stats', 'data': {}},
        ]
        self.session.write_text('\n'.join(json.dumps(row) for row in rows)+'\n')

    def test_session_uses_only_final_assistant_text(self):
        self.write_session([{'type': 'thinking', 'thinking': 'private reasoning'},
                            {'type': 'text', 'text': 'Final answer.'}])
        self.assertEqual(worker.session_final_text(self.sessions), 'Final answer.')
        self.write_session([{'type': 'text', 'text': 'Not finished'}, {'type': 'toolCall', 'name': 'read'}])
        with self.assertRaisesRegex(ValueError, 'final_answer_missing'):
            worker.session_final_text(self.sessions)

    def test_session_rejects_symlinks_and_oversized_files(self):
        self.session.unlink()
        self.session.symlink_to(self.root/'victim')
        with self.assertRaisesRegex(ValueError, 'unsafe'):
            worker.session_final_text(self.sessions)
        self.session.unlink()
        with self.session.open('wb') as handle:
            handle.truncate(worker.MAX_SESSION_BYTES+1)
        with self.assertRaisesRegex(ValueError, 'unsafe'):
            worker.session_final_text(self.sessions)

    def test_reviewer_accepts_session_json_despite_cli_progress(self):
        self.admission['kind'] = 'pr_reviewer'
        review = {'schema_version': 2, 'agent': 'pr_reviewer', 'task_id': self.admission['task_id'],
                  'status': 'ok', 'verdict': 'approved', 'summary': 'Reviewed actual diff.',
                  'comments': [], 'verification': ['Read exact patch.'],
                  'failure_kind': None, 'repair': None}
        self.write_session([{'type': 'thinking', 'thinking': 'ignore'}, {'type': 'text', 'text': json.dumps(review)}])
        with patch.object(worker, 'bounded_run', return_value=(0, 'Working...\n'+json.dumps(review))):
            result = worker.execute(self.admission, self.work, self.root)
        self.assertEqual(result['report'], review)
        self.assertEqual(worker.validate_review('```json\n'+json.dumps(review)+'\n```', self.admission['task_id']), review)
        with self.assertRaises(ValueError):
            worker.validate_review('Working...\n'+json.dumps(review), self.admission['task_id'])

    def test_missing_session_is_bounded_failure_not_stdout_approval(self):
        self.session.unlink()
        self.admission['kind'] = 'pr_reviewer'
        with patch.object(worker, 'bounded_run', return_value=(0, '{"verdict":"approved"}')):
            report = worker.execute(self.admission, self.work, self.root)['report']
        self.assertEqual(report['verdict'], 'unable_to_review')
        self.assertIn('model_session_count_invalid', report['summary'])

    def test_snapshot_tracks_added_deleted_and_mode_changes(self):
        path = self.work / 'old'
        path.write_text('old')
        before = worker.snapshot(self.work)
        path.chmod(0o700)
        self.assertNotEqual(before, worker.snapshot(self.work))
        path.unlink()
        (self.work / 'new').write_text('new')
        after = worker.snapshot(self.work)
        self.assertEqual(set(before) ^ set(after), {'old', 'new'})

    def test_snapshot_rejects_symlinks_including_directories(self):
        (self.work / 'link').symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'unsafe'):
            worker.snapshot(self.work)

    def test_bounded_output_digest_and_timeout(self):
        code, observed = worker.bounded_run([sys.executable, '-c', 'print("x"*100000)'], self.work, 5, 100)
        self.assertEqual(code, 0)
        self.assertLessEqual(len(observed), 100)
        self.assertIn('truncated', observed)
        code, observed = worker.bounded_run([sys.executable, '-c', 'import time; time.sleep(5)'], self.work, .05, 100)
        self.assertEqual(code, -1)
        self.assertIn('timed out', observed)

    def test_noninteractive_child_gets_eof_with_open_parent_stdin(self):
        # Airflow's task process has an open control socket as stdin. Simulate an
        # equally non-EOF pipe, and ensure the model/check never inherits it.
        saved_stdin = os.dup(0)
        read_fd, write_fd = os.pipe()
        try:
            os.dup2(read_fd, 0)
            code, observed = worker.bounded_run(
                [sys.executable, '-c', 'import sys; print(repr(sys.stdin.read()))'],
                self.work, 1, 100,
            )
        finally:
            os.dup2(saved_stdin, 0)
            os.close(saved_stdin)
            os.close(read_fd)
            os.close(write_fd)
        self.assertEqual(code, 0)
        self.assertEqual(observed, "''\n")

    def test_checks_record_failures_and_exact_digest(self):
        commands = [[sys.executable, '-c', 'print("é"); raise SystemExit(3)']]
        checks = worker.verification(commands, self.work, datetime.now(timezone.utc).timestamp() + 20)
        self.assertEqual(checks[0]['exit_code'], 3)
        self.assertEqual(checks[0]['argv'], commands[0])
        self.assertEqual(checks[0]['observation_sha256'], hashlib.sha256(checks[0]['observed'].encode()).hexdigest())

    def test_capability_preflight_blocks_before_model(self):
        self.admission['task']['verification_commands'] = [["definitely-not-a-real-command"]]
        with patch.object(worker, 'bounded_run') as run:
            report = worker.execute(self.admission, self.work, self.root)['report']
        run.assert_not_called()
        self.assertEqual('blocked', report['status'])
        self.assertIn('unavailable', report['blockers'][0])

    def test_executor_runs_checks_independently_after_model_and_observes_changes(self):
        def model(argv, cwd, timeout, limit):
            self.assertIn('--no-extensions', argv)
            self.assertIn('read,grep,glob,bash,edit,write', argv)
            (cwd / 'new.py').write_text('print(1)')
            return 0, 'Working...\nCLI progress is not the final summary.'
        real_run = worker.bounded_run
        def dispatch(argv, *args):
            return model(argv, *args) if argv[0] == 'omp' else real_run(argv, *args)
        with patch.object(worker, 'bounded_run', side_effect=dispatch) as calls:
            report = worker.execute(self.admission, self.work, self.root)['report']
        self.assertEqual(calls.call_count, 2)
        self.assertEqual(report['status'], 'ok')
        self.assertEqual(report['summary'], 'Implemented fix.')
        self.assertEqual(report['changed_paths'], ['new.py'])
        self.assertEqual(report['verification'][0]['observed'], 'passed\n')

    def test_model_or_verification_failure_blocks_publication(self):
        with patch.object(worker, 'bounded_run', side_effect=[(1, 'failed'), (0, 'passed')]):
            report = worker.execute(self.admission, self.work, self.root)['report']
        self.assertEqual(report['status'], 'blocked')
        with patch.object(worker, 'bounded_run', side_effect=[
                (0, 'done'), (1, 'first failure'), (0, 'repaired'), (0, 'passed')
        ]), patch.object(worker, 'session_final_text', side_effect=['Implemented.', 'Repaired.']):
            repaired = worker.execute(self.admission, self.work, self.root)['report']
        self.assertEqual(repaired['status'], 'no_change')
        self.assertEqual([1, 0], [attempt[0]['exit_code'] for attempt in repaired['verification_attempts']])

    def test_failed_check_gets_only_one_bounded_repair_pass(self):
        with patch.object(worker, 'bounded_run', side_effect=[
                (0, 'done'), (1, 'first failure'), (0, 'repair'), (2, 'still failing')
        ]), patch.object(worker, 'session_final_text', side_effect=['Implemented.', 'Tried repair.']) as final:
            report = worker.execute(self.admission, self.work, self.root)['report']
        self.assertEqual('blocked', report['status'])
        self.assertEqual(2, len(report['verification_attempts']))
        self.assertEqual(2, final.call_count)

    def test_reviewer_invalid_output_never_approves(self):
        self.admission['kind'] = 'pr_reviewer'
        with patch.object(worker, 'bounded_run', return_value=(0, 'looks good')) as call:
            report = worker.execute(self.admission, self.work, self.root)['report']
        self.assertEqual(report['verdict'], 'unable_to_review')
        self.assertIn('read,grep,glob,bash', call.call_args.args[0])
        self.assertEqual(call.call_count, 1)

    def test_review_identity_and_shape_checked(self):
        report = {'schema_version': 2, 'agent': 'pr_reviewer', 'task_id': 'a'*36,
                  'status': 'ok', 'verdict': 'approved', 'summary': 'Looks sound.',
                  'comments': [], 'verification': ['Read the change.'],
                  'failure_kind': None, 'repair': None}
        self.assertEqual(worker.validate_review(json.dumps(report), 'a'*36), report)
        with self.assertRaisesRegex(ValueError, 'identity'):
            worker.validate_review(json.dumps(report), 'b'*36)

    def test_reviewer_can_request_a_bounded_repair(self):
        report = {
            'schema_version': 2, 'agent': 'pr_reviewer', 'task_id': 'a' * 36,
            'status': 'ok', 'verdict': 'changes_requested', 'summary': 'One local defect.',
            'comments': [{'body': 'Handle null input.', 'path': 'src/job.py', 'line': 8,
                          'severity': 'blocking'}],
            'verification': ['Read the exact patch.'], 'failure_kind': None,
            'repair': {'instructions': 'Handle null input before parsing.', 'paths': ['src/job.py'],
                       'check_expectations': ['The admitted unit test passes.']},
        }
        self.assertEqual(report, worker.validate_review(json.dumps(report), 'a' * 36))

    def test_result_is_private_and_cannot_overwrite_symlink(self):
        path = self.root / 'result.json'
        worker.write_result(path, {'protocol_version': 2})
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        path.unlink()
        path.symlink_to(self.root / 'victim')
        with self.assertRaises(FileExistsError):
            worker.write_result(path, {})
        self.assertFalse((self.root / 'victim').exists())


if __name__ == '__main__':
    unittest.main()
