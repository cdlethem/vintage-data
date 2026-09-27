"""The admitted-run adapter must satisfy the API's actual strict envelope schema."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from bots import admitted_runner, bot_runner, usage
from airflow.providers.vintage.bot_dashboard.report_schemas import validate_run_envelope
from bots.provider_dashboard import ControlPlaneError


class AdmittedEnvelopeTests(unittest.TestCase):
    def recovery_fixture(self):
        patch_bytes = b'diff --git a/src/file.py b/src/file.py\n'
        payload = {'schema_version': 2, 'agent': 'task_executor', 'task_id': 'a'*36, 'status': 'ok',
                   'summary': 'Implemented and verified.', 'changed_paths': ['src/file.py'],
                   'verification': [{'name': 'verification_1', 'argv': ['python3','check.py'], 'exit_code': 0,
                                     'observed': 'passed', 'observation_sha256': hashlib.sha256(b'passed').hexdigest()}], 'blockers': []}
        report = json.dumps(payload).encode()
        admission = {'task_id': 'a'*36, 'execution_id': 'b'*64, 'revision': 2, 'base_sha': 'c'*40,
                     'patch_sha256': hashlib.sha256(patch_bytes).hexdigest(),
                     'executor_report_sha256': hashlib.sha256(report).hexdigest(), 'model_role': '@task',
                     'pr_number': 1, 'pr_url': 'https://example.test/pull/1', 'trusted_head_sha': 'd'*40,
                     'task': {'allowed_path_globs': ['src/**'], 'verification_commands': [['python3','check.py']]},
                     'repository_policy': {'allowed_path_globs': ['src/**'], 'denied_path_globs': [],
                                           'max_changed_files': 10, 'max_diff_bytes': 700_000}}
        manifest = {field: admission[field] for field in ['task_id','execution_id','revision','base_sha','patch_sha256']}
        manifest.update(manifest_version=1, changed_paths=payload['changed_paths'], patch_bytes=len(patch_bytes),
                        source_report_reference=None, checks=payload['verification'])
        admission['verification_manifest'] = manifest
        client = Mock(spec=['get_artifact','submit_run','finalize_execution','publish_execution'])
        client.get_artifact.side_effect = [report,patch_bytes]
        client.submit_run.return_value = {'status': 'stored'}
        now = datetime.now(timezone.utc)
        args = [admission, client, {'name': 'task_executor', 'output': {'schema': 'task_executor_v2'}},
                bot_runner, SimpleNamespace(ControlPlaneError=lambda code,retry: ValueError(code)),
                {'dag_id': 'bot__task_executor', 'run_id': 'original-run', 'task_id': 'run', 'map_index': -1, 'try_number': 2},
                now, now+timedelta(minutes=30), {'sha256': 'f'*64, 'byte_count': 100, 'build_ms': 1}]
        return args, client

    def test_published_recovery_delivers_existing_evidence_without_model_or_git(self):
        args, client = self.recovery_fixture()
        with patch.object(admitted_runner.subprocess, 'run') as process:
            result = admitted_runner._resume_published(*args)
        process.assert_not_called()
        envelope = client.submit_run.call_args.args[0]
        validate_run_envelope(envelope)
        self.assertEqual(envelope['attempts'], [])
        self.assertEqual(result.reason_code, 'published_report_recovered')
        self.assertEqual(client.finalize_execution.call_args.args[3]['publication']['pr_number'], 1)

    def test_pending_publication_resumes_checkpoint_without_model(self):
        args, client = self.recovery_fixture()
        for key in ('pr_number', 'pr_url', 'trusted_head_sha'):
            args[0][key] = None
        client.publish_execution.return_value = {'status':'published', 'pr_number':2, 'pr_url':'https://example.test/pull/2', 'trusted_head_sha':'d'*40}
        with patch.object(admitted_runner.subprocess, 'run') as process:
            admitted_runner._resume_published(*args)
        process.assert_not_called()
        self.assertEqual(args[0]['patch_sha256'], client.publish_execution.call_args.args[2]['patch_sha256'])
        self.assertEqual(2, client.finalize_execution.call_args.args[3]['publication']['pr_number'])

    def test_recovery_rejects_tampered_report_and_cross_execution_manifest(self):
        args, client = self.recovery_fixture()
        args[0]['executor_report_sha256'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'digest_invalid'):
            admitted_runner._resume_published(*args)
        client.submit_run.assert_not_called()
        args, client = self.recovery_fixture()
        args[0]['verification_manifest']['execution_id'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'manifest_invalid'):
            admitted_runner._resume_published(*args)
        client.finalize_execution.assert_not_called()

    def test_executor_and_reviewer_envelopes_satisfy_api_schema(self):
        now = datetime.now(timezone.utc)
        for agent, payload in [
            ('task_executor', {'schema_version': 2, 'agent': 'task_executor', 'task_id': 'a'*36,
                               'status': 'no_change', 'summary': 'Already implemented.',
                               'changed_paths': [], 'verification': [], 'blockers': []}),
            ('pr_reviewer', {'schema_version': 2, 'agent': 'pr_reviewer', 'task_id': 'a'*36,
                             'status': 'ok', 'verdict': 'approved', 'summary': 'Reviewed exact diff.',
                             'comments': [], 'verification': []}),
        ]:
            with self.subTest(agent=agent):
                attempt = admitted_runner._sandbox_attempt('provider/model', now, 123)
                envelope = bot_runner._envelope(
                    cfg={'name': agent, 'output': {'schema': agent+'_v2'}},
                    identity={'dag_id': 'bot__'+agent, 'run_id': 'admitted_run', 'task_id': 'run', 'map_index': -1, 'try_number': 1},
                    started_at=now, deadline_at=now+timedelta(minutes=30), outcome='succeeded', retry_class='none',
                    reason_code='sandbox_succeeded', failure=None, selected_model='provider/model', attempts=[attempt],
                    context_digest={'sha256': 'b'*64, 'byte_count': 100, 'build_ms': 1}, payload=payload,
                )
                normalized = validate_run_envelope(envelope)
                self.assertEqual(normalized['attempts'][0]['outcome'], 'succeeded')
                self.assertFalse(normalized['attempts'][0]['fallback_used'])
                self.assertIsNone(normalized['usage_total']['cost_micro_usd'])

    def test_attempt_counters_come_from_normalized_usage(self):
        measured = usage.normalize({'prompt_tokens': 123, 'completion_tokens': 45, 'total_tokens': 168}, format='openai')
        with patch.object(admitted_runner.usage_tools, 'normalize', return_value=measured):
            attempt = admitted_runner._sandbox_attempt('provider/model', datetime.now(timezone.utc), 123)
        self.assertEqual((attempt['input_tokens'], attempt['output_tokens'], attempt['total_tokens']), (123,45,168))




class SandboxLaunchDiagnosticsTests(unittest.TestCase):
    def test_nonzero_launcher_stderr_tail_is_bounded_and_redacted(self):
        secret = "gateway-credential-never-persist"
        child = (
            "import sys; "
            f"sys.stderr.write('x' * {admitted_runner._MAX_SANDBOX_STDERR * 2}); "
            f"sys.stderr.write('\\nRuntimeError: confined process exited 1; authorization: Bearer {secret}\\n'); "
            "raise SystemExit(1)"
        )
        process = admitted_runner._run_sandbox(
            [sys.executable, "-c", child],
            timeout=5,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        )
        error = admitted_runner._sandbox_exit_error(
            ControlPlaneError, process.returncode, process.stdout, process.stderr
        )

        self.assertEqual(1, process.returncode)
        self.assertEqual(b"", process.stdout)
        self.assertLessEqual(len(process.stderr), admitted_runner._MAX_SANDBOX_STDERR)
        self.assertTrue(process.stderr.startswith(admitted_runner._OUTPUT_TRUNCATED))
        self.assertEqual("sandbox_exit_1", error.code)
        self.assertIn("RuntimeError: confined process exited 1", str(error))
        self.assertNotIn(secret, str(error))
if __name__ == '__main__':
    unittest.main()
