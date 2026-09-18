"""A published execution can resume reporting only with complete matching evidence."""
import copy
import unittest
from pydantic import ValidationError
from airflow.providers.vintage.bot_dashboard.report_schemas import ExecutorAdmissionV2

class ResumeAdmissionTest(unittest.TestCase):
    def test_fresh_and_published_admissions_reject_partial_or_mismatched_evidence(self):
        value = dict(protocol_version=2, kind='executor',task_id='a'*36,profile='senior',model_role='@default',reviewer_required=True,sequence=1,revision=2,execution_id='b'*64,deadline_at='2026-09-13T08:00:00Z',task=dict(title='Task',category='other',planned_resolution='Implement task',verification_commands=[['python3','-V']],allowed_path_globs=['src/**'],resource_keys=[]),source_artifact=dict(sha256='c'*64,byte_count=100),base_sha='d'*40,repository_policy=dict(allowed_path_globs=['src/**'],denied_path_globs=[],max_changed_files=40,max_diff_bytes=500000))
        self.assertIsNone(ExecutorAdmissionV2.model_validate(value).pr_number)
        publication=dict(patch_sha256='e'*64,trusted_head_sha='f'*40,pr_number=1,pr_url='https://example.com/pull/1',executor_report_sha256='a'*64,verification_manifest=dict(manifest_version=1,task_id=value['task_id'],execution_id=value['execution_id'],revision=2,base_sha=value['base_sha'],changed_paths=['src/change.py'],patch_sha256='e'*64,patch_bytes=20,checks=[]))
        pending={**value, **{key: publication[key] for key in ("patch_sha256", "verification_manifest", "executor_report_sha256")}}
        self.assertIsNone(ExecutorAdmissionV2.model_validate(pending).pr_number)
        value.update(publication)
        self.assertEqual(1,ExecutorAdmissionV2.model_validate(value).pr_number)
        for name in publication:
            broken=copy.deepcopy(value);broken[name]=None
            with self.subTest(missing=name),self.assertRaises(ValidationError):ExecutorAdmissionV2.model_validate(broken)
        for name, replacement in [('task_id','z'*36),('execution_id','z'*64),('revision',3),('base_sha','a'*40),('patch_sha256','b'*64)]:
            broken=copy.deepcopy(value);broken['verification_manifest'][name]=replacement
            with self.subTest(mismatch=name),self.assertRaises(ValidationError):ExecutorAdmissionV2.model_validate(broken)
