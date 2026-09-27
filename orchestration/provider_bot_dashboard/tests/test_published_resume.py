from __future__ import annotations
from datetime import timedelta
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import Mock,patch
from sqlalchemy import create_engine,select
from sqlalchemy.orm import Session
from airflow.providers.vintage.bot_dashboard.execution import claim_run
from airflow.providers.vintage.bot_dashboard.models import Execution,Revision,Task,metadata,utcnow
from airflow.providers.vintage.bot_dashboard.service import create_manual_task,patch_task,PreconditionFailed

class PublishedResumeTest(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');metadata.create_all(self.engine)
    def tearDown(self):self.engine.dispose()
    def test_published_retry_validates_immutable_provider_identity_before_mutation(self):
        from contextlib import ExitStack
        with Session(self.engine) as session:
            created=create_manual_task(session,title='Published',category='other',priority=1,planned_resolution='Done',actor_id='u',actor_name='User')
            task=session.get(Task,uuid.UUID(created['id']));task.state='in_progress'
            revision=session.scalar(select(Revision).where(Revision.task_id==task.id));revision.allowed_path_globs=['src/**'];revision.verification_commands=[['python3','-m','unittest']]
            row=Execution(execution_id='a'*64,task_id=task.id,sequence=1,revision=1,idempotency_key='once',target_run_id='original-run',profile='senior',reviewer_required=True,provider='github',repository='org/repo',branch='bot-dashboard/task/1',target_branch='main',pr_number=1,pr_url='https://example/pr/1',trusted_head_sha='b'*40,service_account_id='123',base_sha='c'*40,dispatch_state='pending')
            row.patch_sha256='e'*64
            row.executor_report_sha256='f'*64
            row.verification_manifest={'manifest_version':1,'task_id':str(task.id),'execution_id':row.execution_id,'revision':1,'base_sha':row.base_sha,'changed_paths':['src/example.py'],'patch_sha256':row.patch_sha256,'patch_bytes':100,'checks':[{'name':'tests','argv':['python3','-m','unittest'],'exit_code':0,'observed':'passed','observation_sha256':'284d1e8c4918248233df17642bbb940c001e1fa856c18aab86ba6dbe7813eb13'}]}
            session.add(row);session.commit()
            patch_task(session,str(task.id),version=task.version,actor_id='u',changes={'title':'Later title','planned_resolution':'Later plan','allowed_path_globs':['different/**'],'verification_commands':[['python3','-m','compileall']]})
            session.commit()
            valid={'provider':'github','number':1,'head_ref':row.branch,'base_ref':'main','author_id':'123','head_sha':'b'*40,'state':'open'}
            config=SimpleNamespace(provider='github',project='org/repo',allowed_path_globs=['src/**'],denied_path_globs=[],max_changed_files=5,max_diff_bytes=1000)
            provider=Mock()
            with ExitStack() as stack:
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.execution.require_executor_preconditions'))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.git_provider.load_repository_config',return_value=config))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.git_provider.get_provider',return_value=provider))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.repository_ops.create_source_artifact',return_value={'sha256':'d'*64,'byte_count':1}))
                stack.enter_context(patch('airflow.providers.vintage.bot_dashboard.model_settings.model_for_role',return_value={'provider_id':'fixture','model':'small','base_url':'https://model.example/v1'}))
                for key in valid:
                    provider.read_change.return_value={**valid,key:'changed'}
                    with self.subTest(key=key),self.assertRaisesRegex(PreconditionFailed,'changed before resume'):
                        claim_run(session,dag_id='bot__task_executor',run_id='original-run',conf_value={'execution_id':row.execution_id,'revision':1},kind='executor',deadline_at=utcnow()+timedelta(minutes=5))
                    self.assertIsNone(row.claimed_run_id)
                    self.assertEqual('pending',row.dispatch_state)
                    self.assertEqual('b'*40,row.trusted_head_sha)
                provider.read_change.return_value=valid
                admission=claim_run(session,dag_id='bot__task_executor',run_id='original-run',conf_value={'execution_id':row.execution_id,'revision':1},kind='executor',deadline_at=utcnow()+timedelta(minutes=5))
                self.assertEqual(1,admission['pr_number'])
                self.assertEqual('b'*40,admission['trusted_head_sha'])
                self.assertEqual('original-run',row.claimed_run_id)
                self.assertEqual('Later plan',task.planned_resolution)
                self.assertEqual('Published',admission['task']['title'])
                self.assertEqual('Done',admission['task']['planned_resolution'])
                self.assertEqual(['src/**'],admission['task']['allowed_path_globs'])
                self.assertEqual([['python3','-m','unittest']],admission['task']['verification_commands'])

if __name__=='__main__':unittest.main()
