from __future__ import annotations
from datetime import timedelta
import unittest
import uuid
from unittest.mock import patch
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, Revision, Task, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.service import Conflict, PreconditionFailed, assign_task, create_manual_task, start_task

class ExecutionRetryTest(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:');metadata.create_all(self.engine)
    def tearDown(self): self.engine.dispose()
    def fixture(self,session):
        created=create_manual_task(session,title='Retry fixture',category='other',priority=1,planned_resolution='Fix one file',actor_id='user',actor_name='User')
        assigned=assign_task(session,created['id'],version=1,actor_id='user',actor_name='User',kind='bot',profile='senior')
        revision=session.scalar(select(Revision).where(Revision.task_id==uuid.UUID(created['id'])))
        revision.verification_commands=[['python3','-m','unittest']];revision.allowed_path_globs=['src/**'];revision.suggested_executor='senior'
        start_task(session,created['id'],version=assigned['version'],actor_id='user',idempotency_key='first')
        task=session.get(Task,uuid.UUID(created['id']));row=session.scalar(select(Execution).where(Execution.task_id==task.id))
        row.terminal_at=utcnow();row.dispatch_state='terminal';row.terminal_reason_code='launcher_failed';task.state='blocked';task.blocked_from_state='in_progress'
        session.commit();return task,row
    def test_retry_creates_new_sequence_preserves_failure_and_replays(self):
        with Session(self.engine, autoflush=False) as session:
            task,prior=self.fixture(session)
            version=task.version
            result=start_task(session,str(task.id),version=version,actor_id='user',idempotency_key='retry')
            session.commit()
            self.assertEqual('accepted',task.state);self.assertIsNone(task.blocked_from_state)
            self.assertEqual(2,result['admission']['sequence']);self.assertEqual(version+1,task.version)
            self.assertEqual('launcher_failed',prior.terminal_reason_code);self.assertIsNotNone(prior.terminal_at)
            replay=start_task(session,str(task.id),version=version,actor_id='user',idempotency_key='retry')
            self.assertEqual(result['admission'],replay['admission'])
            events=session.scalars(select(Event).where(Event.event_type=='execution_retry_requested')).all()
            self.assertEqual(1,len(events));self.assertEqual(('blocked','accepted'),(events[0].from_state,events[0].to_state))
            self.assertEqual(2,len(session.scalars(select(Execution)).all()))
            sequences=session.scalars(select(Event.sequence).where(Event.task_id==task.id).order_by(Event.sequence)).all()
            self.assertEqual(list(range(1,len(sequences)+1)),sequences)
    def test_restored_in_progress_terminal_execution_can_be_retried_explicitly(self):
        from airflow.providers.vintage.bot_dashboard.autopilot import _actions
        from airflow.providers.vintage.bot_dashboard.service import get_task
        with Session(self.engine) as session:
            task,prior=self.fixture(session)
            task.state='in_progress';task.blocked_from_state=None
            session.commit()
            self.assertIn('start',_actions(get_task(session,str(task.id))))
            result=start_task(session,str(task.id),version=task.version,actor_id='executive',idempotency_key='explicit-retry')
            self.assertEqual(2,result['admission']['sequence'])
            self.assertIsNotNone(prior.terminal_at)
            session.commit()
            self.assertEqual([], _actions(get_task(session,str(task.id))))
            event=session.scalar(select(Event).where(Event.event_type=='execution_retry_requested'))
            self.assertEqual('in_progress',event.from_state)

    def test_rate_limited_review_preserves_pr_evidence_and_replays_safely(self):
        from airflow.providers.vintage.bot_dashboard.model_recovery import retry_model
        with Session(self.engine) as session:
            task,row=self.fixture(session)
            row.admission_kind='pr_reviewer';row.pr_number=17;row.pr_url='https://example.test/pr/17'
            row.trusted_head_sha='a'*40;row.source_artifact_sha256='b'*64
            row.terminal_reason_code='model_rate_limited';task.blocked_from_state='in_review'
            session.commit();version=task.version;prior_run=row.target_run_id
            with patch('airflow.providers.vintage.bot_dashboard.model_recovery.model_for_role') as model:
                result=retry_model(session,str(task.id),version=version,actor_id='user',idempotency_key='retry-model')
                session.commit()
                self.assertEqual('queued',result['status']);self.assertEqual('in_review',task.state)
                self.assertEqual(17,row.pr_number);self.assertEqual('a'*40,row.trusted_head_sha)
                self.assertEqual('b'*64,row.source_artifact_sha256);self.assertTrue(row.reviewer_required)
                self.assertNotEqual(prior_run,row.target_run_id);self.assertIsNone(row.terminal_at)
                self.assertEqual('pending',row.dispatch_state)
                model.assert_called_once_with(session,'pr_reviewer')
                self.assertEqual('already_requested',retry_model(session,str(task.id),version=version,actor_id='user',idempotency_key='retry-model')['status'])
                with self.assertRaises(PreconditionFailed):
                    retry_model(session,str(task.id),version=task.version,actor_id='user',idempotency_key='another-retry')
            self.assertEqual(1,len(session.scalars(select(Execution)).all()))

    def test_launch_retry_requires_same_open_head_and_no_existing_verdict(self):
        from types import SimpleNamespace
        from airflow.providers.vintage.bot_dashboard.model_recovery import retry_review_launch
        with Session(self.engine) as session:
            task,row=self.fixture(session)
            task.state='in_review';row.admission_kind='pr_reviewer';row.pr_number=17
            row.pr_url='https://example.test/pr/17';row.trusted_head_sha='a'*40
            row.source_artifact_sha256='b'*64;row.terminal_reason_code='sandbox_exit_1'
            row.provider='github';row.repository='owner/repo';row.branch='candidate';row.target_branch='main';row.service_account_id='bot'
            session.commit();version=task.version
            observed={'provider':'github','number':17,'head_sha':'c'*40,'head_ref':'candidate','base_ref':'main','author_id':'bot','state':'open'}
            with patch('airflow.providers.vintage.bot_dashboard.model_recovery.model_for_role'), patch('airflow.providers.vintage.bot_dashboard.git_provider.load_repository_config', return_value=SimpleNamespace(provider='github',project='owner/repo')), patch('airflow.providers.vintage.bot_dashboard.git_provider.get_provider') as provider:
                provider.return_value.read_change.return_value=observed
                with self.assertRaises(PreconditionFailed):
                    retry_review_launch(session,str(task.id),version=version,actor_id='operator',idempotency_key='repair')
                self.assertIsNotNone(row.terminal_at)
                observed['head_sha']='a'*40
                row.review_verdict='changes_requested'
                with self.assertRaises(PreconditionFailed):
                    retry_review_launch(session,str(task.id),version=version,actor_id='operator',idempotency_key='repair')
                row.review_verdict=None
                result=retry_review_launch(session,str(task.id),version=version,actor_id='operator',idempotency_key='repair')
                self.assertEqual('queued',result['status']);self.assertEqual('in_review',task.state)
                self.assertEqual('a'*40,row.trusted_head_sha);self.assertEqual('b'*64,row.source_artifact_sha256)
                self.assertTrue(row.reviewer_required);self.assertIsNone(row.review_verdict)
                self.assertEqual('already_requested',retry_review_launch(session,str(task.id),version=version,actor_id='operator',idempotency_key='repair')['status'])

    def test_revision_dispatch_switches_reviewer_back_to_executor(self):
        with Session(self.engine) as session:
            task,row=self.fixture(session)
            task.state='in_review';row.admission_kind='pr_reviewer';row.terminal_at=None
            row.dispatch_state='running';row.stage='reviewed';row.review_verdict='changes_requested'
            from airflow.providers.vintage.bot_dashboard.service import patch_task
            patch_task(session,str(task.id),version=task.version,actor_id='user',changes={'planned_resolution':'Address reviewer findings'})
            session.commit()
            result=start_task(session,str(task.id),version=task.version,actor_id='user',idempotency_key='revision',revision=True)
            self.assertEqual('executor',row.admission_kind)
            self.assertTrue(row.target_run_id.startswith('task__'))
            self.assertEqual('pending',row.dispatch_state)

    def test_prerequisite_publication_active_scope_and_capacity_guards_remain(self):
        for case in ['no_execution','pr','reviewer','active','no_change','scope','capacity']:
            with self.subTest(case=case),Session(self.engine) as session:
                task,prior=self.fixture(session)
                if case=='no_execution':session.delete(prior)
                elif case=='pr':prior.pr_number=10
                elif case=='reviewer':prior.admission_kind='pr_reviewer'
                elif case=='active':prior.terminal_at=None
                elif case=='no_change':prior.terminal_reason_code='no_change'
                elif case=='scope':session.scalar(select(Revision).where(Revision.task_id==task.id)).allowed_path_globs=[]
                session.flush()
                with self.assertRaises((PreconditionFailed,Conflict)):
                    start_task(session,str(task.id),version=task.version,actor_id='user',idempotency_key='rejected',max_queued=0 if case=='capacity' else 20)
                self.assertEqual('blocked',task.state)
                session.rollback()
                # Each subtest gets an independent provider database.
                metadata.drop_all(self.engine);metadata.create_all(self.engine)

if __name__=='__main__':unittest.main()
