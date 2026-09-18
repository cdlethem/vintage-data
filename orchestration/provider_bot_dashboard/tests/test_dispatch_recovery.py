from __future__ import annotations
from datetime import timedelta
import unittest
import uuid
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session
from airflow.providers.vintage.bot_dashboard.maintenance import recover_failed_dispatches
from airflow.providers.vintage.bot_dashboard.execution import expire_leases
from airflow.providers.vintage.bot_dashboard.models import Event, Execution, RunReport, Task, metadata, utcnow
from airflow.providers.vintage.bot_dashboard.service import create_manual_task

class DispatchRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.engine=create_engine('sqlite:///:memory:')
        metadata.create_all(self.engine)
        with self.engine.begin() as connection:
            connection.execute(text('CREATE TABLE dag_run (dag_id TEXT, run_id TEXT, state TEXT)'))
            connection.execute(text('CREATE TABLE task_instance (dag_id TEXT, run_id TEXT, task_id TEXT, state TEXT)'))

    def tearDown(self): self.engine.dispose()

    def fixture(self, session, dag_state='failed', task_state='failed', claimed=False, kind='executor', wrong_dag=False, reported=False):
        identifier=uuid.uuid4().hex
        task=create_manual_task(session,title='Fixture',category='other',priority=1,planned_resolution='Change fixture',actor_id='operator',actor_name='Operator')
        row=Execution(execution_id=identifier*2,task_id=uuid.UUID(task['id']),sequence=1,revision=1,idempotency_key=identifier,target_run_id=identifier,profile='senior',reviewer_required=True,admission_kind=kind,dispatch_state='leased',lease_expires_at=utcnow()-timedelta(minutes=1),claimed_run_id=identifier if claimed else None)
        session.add(row)
        dag='bot__pr_reviewer' if kind=='pr_reviewer' else 'bot__task_executor'
        if wrong_dag: dag='bot__unrelated'
        if dag_state is not None:
            session.execute(text('INSERT INTO dag_run VALUES (:dag,:run,:state)'),{'dag':dag,'run':identifier,'state':dag_state})
            session.execute(text('INSERT INTO task_instance VALUES (:dag,:run,:task,:state)'),{'dag':dag,'run':identifier,'task':'run','state':task_state})
        if reported:
            now=utcnow()
            session.add(RunReport(dag_id=dag,run_id=identifier,task_id='run',bot_name='task_executor',status='failed',outcome='failed',retry_class='terminal',reason_code='fixture',started_at=now,finished_at=now,deadline_at=now,duration_ms=0,context_sha256='a'*64,context_byte_count=0,context_build_ms=0,sha256='b'*64,byte_count=0,expires_at=now+timedelta(days=1)))
        session.commit()
        return row, session.get(Task,uuid.UUID(task['id']))

    def test_failed_unclaimed_run_blocks_once_before_lease_recycling(self):
        with Session(self.engine) as session:
            row,task=self.fixture(session)
            self.assertEqual(1,recover_failed_dispatches(session))
            session.commit()
            self.assertEqual('blocked',task.state)
            self.assertEqual('accepted',task.blocked_from_state)
            self.assertEqual('terminal',row.dispatch_state)
            self.assertEqual('report_missing',row.terminal_reason_code)
            self.assertEqual('DispatchFailure',row.terminal_failure_class)
            self.assertIn(row.target_run_id,row.terminal_detail)
            self.assertEqual(0,expire_leases(session))
            self.assertEqual(0,recover_failed_dispatches(session))
            self.assertEqual(1,len(session.scalars(select(Event).where(Event.event_type=='execution_dispatch_failed')).all()))

    def test_live_retry_claimed_reported_and_unrelated_runs_are_preserved(self):
        cases=[{'dag_state':None},{'dag_state':'queued'},{'dag_state':'running'},{'dag_state':'success'},{'task_state':'up_for_retry'},{'task_state':'running'},{'task_state':'queued'},{'task_state':'scheduled'},{'task_state':'deferred'},{'task_state':None},{'wrong_dag':True}]
        with Session(self.engine) as session:
            rows=[self.fixture(session,**case) for case in cases]
            self.assertEqual(0,recover_failed_dispatches(session))
            for row,task in rows:
                self.assertIsNone(row.terminal_at)
                self.assertEqual('accepted',task.state)

    def test_exhausted_claimed_run_is_recovered_once(self):
        with Session(self.engine) as session:
            row,task=self.fixture(session,claimed=True,reported=True)
            row.dispatch_state='running';task.state='in_progress'
            session.commit()
            self.assertEqual(1,recover_failed_dispatches(session))
            self.assertEqual('blocked',task.state)
            self.assertEqual('in_progress',task.blocked_from_state)
            self.assertEqual(0,recover_failed_dispatches(session))

    def test_exhausted_rate_limit_preserves_reported_reason(self):
        with Session(self.engine) as session:
            row,task=self.fixture(session,claimed=True,reported=True)
            report=session.scalar(select(RunReport))
            report.reason_code='model_rate_limited'; report.failure_detail='Choose another model in Models & connections.'
            report.failure_class='ModelRateLimited'; session.commit()
            self.assertEqual(1,recover_failed_dispatches(session))
            self.assertEqual('model_rate_limited', row.terminal_reason_code)
            self.assertIn("Change the affected bot", row.terminal_detail)
            self.assertEqual('blocked', task.state)

    def test_pending_recycled_reviewer_is_also_recovered_with_bounded_limit(self):
        with Session(self.engine) as session:
            first,_=self.fixture(session,kind='pr_reviewer')
            first.dispatch_state='pending'
            self.fixture(session)
            self.assertEqual(1,recover_failed_dispatches(session,limit=1))
            self.assertEqual('terminal',first.dispatch_state)
            self.assertEqual(1,recover_failed_dispatches(session,limit=1))

if __name__=='__main__':unittest.main()
