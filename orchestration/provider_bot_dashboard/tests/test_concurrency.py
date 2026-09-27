import json
import unittest
from unittest.mock import patch
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from airflow.models.variable import Variable
from airflow.models.pool import Pool
from airflow.providers.vintage.bot_dashboard import concurrency as c
from airflow.providers.vintage.bot_dashboard.models import Policy, metadata
from airflow.providers.vintage.bot_dashboard.service import Conflict

class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite:///:memory:')
        metadata.create_all(self.engine)
        Variable.__table__.create(self.engine)
        Pool.__table__.create(self.engine)
        self.session = Session(self.engine)
        self.session.add(Policy(category='*', mode='manual'))
        self.session.commit()
    def tearDown(self):
        self.session.close(); self.engine.dispose()
    def test_persists_independent_limits_and_rejects_stale_updates(self):
        limits = dict.fromkeys(c.BOTS, 1) | {'task_executor': 4, 'pr_reviewer': 2}
        with patch.object(c, 'get_settings', side_effect=lambda s: c._read(s)[1]):
            result = c.set_settings(self.session, c.Settings(version=0, limits=limits), 'operator')
            self.session.commit()
            self.assertEqual(1, result['version'])
            pools = {p.pool: p.slots for p in self.session.scalars(select(Pool))}
            self.assertEqual(4, pools[c.worker_pool('task_executor')])
            self.assertEqual(2, pools[c.worker_pool('pr_reviewer')])
            with self.assertRaises(Conflict):
                c.set_settings(self.session, c.Settings(version=0, limits=limits), 'stale')
            c.set_settings(self.session, c.Settings(version=1, limits=limits | {'task_executor': 1}), 'operator')
            self.assertEqual(1, self.session.scalar(select(Pool).where(Pool.pool == c.worker_pool('task_executor'))).slots)
            audit = json.loads(self.session.scalar(select(Variable)).val)
            self.assertEqual('operator', audit['history'][-1]['actor_id'])
    def test_dag_parse_uses_supported_sdk_variable_access(self):
        limits = dict.fromkeys(c.BOTS, 1) | {'task_executor': 4}
        with patch('airflow.sdk.Variable.get', return_value={'limits': limits}) as variable:
            self.assertEqual(limits, c.scheduler_limits())
            self.assertEqual(c.KEY, variable.call_args.args[0])
            self.assertTrue(variable.call_args.kwargs['deserialize_json'])

    def test_limits_are_bounded_and_executive_configurable(self):
        limits = dict.fromkeys(c.BOTS, 1)
        for changed in ({'task_executor': 0}, {'task_executor': 17}, {'task_executor': True}, {'executive': 17}, {'unknown': 1}):
            with self.subTest(changed=changed), self.assertRaises(ValidationError):
                c.Settings(version=0, limits=limits | changed)

    def test_worker_capacity_is_shared_and_executive_is_independent(self):
        from airflow.providers.vintage.bot_dashboard.service import DomainError
        limits = dict.fromkeys(c.BOTS, 1) | {'task_executor': 14, 'pr_reviewer': 2, 'executive': 4}
        with patch.object(c, 'get_settings', side_effect=lambda s: c._read(s)[1]):
            result = c.set_settings(self.session, c.Settings(version=0, limits=limits), 'operator')
            self.assertEqual(limits, result['limits'])
            with self.assertRaises(DomainError):
                c.set_settings(self.session, c.Settings(version=1, limits=limits | {'pr_reviewer': 3}), 'operator')
