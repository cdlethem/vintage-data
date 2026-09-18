import copy
import json
import unittest
from airflow.providers.vintage.bot_dashboard.context_budget import compact_manager_context

class ContextBudgetTest(unittest.TestCase):
    def test_bounds_large_evidence_without_changing_freshness(self):
        context = dict(freshness_ok=False, missing_agents=['missing'], stale_agents=[], failed_agents=['failed'],
                       specialists=[dict(bot='failed', freshness='failed', latest_run={'failure_detail':'failure'*10000}, latest_useful_payload={'payload':{'summary':'summary'*2000,'plans':[{'detail':'x'*10000}]*30}})],
                       backlog=[dict(id=str(i),title='Ticket',resource_keys=['source:test'],planned_resolution='文'*10000) for i in range(200)],
                       bot_health=[{'failure_detail':'文'*10000}]*50)
        original = copy.deepcopy(context)
        result = compact_manager_context(context)
        self.assertLessEqual(len(json.dumps({'MANAGER_CONTEXT':result},sort_keys=True,separators=(',',':')).encode()),90*1024)
        self.assertEqual(context,original)
        self.assertEqual(result,compact_manager_context(context))
        self.assertFalse(result['freshness_ok'])
        self.assertEqual(['missing'],result['missing_agents'])
        self.assertEqual(200,len(result['backlog'])+result['context_summary']['backlog_omitted_count'])
        self.assertEqual(['source:test'],result['backlog'][0]['resource_keys'])
    def test_normal_queue_keeps_every_ticket_and_resource(self):
        result=compact_manager_context({'specialists':[], 'bot_health':[], 'backlog':[{'id':str(i),'resource_keys':[f'source:{i}'],'planned_resolution':'x'*3000} for i in range(44)]})
        self.assertEqual(44,len(result['backlog']))
        self.assertEqual(0,result['context_summary']['backlog_omitted_count'])

    def test_summary_and_maintenance_counts_satisfy_api_response_contracts(self):
        from airflow.providers.vintage.bot_dashboard.api_models import ManagerContextResponse, MaintenanceResponse
        context = dict(context_schema_version=1,context_kind='manager',generated_at='2026-09-17T00:00:00Z',as_of='2026-09-17T00:00:00Z',specialists=[],stale_agents=[],missing_agents=[],failed_agents=[],freshness_ok=True,bot_health=[],backlog=[])
        from airflow.providers.vintage.bot_dashboard.workload import evaluate
        context['workload'] = evaluate(open_count=0, created_24h=0, completed_24h=0,
                                       completed_7d=0, new_sources_24h=0)
        result = compact_manager_context(context)
        self.assertEqual(result,ManagerContextResponse.model_validate(result).model_dump())
        maintenance = dict(failed_dispatches=4,expired_leases=0,pruned_reports=0,pruned_artifacts=0,provider=dict(checked=0,changed=0,errors=0,follow_up_bots=[]),follow_up_bots=[])
        self.assertEqual(4,MaintenanceResponse.model_validate(maintenance).failed_dispatches)
