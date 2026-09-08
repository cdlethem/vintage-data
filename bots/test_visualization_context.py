import copy
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from bots import agent_context
from visualization.test_project import model_fixture


class AnalyticsVisualizationTest(unittest.TestCase):
    def context(self, *, modeled=True, issues=None, tasks=None, sync=None):
        manifest=model_fixture()
        if not modeled:
            manifest['nodes'].pop('model.vintage_data.fct_demo')
        report={'errors':[],'mart_count':int(modeled),'covered_count':int(modeled and not issues),'models':
                [{'unique_id':'model.vintage_data.fct_demo','name':'fct_demo','family':'demo','sources':['demo'],'charts':[],'issues':issues or []}] if modeled else []}
        client=mock.Mock();client.manager_context.return_value={'backlog':tasks or []}
        with mock.patch.object(agent_context,'_control_client',return_value=client):
            return agent_context.analytics_context(sync_result=sync or {'ok':True,'sources':[{'name':'demo','columns':[]}]},manifest=manifest,visualization_report=report)

    def test_base_alone_is_not_modeled(self):
        result=self.context(modeled=False)
        self.assertEqual(result['modeled_count'],0)
        self.assertEqual(result['selected']['work_kind'],'model')

    def test_missing_chart_reopens_modeled_source(self):
        result=self.context(issues=['missing dashboard visualization'])
        self.assertEqual(result['modeled_count'],1)
        self.assertEqual(result['selected']['work_kind'],'visualization')
        self.assertEqual(result['backlog_count'],1)

    def test_active_task_suppresses_but_completed_or_prior_report_does_not(self):
        task={'task_id':'one','state':'in_progress','resource_keys':['visualization:demo']}
        self.assertEqual(self.context(issues=['missing metrics'],tasks=[task])['backlog_count'],0)
        task['state']='completed'
        self.assertEqual(self.context(issues=['missing metrics'],tasks=[task])['backlog_count'],1)

    def test_drift_is_work_not_probe_failure(self):
        result=self.context(sync={'ok':False,'sources':[{'name':'demo'}],'schema_drift':[{'source':'demo'}]})
        self.assertEqual(result['selected']['work_kind'],'repair')

    def test_full_coverage_is_a_deterministic_skip(self):
        self.assertEqual(self.context()['backlog_count'],0)


class DataAnalystContextTest(unittest.TestCase):
    def context(self, *, gaps=None, issues=None, tasks=None):
        manifest=model_fixture()
        report={'errors':[],'mart_count':1,'covered_count':int(not issues),'trend_covered_count':0,
                'trend_chart_count':3,'gap_count':len(gaps or []),'models':
                [{'unique_id':'model.vintage_data.fct_demo','name':'fct_demo','family':'demo','sources':['demo'],
                  'charts':[],'trend_count':3,'issues':issues or [],'gaps':gaps or []}]}
        client=mock.Mock();client.manager_context.return_value={'backlog':tasks or []}
        with mock.patch.object(agent_context,'_control_client',return_value=client):
            return agent_context.analyst_context(manifest=manifest,visualization_report=report)

    def test_zero_gaps_is_a_deterministic_skip(self):
        result=self.context()
        self.assertEqual(result['gap_backlog_count'],0)
        self.assertIsNone(result['selected'])

    def test_gap_selects_the_family_and_names_where_to_probe(self):
        result=self.context(gaps=['no time series: add config.meta.vintage.visualization.analysis.trends'])
        selected=result['selected']
        self.assertEqual(selected['work_kind'],'analysis')
        self.assertEqual(selected['allowed_path_globs'],['transform/models/marts/demo/_demo_models.yml'])
        model=selected['models'][0]
        self.assertEqual(model['collection_time'],'observed_at')
        # The extractor timestamp is never offered as an event-time candidate.
        self.assertEqual([entry['column'] for entry in model['event_time_candidates']],['happened_at'])
        self.assertIn('MONTH',model['event_time_candidates'][0]['intervals'])
        self.assertEqual(model['category_dimensions'],['id','category'])
        self.assertEqual(sorted(model['metrics']),['entities','reported'])

    def test_contract_issues_are_ordered_behind_pure_analysis_work(self):
        analysis_only=self.context(gaps=['no dimensional trend: add a kind breakdown or composition time series'])
        self.assertEqual(analysis_only['selected']['name'],'demo')
        broken=self.context(issues=['missing metrics'])
        # Still surfaced as work, but the analytics engineer owns the breach.
        self.assertEqual(broken['gap_backlog_count'],1)

    def test_active_task_suppresses_the_family(self):
        task={'task_id':'one','state':'in_progress','resource_keys':['analysis:demo']}
        result=self.context(gaps=['no time series'],tasks=[task])
        self.assertEqual(result['gap_backlog_count'],0)
        self.assertEqual(result['suppressed_family_count'],1)
