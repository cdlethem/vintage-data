from __future__ import annotations
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).parent))
import bot_runner

class DashboardModelTest(unittest.TestCase):
    def setUp(self):
        self.cfg={'name':'manager','requires_capabilities':['repository_execute']}
        self.models={'default':'cli','models':{'cli':{'provider':'command','model':'@plan','argv':['omp','-p','--model','{model}','--tools','read,grep,glob,bash','--usage-file','{usage_file}','{prompt}'],'capabilities':['repository_execute'],'max_concurrency':1,'inherit_env':True,'env':{'OLD_KEY':'must-not-inherit'},'usage':{'source':'file','format':'openai','path':'{usage_file}'}}}}
        self.settings={'providers':[{'id':'remote','base_url':'https://provider.example/v1','api_key':'private-provider-key'}],'assignments':[{'role':'manager','provider_id':'remote','model':'small-model'}]}

    def test_managed_remote_models_use_scheduler_capacity_not_local_model_locks(self):
        with patch.object(bot_runner, '_acquire_one', side_effect=AssertionError('unexpected local lock')):
            with bot_runner._inference_slot({}, {'_dashboard_managed': True}):
                pass

    def test_mapping_preserves_tools_budgets_and_telemetry_but_not_secrets(self):
        closed=[]
        @contextmanager
        def gateway(provider,model,**kwargs):
            self.assertEqual('private-provider-key',provider['api_key'])
            self.assertEqual('small-model',model)
            yield Mock(base_url='http://127.0.0.1:12345/v1', telemetry={})
            closed.append(True)
        control=Mock(model_settings=Mock(return_value=self.settings))
        with patch('model_gateway.model_gateway',gateway), ExitStack() as stack:
            model=bot_runner.dashboard_model(self.cfg,self.models,control,stack,datetime.now(timezone.utc)+timedelta(seconds=60))
            home=Path(model['env']['HOME'])
            self.assertTrue(home.exists())
            self.assertEqual(self.models['models']['cli']['argv'],model['argv'])
            self.assertEqual(1,model['max_concurrency'])
            self.assertEqual(self.models['models']['cli']['usage'],model['usage'])
            self.assertEqual('gateway/small-model',model['model'])
            self.assertFalse(model['inherit_env'])
            self.assertNotIn('private-provider-key',json.dumps(model))
            self.assertNotIn('OLD_KEY',model['env'])
            config=(home/'.omp/agent/models.yml').read_text()
            self.assertIn('gateway-placeholder',config)
            self.assertNotIn('private-provider-key',config)
        self.assertFalse(home.exists())
        self.assertEqual([True],closed)

    def test_unmapped_falls_back_but_missing_configured_provider_fails(self):
        control=Mock(model_settings=Mock(return_value={'providers':[],'assignments':[]}))
        with ExitStack() as stack:
            self.assertIsNone(bot_runner.dashboard_model(self.cfg,self.models,control,stack,None))
            control.model_settings.return_value={'providers':[],'assignments':self.settings['assignments']}
            with self.assertRaisesRegex(bot_runner.BotError,'mapped model provider is missing'):
                bot_runner.dashboard_model(self.cfg,self.models,control,stack,None)

if __name__=='__main__':unittest.main()
