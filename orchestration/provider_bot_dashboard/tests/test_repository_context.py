import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
from airflow.providers.vintage.bot_dashboard.repository_context import repository_inventory
from airflow.providers.vintage.bot_dashboard.git_provider import GitProviderError

class RepositoryContextTest(unittest.TestCase):
    def provider(self, kind='github'):
        return Mock(config=SimpleNamespace(provider=kind,project='org/repo',base_branch='release/next',
            allowed_path_globs=('extract/tests/**',), denied_path_globs=('**/secrets/**',),
            max_changed_files=20, max_diff_bytes=100000, token='private-not-for-context'), project_path='org%2Frepo')
    def test_github_inventory_pins_tree_and_does_not_use_local_checkout(self):
        provider=self.provider()
        provider._request.side_effect=[{'sha':'a'*40,'commit':{'tree':{'sha':'b'*40}}},
            {'tree':[{'path':'extract/new.py','type':'blob'},{'path':'extract','type':'tree'}],'truncated':False}]
        with patch('airflow.providers.vintage.bot_dashboard.repository_context.get_provider',return_value=provider):
            result=repository_inventory()
        self.assertEqual(['extract/new.py'], result['files'])
        self.assertEqual('a'*40,result['head_sha']);self.assertFalse(result['truncated'])
        self.assertEqual('GET',provider._request.call_args_list[0].args[0])
        self.assertIn('commits/release%2Fnext',provider._request.call_args_list[0].args[1])
        self.assertIn('trees/'+'b'*40,provider._request.call_args_list[1].args[1])
        self.assertEqual({'allowed_path_globs':['extract/tests/**'],
                          'denied_path_globs':['**/secrets/**'],
                          'max_changed_files':20,'max_diff_bytes':100000},result['repository_policy'])
        self.assertNotIn('private-not-for-context',str(result))
    def test_github_truncation_is_explicit(self):
        provider=self.provider()
        provider._request.side_effect=[{'sha':'a'*40,'commit':{'tree':{'sha':'b'*40}}},
            {'tree':[{'path':'x'*50001,'type':'blob'}],'truncated':False}]
        with patch('airflow.providers.vintage.bot_dashboard.repository_context.get_provider',return_value=provider):
            result=repository_inventory()
        self.assertTrue(result['truncated']);self.assertEqual([],result['files'])
    def test_gitlab_inventory_uses_commit_for_tree_pages(self):
        provider=self.provider('gitlab')
        provider._request.side_effect=[{'id':'c'*40},[{'type':'blob','path':'new/file.py'}]]
        with patch('airflow.providers.vintage.bot_dashboard.repository_context.get_provider',return_value=provider):
            result=repository_inventory()
        self.assertEqual(['new/file.py'],result['files'])
        self.assertIn('ref='+'c'*40,provider._request.call_args_list[1].args[1])
    def test_invalid_commit_fails_without_fallback_to_stale_files(self):
        provider=self.provider()
        provider._request.return_value={'sha':'bad','commit':{'tree':{'sha':'b'*40}}}
        with patch('airflow.providers.vintage.bot_dashboard.repository_context.get_provider',return_value=provider), self.assertRaises(GitProviderError):
            repository_inventory()
        self.assertEqual(1,provider._request.call_count)
