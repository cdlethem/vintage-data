from __future__ import annotations
import argparse
import os
import tempfile
import unittest
from unittest.mock import Mock, patch
from airflow.providers.vintage.bot_dashboard.git_setup import read_token, validate_identity, provision
from airflow.providers.vintage.bot_dashboard.git_provider import GitProviderError

class GitSetupTest(unittest.TestCase):
    def test_preview_never_writes_and_unencrypted_credentials_are_rejected(self):
        args = argparse.Namespace(provider='github', project='org/repo', api_base_url='https://api.github.com', clone_url='https://github.com/org/repo.git', base_branch='main', service_account_id='123', max_changed_files=40, max_diff_bytes=500000, allow_path=['src/**'], deny_path=[], connection_id='bot_dashboard_git', apply=False)
        candidate = Mock(is_encrypted=True)
        with patch('airflow.providers.vintage.bot_dashboard.git_setup.read_token', return_value='test-credential'), patch('airflow.providers.vintage.bot_dashboard.git_setup.Connection', return_value=candidate), patch('airflow.providers.vintage.bot_dashboard.git_setup.repository_config_from_connection', return_value=args), patch('airflow.providers.vintage.bot_dashboard.git_setup.validate_identity') as validate, patch('airflow.providers.vintage.bot_dashboard.git_setup.create_session') as session:
            self.assertEqual('validated', provision(args)['status'])
            validate.assert_called_once_with(args)
            session.assert_not_called()
            candidate.is_encrypted = False
            with self.assertRaises(GitProviderError): provision(args)
            session.assert_not_called()

    def test_private_file_and_named_environment_credentials(self):
        with tempfile.NamedTemporaryFile(mode='w') as file:
            file.write('test-credential\n'); file.flush()
            args = argparse.Namespace(token_env=None, token_file=file.name)
            self.assertEqual('test-credential', read_token(args))
            os.chmod(file.name, 0o644)
            with self.assertRaises(GitProviderError): read_token(args)
        with patch.dict(os.environ, {'GIT_SETUP_TEST_TOKEN': 'test-credential'}):
            self.assertEqual('test-credential', read_token(argparse.Namespace(token_env='GIT_SETUP_TEST_TOKEN')))

    def test_identity_and_push_permission_are_both_required(self):
        config = argparse.Namespace(provider='github', project='org/repo', service_account_id='123')
        for identity, push, valid in [('123', True, True), ('456', True, False), ('123', False, False)]:
            provider = Mock()
            provider._request.side_effect = [{'id': identity}, {'permissions': {'push': push}}]
            with self.subTest(identity=identity, push=push), patch('airflow.providers.vintage.bot_dashboard.git_setup.get_provider', return_value=provider):
                if valid: validate_identity(config)
                else:
                    with self.assertRaises(GitProviderError): validate_identity(config)

if __name__ == '__main__': unittest.main()
