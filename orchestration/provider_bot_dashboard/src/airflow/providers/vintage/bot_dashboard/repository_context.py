"""Read-only, commit-pinned repository inventory for executive decisions."""
import re
from urllib.parse import quote
from .git_provider import get_provider, GitProviderError


def repository_inventory():
    provider = get_provider()
    config = provider.config
    branch = quote(config.base_branch, safe='')
    entries = []
    truncated = False
    if config.provider == 'github':
        commit = provider._request('GET', f'repos/{config.project}/commits/{branch}')
        head = commit['sha']
        tree = commit['commit']['tree']['sha']
        if not re.fullmatch(r'[a-f0-9]{40,64}', head) or not re.fullmatch(r'[a-f0-9]{40,64}', tree):
            raise GitProviderError('Repository inventory has an invalid commit identity')
        result = provider._request('GET', f'repos/{config.project}/git/trees/{tree}?recursive=1')
        entries = [item['path'] for item in result['tree'] if item.get('type') == 'blob']
        truncated = bool(result.get('truncated'))
    else:
        prefix = f'projects/{provider.project_path}/repository'
        commit = provider._request('GET', f'{prefix}/commits/{branch}')
        head = commit['id']
        if not re.fullmatch(r'[a-f0-9]{40,64}', head):
            raise GitProviderError('Repository inventory has an invalid commit identity')
        # A bounded prefix is explicitly incomplete, never evidence of absence.
        for page in range(1, 11):
            result = provider._request('GET', f'{prefix}/tree?ref={head}&recursive=true&per_page=100&page={page}')
            entries.extend(item['path'] for item in result if item.get('type') == 'blob')
            if len(result) < 100:
                break
        else:
            truncated = True
    files, size = [], 0
    for path in sorted(set(entries)):
        size += len(path.encode('utf-8')) + 1
        if size > 50_000:
            truncated = True
            break
        files.append(path)
    return {'repository': config.project, 'branch': config.base_branch, 'head_sha': head,
            'files': files, 'truncated': truncated, 'source': 'configured_remote_repository',
            'repository_policy': {
                'allowed_path_globs': list(config.allowed_path_globs),
                'denied_path_globs': list(config.denied_path_globs),
                'max_changed_files': config.max_changed_files,
                'max_diff_bytes': config.max_diff_bytes,
            }}
