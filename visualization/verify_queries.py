"""Execute every saved chart through the same async query API as the UI."""
import concurrent.futures
import time

from visualization.api import Client


def verify(
    url: str,
    token: str,
    project: str,
    *,
    chart_slugs: set[str] | None = None,
) -> dict:
    client = Client(url, token)
    charts = client.request('GET', f'/api/v1/projects/{project}/charts')
    if chart_slugs is not None:
        charts = [chart for chart in charts if chart.get("slug") in chart_slugs]
        missing = sorted(chart_slugs - {chart.get("slug") for chart in charts})
    else:
        missing = []

    def check(chart):
        api = Client(url, token)
        base = f'/api/v2/projects/{project}/query'
        try:
            started = api.request('POST', base + '/chart', {'chartUuid': chart['uuid'], 'invalidateCache': True})
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                result = api.request('GET', base + '/' + started['queryUuid'])
                if result['status'] == 'ready':
                    return None
                if result['status'] in ('error', 'expired', 'cancelled'):
                    raise RuntimeError('query status: ' + result['status'])
                time.sleep(0.25)
            raise TimeoutError('query exceeded 120 seconds')
        except Exception as exc:
            return {'chart': chart['name'], 'uuid': chart['uuid'], 'error': str(exc)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        failures = [item for item in pool.map(check, charts) if item]
    return {
        'queried': len(charts),
        'failures': failures,
        'missing': missing,
        'ok': bool(charts) and not missing and not failures,
    }
