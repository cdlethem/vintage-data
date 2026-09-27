"""Human model configuration and trusted runtime credential resolution."""
from __future__ import annotations

import ipaddress
import json
import os
from pathlib import Path
import socket
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from .service import DomainError, PreconditionFailed

PREFIX = 'bot_dashboard_model_'
ASSIGNMENTS = 'bot_dashboard_model_assignments'
ROLES = ['source_discovery', 'source_vetting', 'source_scheduling', 'cadence_review', 'failure_triage', 'analytics_engineer', 'data_analyst', 'manager', 'executive', 'pr_reviewer', 'executor_junior', 'executor_senior', 'executor_staff']

class ProviderBody(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str = Field(pattern=r'^[a-z][a-z0-9_-]{0,39}$')
    name: str = Field(min_length=1, max_length=100)
    base_url: str = Field(min_length=1, max_length=2000)
    api_key: str | None = Field(default=None, max_length=65536)

class AssignmentBody(BaseModel):
    model_config = ConfigDict(extra='forbid')
    role: str
    provider_id: str = Field(pattern=r'^[a-z][a-z0-9_-]{0,39}$')
    model: str = Field(min_length=1, max_length=200)

class SettingsBody(BaseModel):
    model_config = ConfigDict(extra='forbid')
    assignments: list[AssignmentBody] = Field(max_length=len(ROLES))


def validate_base_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {'https', 'http'} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise DomainError('Use a canonical HTTP(S) API base URL without credentials or query parameters')
    try:
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    except ValueError:
        raise DomainError('Invalid provider URL port') from None
    # Explicit exact host:port opt-in is reserved for trusted local model gateways.
    allowed = {entry.strip() for entry in os.environ.get('BOT_DASHBOARD_MODEL_ALLOWED_HOSTS', '').split(',') if entry.strip()}
    trusted_local = f'{parsed.hostname}:{port}' in allowed
    if parsed.scheme != 'https' and not trusted_local:
        raise DomainError('HTTP model gateways require an operator host:port allowlist entry')
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)}
    except OSError:
        raise DomainError('Provider hostname could not be resolved') from None
    if not addresses or (not trusted_local and any(not ipaddress.ip_address(address).is_global for address in addresses)):
        raise DomainError('Private model gateways require an operator host:port allowlist entry')
    return value.rstrip('/')


def _connections(session: Session) -> dict[str, Connection]:
    return {row.conn_id[len(PREFIX):]: row for row in session.scalars(select(Connection).where(Connection.conn_id.startswith(PREFIX, autoescape=True))).all()}


def _assignments(session: Session) -> list[dict]:
    row = session.scalar(select(Variable).where(Variable.key == ASSIGNMENTS))
    return json.loads(row.val) if row else []


def _public_provider(identifier: str, row: Connection) -> dict:
    extra = row.extra_dejson
    return {'id': identifier, 'name': extra['name'], 'base_url': extra['base_url'], 'has_api_key': bool(row.password), 'models': extra.get('models', [])}


def current_models() -> list[dict]:
    import yaml
    try:
        config = yaml.safe_load(Path(os.environ['BOTS_MODELS_CONFIG']).read_text())
        result = []
        for role in ROLES:
            if role.startswith('executor_') or role == 'executive':
                result.append({'role': role, 'current_provider': 'unconfigured', 'current_model': 'unconfigured'})
                continue
            configured_role = role
            alias = config.get('bots', {}).get(configured_role, config.get('default'))
            alias = alias[0] if isinstance(alias, list) else alias
            model = config.get('models', {}).get(alias, {})
            result.append({'role': role, 'current_provider': model.get('provider', 'unconfigured'), 'current_model': model.get('model', alias or 'unconfigured')})
        return result
    except (OSError, KeyError, TypeError, ValueError, yaml.YAMLError):
        return []


def get_settings(session: Session) -> dict:
    providers = _connections(session)
    assignments = _assignments(session)
    effective = {item['role']: item for item in current_models()}
    for assignment in assignments:
        provider = providers.get(assignment['provider_id'])
        effective[assignment['role']] = {'role': assignment['role'], 'current_provider': provider.extra_dejson['name'] if provider else 'unavailable', 'current_model': assignment['model']}
    return {'providers': [_public_provider(key, row) for key, row in providers.items()], 'assignments': assignments, 'roles': ROLES, 'current_models': list(effective.values())}


def test_provider(session: Session, body: ProviderBody) -> dict:
    base = validate_base_url(body.base_url)
    existing = _connections(session).get(body.id)
    key = body.api_key or (existing.password if existing else '')
    if existing and not body.api_key and existing.extra_dejson.get('base_url') != base:
        raise DomainError('Enter an API key again when changing the provider URL')
    headers = {'Authorization': f'Bearer {key}'} if key else {}
    try:
        with httpx.Client(timeout=15, follow_redirects=False, trust_env=False) as client:
            with client.stream('GET', base + '/models', headers=headers) as response:
                if response.status_code != 200:
                    raise DomainError(f'Provider connection test returned HTTP {response.status_code}')
                content = bytearray()
                for chunk in response.iter_bytes():
                    content.extend(chunk)
                    if len(content) > 1_048_576:
                        raise DomainError('Provider model list exceeds 1 MiB')
        payload = json.loads(content)
        models = sorted({item['id'] for item in payload.get('data', []) if isinstance(item, dict) and isinstance(item.get('id'), str) and 0 < len(item['id']) <= 200})[:1000]
    except (httpx.HTTPError, ValueError, TypeError, AttributeError):
        raise DomainError('Provider connection failed or returned an invalid model list') from None
    if not models:
        raise DomainError('Provider returned no models; check the API base URL and account access')
    return {'models': models, 'message': f'Connected; {len(models)} models available'}


def save_provider(session: Session, body: ProviderBody) -> dict:
    result = test_provider(session, body)
    row = _connections(session).get(body.id)
    if row is None:
        row = Connection(conn_id=PREFIX + body.id, conn_type='generic')
        session.add(row)
    if body.api_key:
        row.password = body.api_key
        if not row.is_encrypted:
            raise DomainError('Airflow Fernet encryption must be configured before storing an API key')
    row.extra = json.dumps({'name': body.name, 'base_url': body.base_url.rstrip('/'), 'models': result['models']})
    session.flush()
    return _public_provider(body.id, row)


def save_assignments(session: Session, body: SettingsBody) -> dict:
    providers = _connections(session)
    seen = set()
    for assignment in body.assignments:
        if assignment.role not in ROLES or assignment.role in seen:
            raise DomainError('Mappings require unique supported bot roles')
        seen.add(assignment.role)
        provider = providers.get(assignment.provider_id)
        if provider is None or assignment.model not in provider.extra_dejson.get('models', []):
            raise DomainError('Choose a model from a connected provider')
    values = [item.model_dump() for item in body.assignments]
    Variable.set(ASSIGNMENTS, values, serialize_json=True, session=session)
    session.flush()
    return get_settings(session)


def runtime_settings(session: Session) -> dict:
    """Only trusted service clients may receive this result; never return to the UI."""
    providers = _connections(session)
    mappings = _assignments(session)
    referenced = {item['provider_id'] for item in mappings}
    return {'providers': [{'id': key, 'base_url': row.extra_dejson['base_url'], 'api_key': row.password or ''} for key, row in providers.items() if key in referenced], 'assignments': mappings}


def model_for_role(session: Session, role: str) -> dict:
    assignment = next((item for item in _assignments(session) if item['role'] == role), None)
    if assignment is None:
        raise PreconditionFailed(f'Connect a model provider and map {role} in Model settings before starting')
    provider = _connections(session).get(assignment['provider_id'])
    if provider is None:
        raise PreconditionFailed(f'The provider mapped to {role} is missing; reconnect it in Model settings')
    return {'provider_id': assignment['provider_id'], 'model': assignment['model'], 'base_url': provider.extra_dejson['base_url']}
