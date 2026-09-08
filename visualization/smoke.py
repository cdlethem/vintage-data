"""Explicit disposable-stack bootstrap and contract fixture for integration tests.

Never point --env-file at production: this publishes synthetic contract rows.
Only accepts a /tmp state directory and a non-default database port.
"""
import argparse
import copy
import json
import os
import pathlib
import secrets
import sys
import uuid

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from visualization.api import Client,bootstrap
from visualization import project,publisher


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file',type=pathlib.Path,required=True)
    parser.add_argument('--manifest',type=pathlib.Path,required=True)
    args=parser.parse_args()
    values=dict(line.split('=',1) for line in args.env_file.read_text().splitlines() if line and not line.startswith('#'))
    root=pathlib.Path(values['LIGHTDASH_STATE_ROOT']).resolve()
    if not root.is_relative_to('/tmp') or values['LIGHTDASH_PG_PORT'] in {'5432','5433'}:
        raise ValueError('smoke runner requires an isolated /tmp stack on a test port')
    if not values.get('LIGHTDASH_API_KEY'):
        values['LIGHTDASH_ADMIN_PASSWORD']=values.get('LIGHTDASH_ADMIN_PASSWORD') or secrets.token_hex(32)
        # Save the password before requests so an interrupted bootstrap can log in again.
        args.env_file.write_text('\n'.join(f'{k}={v}' for k,v in values.items())+'\n')
        values['LIGHTDASH_API_KEY']=bootstrap(values['LIGHTDASH_URL'],'integration@vintage-data.local',values['LIGHTDASH_ADMIN_PASSWORD'])
        args.env_file.write_text('\n'.join(f'{k}={v}' for k,v in values.items())+'\n')
    os.environ.update(values)
    manifest=json.loads(args.manifest.read_text())
    uid=str(uuid.uuid4())
    manifest['metadata']['invocation_id']=uid
    manifest['metadata']['vintage_test_fixture']=True
    results={'metadata':{'invocation_id':uid},'results':[]}
    import duckdb
    warehouse=root/'fixture.duckdb'
    with duckdb.connect(str(warehouse)) as con:
        con.execute('CREATE SCHEMA IF NOT EXISTS transform_marts')
        for model_id,node in project.mart_nodes(manifest).items():
            name=node.get('alias') or node['name']
            node['schema']='transform_marts'
            definition=', '.join(project.identifier(k)+' '+c['data_type'] for k,c in node['columns'].items())
            con.execute(f'CREATE OR REPLACE TABLE transform_marts.{project.identifier(name)} ({definition})')
            expressions=[]
            for col,cfg in node['columns'].items():
                typ=cfg['data_type'].lower()
                value='current_timestamp' if 'timestamp' in typ else 'current_date' if typ=='date' else "'{}'" if typ=='json' else ('false' if col=='is_missing' else 'true') if typ=='boolean' else '1' if typ.startswith(('bigint','integer','double','decimal')) else "'Sample'"
                expressions.append(value)
            con.execute(f'INSERT INTO transform_marts.{project.identifier(name)} VALUES ({", ".join(expressions)})')
            results['results'].append({'unique_id':model_id,'status':'success'})
    batch=publisher.capture(manifest,results,warehouse,root)
    outcome=publisher.publish(pathlib.Path(batch['path']))
    print('Published synthetic integration fixture:',len(outcome['published']),'marts')
    project.bundle(manifest,root/'bundles/current','vintage_serving')
    print('Bootstrap and bundle ready; credentials retained only in the test env file.')


if __name__=='__main__':
    main()
