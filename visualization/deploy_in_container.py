"""Run deploy/upload/validate in one disposable CLI session."""
import json
import os
import pathlib
import subprocess
import sys

mode, project_path, create = sys.argv[1:]
common = ['--project-dir',project_path,'--profiles-dir',project_path,'--skip-dbt-compile','--no-partial-compilation','--validate-warehouse-columns']
verb = 'start-preview' if mode == 'preview' else 'deploy'
extra = ['--name','vintage-data-preview','--skip-copy-content','--no-combine','--assume-yes'] if mode == 'preview' else (['--create','Vintage Data'] if create == '1' else [])
subprocess.run(['lightdash',verb,*common,*extra],check=True)
config = json.loads(subprocess.check_output(['node', '-e', "const fs=require('fs'); const yaml=require('/opt/vintage-cli/node_modules/yaml'); process.stdout.write(JSON.stringify(yaml.parse(fs.readFileSync(process.argv[1],'utf8'))));", str(pathlib.Path.home()/'.config/lightdash/config.yaml')], text=True))
context = config.get('context',{})
project = context.get('previewProject') if mode == 'preview' else (context.get('project') if create == '1' else os.environ.get('LIGHTDASH_PROJECT') or context.get('project'))
if not project:
    raise RuntimeError('No project identity after deployment; refusing to upload to an implicit project')
os.environ['LIGHTDASH_PROJECT'] = project
identity = {'project_uuid':project,'mode':mode}
(pathlib.Path(project_path)/'deployment.json').write_text(json.dumps(identity)+'\n')
# Preserve identity even if upload/validation fails so retries cannot create
# another production project. This file is not a success marker.
if mode == 'deploy':
    pathlib.Path('/state/deployment.json').write_text(json.dumps(identity)+'\n')
subprocess.run(['lightdash','upload','--path','/content','--force','--project',project],check=True)
subprocess.run(['lightdash','validate',*common,'--project',project],check=True)
(pathlib.Path(project_path)/'validation.json').write_text(json.dumps({'project_uuid':project,'status':'ok'})+'\n')
