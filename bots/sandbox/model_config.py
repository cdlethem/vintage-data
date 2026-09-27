"""Public in-sandbox model configuration: no upstream credentials."""
import json
from pathlib import Path

def write_config(home: Path, model: str):
    if not isinstance(model, str) or not model.strip() or len(model) > 512 or any(ord(char) < 32 for char in model):
        raise ValueError('invalid model identifier')
    path=home/'.omp/agent';path.mkdir(parents=True,mode=0o700)
    # JSON is valid YAML and avoids another launcher-side dependency.
    (path/'models.yml').write_text(json.dumps({'providers':{'gateway':{'baseUrl':'http://127.0.0.1:18080/v1','api':'openai-completions','apiKey':'sandbox-scoped','models':[{'id':model,'name':model,'contextWindow':200000,'maxTokens':16384,'reasoning':True}]}}}))
    (path/'config.yml').write_text(json.dumps({'modelRoles':{r:'gateway/'+model for r in ['task','default','plan','smol','tiny']},'tools':{'approvalMode':'yolo'}}))
