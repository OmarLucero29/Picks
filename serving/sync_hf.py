import os, sys
from pathlib import Path
from huggingface_hub import HfApi, create_repo, upload_file
SPACE=os.getenv('HF_SPACE'); TOKEN=os.getenv('HF_TOKEN')
if not SPACE or not TOKEN: 
    print('HF missing'); sys.exit(0)
api=HfApi(token=TOKEN)
try: api.repo_info(SPACE, repo_type='space')
except Exception: create_repo(repo_id=SPACE, repo_type='space', space_sdk='gradio', exist_ok=True, private=False, token=TOKEN)
def up(local,dest): upload_file(path_or_fileobj=local, path_in_repo=dest, repo_id=SPACE, repo_type='space', token=TOKEN)
Path('reports').mkdir(parents=True, exist_ok=True)
for f in ['reports/picks.csv','reports/parlay.csv']:
    if not Path(f).exists(): Path(f).write_text('')
Path('serving/space/requirements.txt').write_text('pandas\ngradio\n')
up('serving/space/app.py','app.py'); up('serving/space/requirements.txt','requirements.txt')
up('reports/picks.csv','picks.csv'); up('reports/parlay.csv','parlay.csv')
if Path('serving/prefs.json').exists(): up('serving/prefs.json','prefs.json')
print('Space synced')
