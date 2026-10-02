import json, os, urllib.request
url=f"https://api.github.com/repos/{os.environ['GITHUB_REPOSITORY']}/actions/caches?per_page=100"
request=urllib.request.Request(url,headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json'})
with urllib.request.urlopen(request) as response:
    data=json.load(response)
for cache in data['actions_caches']:
    if cache['ref'] == os.environ['GITHUB_REF']:
        print(json.dumps({key:cache[key] for key in ['id','key','ref','size_in_bytes','created_at','last_accessed_at']}))

import subprocess
print(subprocess.check_output(['docker','image','inspect','notion-knowledge:smoke','notion-knowledge:smoke-builder','--format','{{.Id}} {{.Size}}'],text=True))
