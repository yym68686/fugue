import pathlib, subprocess, re, json, collections, concurrent.futures, os
repo=pathlib.Path(__file__).resolve().parents[2]
out=pathlib.Path('/tmp/fugue-cli-audit-20260908'); out=pathlib.Path(str(out)+'-data'); out.mkdir(exist_ok=True)
env={'PATH':os.environ['PATH'],'HOME':'/tmp','FUGUE_SKIP_UPDATE_CHECK':'1','NO_COLOR':'1','FUGUE_CONFIG_FILE':'/tmp/fugue-audit-nonexistent-config.json'}
def help_cmd(parts):
 p=subprocess.run(['/tmp/fugue-cli-audit-20260908',*parts,'--help'],capture_output=True,text=True,env=env,timeout=15)
 txt=p.stdout; children=[]; on=False
 for line in txt.splitlines():
  if line=='Available Commands:': on=True; continue
  if on:
   if not line.strip(): on=False; continue
   m=re.match(r'^  ([a-zA-Z][a-zA-Z0-9-]*)\s+(.*)',line)
   if m and m[1]!='help': children.append(m[1])
 usage=re.search(r'Usage:\n  ([^\n]+)',txt)
 return {'command':'fugue' + (' '+' '.join(parts) if parts else ''),'parts':parts,'help':txt,'children':children,'usage':usage[1] if usage else '', 'code':p.returncode}
items=[];queue=[[]]
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
 while queue:
  batch=list(pool.map(help_cmd,queue));items+=batch
  queue=[x['parts']+[sub] for x in batch for sub in x['children']]
json.dump(items,open(out/'commands.json','w'),ensure_ascii=False,indent=2)
counts=collections.Counter(x['parts'][0] for x in items if x['parts'])
print('visible nodes',len(items),'leaves',sum(not x['children'] for x in items),'groups',dict(counts))
print('root:',', '.join(items[0]['children']))
print('admin:',', '.join(next(x['children'] for x in items if x['parts']==['admin'])))
for x in items:
 if x['parts'] in [['deploy'],['app'],['operation'],['backup'],['data'],['admin','platform'],['admin','release']]: print(x['command'],x['children'])
# Contract operation index, using structural indentation in the authoritative YAML.
lines=(repo/'openapi/openapi.yaml').read_text().splitlines();ops=[];path=None;op=None
for n,line in enumerate(lines,1):
 if re.match(r'^  /[^ ]+:$',line): path=line[2:-1];op=None
 elif path and re.match(r'^    (get|post|put|patch|delete|head|options):$',line):
  op={'path':path,'method':line.strip()[:-1].upper(),'line':n};ops.append(op)
 elif op:
  m=re.match(r'^      (operationId|summary|tags|x-fugue-handler): (.*)',line)
  if m: op[m[1]]=m[2]
 if line=='components:': break
json.dump(ops,open(out/'operations.json','w'),ensure_ascii=False,indent=2)
print('OpenAPI operations',len(ops),'paths',len(set(o['path'] for o in ops)))
print('tags',dict(collections.Counter(o.get('tags','') for o in ops)))
