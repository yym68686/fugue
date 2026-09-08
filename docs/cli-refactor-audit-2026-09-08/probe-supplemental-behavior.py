import http.server,threading,subprocess,json,os,urllib.parse
app={'id':'app_audit','name':'audit','tenant_id':'tenant_audit','project_id':'project_audit','spec':{'image':'test:latest','replicas':1,'env':{'AUDIT_SECRET':'synthetic-secret-marker'}},'status':{'phase':'deployed'}}
state={'requests':[],'fail_inventory':False}
class H(http.server.BaseHTTPRequestHandler):
 def log_message(self,*a): pass
 def do_GET(self):
  p=urllib.parse.urlparse(self.path).path;state['requests'].append(self.path); code=200
  if p=='/v1/apps': body={'apps':[app]}
  elif p=='/v1/apps/app_audit': body={'app':app}
  elif p=='/v1/apps/app_audit/env': body={'env':app['spec']['env']}
  elif p=='/v1/tenants': body={'tenants':[]}
  elif p=='/v1/projects':body={'projects':[]}
  else: code=403;body={'error':'synthetic inventory unavailable'}
  self.send_response(code);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(json.dumps(body).encode())
s=http.server.ThreadingHTTPServer(('127.0.0.1',0),H);threading.Thread(target=s.serve_forever,daemon=True).start()
env={'PATH':os.environ['PATH'],'HOME':'/tmp','FUGUE_SKIP_UPDATE_CHECK':'1','NO_COLOR':'1','FUGUE_CONFIG_FILE':'/tmp/fugue-audit-nonexistent-config.json'}
base=['/tmp/fugue-cli-audit-20260908','--base-url',f'http://127.0.0.1:{s.server_port}','--token','synthetic-audit-key']
results=[]
for args in [['env','ls','audit','--json'],['app','release','policy','show','audit','--json'],['app','overview','audit','--json']]:
 state['requests']=[];p=subprocess.run(base+args,env=env,capture_output=True,text=True,timeout=10)
 r={'command':'fugue '+' '.join(args),'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr,'requests':state['requests'][:]};results.append(r)
 print(json.dumps(r,ensure_ascii=False))
s.shutdown();json.dump(results,open('/tmp/fugue-cli-audit-20260908-data/supplemental-behavior.json','w'),indent=2)
