import http.server,threading,subprocess,json,os,time
state={'mode':'pending','requests':[]}
class H(http.server.BaseHTTPRequestHandler):
 def log_message(self,*a): pass
 def do_GET(self):
  state['requests'].append(self.path)
  if state['mode'].isdigit():
   self.send_response(int(state['mode'])); body={'error':'opaque failure','code':'test_failure'}
  else:
   self.send_response(200);body={'operation':{'id':'op_audit','type':'deploy','status':state['mode'],'error_message':'synthetic failure' if state['mode']=='failed' else '', 'created_at':'2026-09-08T00:00:00Z','updated_at':'2026-09-08T00:00:00Z'}}
  self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(json.dumps(body).encode())
s=http.server.ThreadingHTTPServer(('127.0.0.1',0),H); threading.Thread(target=s.serve_forever,daemon=True).start()
env={'PATH':os.environ['PATH'],'HOME':'/tmp','FUGUE_SKIP_UPDATE_CHECK':'1','NO_COLOR':'1','FUGUE_CONFIG_FILE':'/tmp/fugue-audit-nonexistent-config.json'}
base=['/tmp/fugue-cli-audit-20260908','--base-url',f'http://127.0.0.1:{s.server_port}','--token','synthetic-audit-key']
results=[]
for mode,args in [('pending',['operation','wait','op_audit']),('failed',['operation','wait','op_audit']),('failed',['operation','wait','op_audit','--json']),('403',['operation','show','op_audit','--json']),('404',['operation','show','op_audit','--json']),('pending',['operation','show','op_audit','--typo','--json'])]:
 state['mode']=mode;state['requests']=[];t=time.monotonic()
 p=subprocess.run(base+args,env=env,capture_output=True,text=True,timeout=10)
 results.append({'api_state':mode,'command':'fugue '+' '.join(args),'exit':p.returncode,'elapsed_ms':round((time.monotonic()-t)*1000),'stdout':p.stdout,'stderr':p.stderr,'requests':state['requests'][:]})
s.shutdown();json.dump(results,open('/tmp/fugue-cli-audit-20260908-data/behavior.json','w'),indent=2)
for r in results: print(json.dumps(r,ensure_ascii=False))
