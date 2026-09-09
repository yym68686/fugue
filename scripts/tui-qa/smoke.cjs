// Run after npm install in this directory: FUGUE_TUI_BINARY=/path/to/fugue npm run smoke
// Only synthetic, local HTTP data is used. No credentials or production writes.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const pty = require('node-pty');
const { Terminal } = require('@xterm/headless');
const binary = process.env.FUGUE_TUI_BINARY;
assert(binary, 'FUGUE_TUI_BINARY is required');
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const app = {id:'app-a', name:'Sample API', tenant_id:'tenant-a', project_id:'project-a', spec:{image:'example/image:latest', runtime_id:'runtime-a', replicas:2}, status:{phase:'ready',current_replicas:2}};
const project = {id:'project-a',name:'Sample',tenant_id:'tenant-a'};
let disconnected = false;
let activeStreams = 0;
let writes = 0;
const counts = {};
const server = http.createServer((req,res) => {
 const url = new URL(req.url, 'http://localhost');
 counts[url.pathname] = (counts[url.pathname] || 0) + 1;
 if (req.method !== 'GET') { writes++; res.writeHead(405); res.end(); return; }
 const now = new Date().toISOString();
 const result = value => { res.setHeader('Content-Type','application/json');res.end(JSON.stringify(value)); };
 if (url.pathname.endsWith('/stream')) {
  activeStreams++;
  res.writeHead(200,{'Content-Type':'text/event-stream'});
  res.write('event: ready\ndata: {}\n\n');
  let index=0;
  const timer=setInterval(() => {
   if(url.pathname.includes('runtime-logs')) res.write(`event: log\nid: log-${++index}\ndata: {"line":"repeated log line ${index}"}\n\n`);
   else res.write('event: changed\ndata: {}\n\n');
  },200);
  req.on('close',()=>{activeStreams--;clearInterval(timer)});
  return;
 }
 if (disconnected && url.pathname==='/v1/apps/app-a') { res.writeHead(503);res.end('{"error":"synthetic disconnect"}');return; }
 switch(url.pathname) {
 case '/v1/auth/context': return result({principal:{platform_admin:req.headers.authorization==='Bearer synthetic-admin',scopes:['app.read','app.deploy','app.scale','app.observability.read'],tenant_id:'tenant-a'}});
 case '/openapi.json': return result({paths:{}}); // old server must keep TUI writes disabled
 case '/v1/tenants': return result({tenants:[{id:'tenant-a',name:'Sample',slug:'sample'}]});
 case '/v1/projects': return result({projects:[project]});
 case '/v1/console/projects/project-a': return result({project_id:project.id,project_name:project.name,project,apps:[app],operations:[]});
 case '/v1/apps': return result({apps:[app]});
 case '/v1/apps/app-a': return result({app});
 case '/v1/apps/app-a/runtime-pods': return result({groups:[{pods:Array.from({length:1000},(_,i)=>({name:`pod-${i}`,node_name:'node-a',phase:'Running',ready:true,containers:[]}))}]});
 case '/v1/apps/app-a/runtime-logs': return result({logs:'snapshot log line\n',available:true});
 case '/v1/apps/app-a/observability/metrics/timeseries': return result({source:{available:true,status:'available'},series:['cpu','memory','rpm','p95_duration_ms','error_rate'].map((name,i)=>({name,unit:i===1?'bytes':'%',source:'synthetic timestamped history',state:'available',interval_seconds:5,points:Array.from({length:120},(_,j)=>({observed_at:new Date(Date.now()-(119-j)*5000).toISOString(),value:(j*7+i*11)%90}))}))});
 case '/v1/operations': return result({operations:[]});
 case '/v1/cluster/nodes': return result({cluster_nodes:[{name:'node-a',status:'ready',observed_at:now,cpu:{usage_percent:45},memory:{usage_percent:60},ephemeral_storage:{usage_percent:30},workloads:[{kind:'app',id:app.id,name:app.name,tenant_id:app.tenant_id,project_id:app.project_id,pod_count:2}]}]});
 case '/v1/cluster/node-policies/status': return result({summary:{total:1,ready:1,reconciled:1},node_policies:[]});
 case '/v1/runtimes': return result({runtimes:[{id:'runtime-a',name:'Sample Runtime',status:'active',type:'managed-owned',cluster_node_name:'node-a'}]});
 case '/v1/runtimes/runtime-a': return result({runtime:{id:'runtime-a',name:'Sample Runtime',status:'active',cluster_node_name:'node-a'}});
 case '/v1/cluster/control-plane': return result({control_plane:{status:'ready',observed_at:now,components:[{component:'api',deployment_name:'control-api',status:'ready',desired_replicas:1,ready_replicas:1,image_tag:'build-a',observed_pods:[{name:'api-pod',node_name:'node-a',phase:'Running',ready:true}]}],deploy_workflow:{workflow:'ci.yml',head_sha:'abcdef0123456789',status:'completed',conclusion:'success'}}});
 default: res.writeHead(404);return result({error:'unimplemented synthetic endpoint'});
 }
});

async function run(args,{admin=false,termName='xterm-256color',color='truecolor',quit='q'}={}) {
 const terminal=new Terminal({cols:140,rows:40,allowProposedApi:true});
 const started=Date.now();let output='',exitCode;let inputP95;
 const proc=pty.spawn(binary,[...args,'--base-url',`http://127.0.0.1:${server.address().port}`,'--token',admin?'synthetic-admin':'synthetic-user','--interval','1s','--mouse','--theme','carbon'],{name:termName,cols:140,rows:40,env:{...process.env,TERM:termName,COLORTERM:color,NO_COLOR:color==='none'?'1':'',FUGUE_SKIP_UPDATE_CHECK:'1'}});
 const done=new Promise(resolve=>proc.onExit(value=>{exitCode=value.exitCode;resolve(value)}));
 proc.onData(data=>{output+=data;terminal.write(data)});terminal.onData(data=>proc.write(data));
 const text=()=>Array.from({length:terminal.rows},(_,i)=>terminal.buffer.active.getLine(i)?.translateToString(true)||'').join('\n');
 const wait=async (condition,label)=>{const deadline=Date.now()+12000;while(Date.now()<deadline){if(condition())return;if(exitCode!==undefined)throw Error(`early exit ${exitCode}: ${output}`);await pause(25)}throw Error(`${label}\n${text()}`)};
 const clickLabel=label=>{for(let y=0;y<terminal.rows;y++){let x=(terminal.buffer.active.getLine(y)?.translateToString(true)||'').indexOf(label);if(x>=0){proc.write(`\x1b[<0;${x+2};${y+1}M\x1b[<0;${x+2};${y+1}m`);return}}throw Error('missing click label '+label)};
 try {
  await wait(()=>text().includes('FUGUE'),'skeleton');const firstPaint=Date.now()-started;
  assert(firstPaint<1000,`first paint ${firstPaint}ms`);
  if(args[0]==='app') {
   await wait(()=>text().includes('pod-0'),'app table');
   const latencies=[];
   for(let i=1;i<=40;i++){
    const begin=performance.now();proc.write('j');const deadline=Date.now()+2000;
    while(!text().includes(`› pod-${i} `)) {assert(Date.now()<deadline,'selection response timed out');await pause(1)}
    latencies.push(performance.now()-begin);
   }
   latencies.sort((a,b)=>a-b);inputP95=latencies[Math.ceil(latencies.length*.95)-1];assert(inputP95<=50,`input p95 ${inputP95.toFixed(1)}ms`);
   proc.write('\x1b[H');await wait(()=>text().includes('› pod-0 '),'home');
   clickLabel('Logs');await wait(()=>text().includes('repeated log line'),'SSE log');
   proc.write('1');await wait(()=>text().includes('PODS'),'dashboard');
   await wait(()=>activeStreams===0,'subscription cancellation');
   proc.resize(60,18);terminal.resize(60,18);await pause(150);proc.write('j'.repeat(15));await wait(()=>text().includes('pod-15'),'narrow scrolling');
   proc.resize(140,40);terminal.resize(140,40);await pause(150);
   disconnected=true;proc.write('r');await wait(()=>text().includes('STALE'),'stale');assert(text().includes('pod-'));
   disconnected=false;proc.write('r');await wait(()=>!text().includes('STALE'),'recovery');
  } else if(admin) {
   await wait(()=>text().includes('CONTROL PLANE'),'admin components');
   proc.write('\r');await wait(()=>text().includes('api-pod'),'component drilldown');
  } else await wait(()=>text().includes('Sample'),'workspace/project');
  proc.write('?');await wait(()=>text().includes('KEYBOARD'),'keyboard help');
  proc.write(quit);const exited=await done;assert.equal(exited.exitCode,0);assert(output.includes('\x1b[?1049l'),'alt screen restored');assert(output.includes('\x1b[?1006l'),'mouse restored');
  return {command:args.join(' '),firstPaint,inputP95,terminal:termName,color,exit:quit==='q'?'q':'Ctrl-C'};
 } finally { if(exitCode===undefined)proc.kill();terminal.dispose();disconnected=false; }
}
(async()=>{
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const results=[];
 for(const [termName,color] of [['xterm-256color','truecolor'],['xterm-256color',''],['ansi',''],['xterm-256color','none']])results.push(await run(['app','top','Sample API'],{termName,color}));
 results.push(await run(['admin','cluster','top','--scope','control-plane'],{admin:true}));
 results.push(await run(['project','top','Sample'],{quit:'\x03'}));
 results.push(await run(['console']));
 await pause(300);assert.equal(activeStreams,0);assert.equal(writes,0);
 const report={status:'pass',results,activeStreams,writes,requests:counts};
 if(process.env.FUGUE_TUI_REPORT)fs.writeFileSync(process.env.FUGUE_TUI_REPORT,JSON.stringify(report,null,2)+'\n');
 console.log(JSON.stringify(report));
})().catch(err=>{console.error(err);process.exitCode=1}).finally(()=>server.close());
