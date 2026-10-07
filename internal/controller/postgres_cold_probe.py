"""Read-only physical recovery evidence. Never starts or repairs PostgreSQL."""
import hashlib,json,os,re,stat,subprocess,sys,urllib.request
mode=sys.argv[1]
data=os.environ.get('PGDATA') if mode=='source' else sys.argv[2]
if not data or os.path.realpath(data)!=data:raise RuntimeError('PGDATA must be a canonical directory')
if mode=='source':
 metrics=urllib.request.urlopen('http://127.0.0.1:9187/metrics',timeout=10).read().decode()
 if not re.search(r'^cnpg_collector_fencing_on 1(?:\.0)?$',metrics,re.M):raise RuntimeError('instance manager has not acknowledged fencing')
 status=subprocess.run(['pg_ctl','-D',data,'status'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 if status.returncode!=3:raise RuntimeError('PostgreSQL is running or its stopped state is unknown')
if os.path.islink(os.path.join(data,'pg_wal')):raise RuntimeError('external WAL is unsupported')
if os.listdir(os.path.join(data,'pg_tblspc')):raise RuntimeError('external tablespaces are unsupported')
for marker in ['backup_label','recovery.signal','standby.signal']:
 if os.path.exists(os.path.join(data,marker)):raise RuntimeError('source requires another recovery strategy: '+marker)
control=subprocess.check_output(['pg_controldata',data],env={**os.environ,'LC_ALL':'C'},text=True)
identifier=re.search(r'^Database system identifier:\s*(\d+)\s*$',control,re.M)
wal=re.search(r"^Latest checkpoint's REDO WAL file:\s*([0-9A-F]{24})\s*$",control,re.M)
if not identifier or not wal or not os.path.isfile(os.path.join(data,'pg_wal',wal[1])):raise RuntimeError('system identity or checkpoint WAL is missing')
# Instance-manager configuration is continuously maintained while fenced.
# Hash every database/WAL byte, permission and symlink; exclude only mutable
# instance configuration and runtime markers, never relation or WAL files.
ignore={'postmaster.pid','postmaster.opts','postgresql.conf','postgresql.auto.conf','custom.conf','pg_hba.conf','pg_ident.conf'}
h=hashlib.sha256();content_h=hashlib.sha256();metadata_h=hashlib.sha256();metadata_entries=[];count=0;size=0
for root,dirs,files in os.walk(data,followlinks=False):
 dirs.sort();files.sort()
 for name in sorted(dirs+files):
  p=os.path.join(root,name);relative=os.path.relpath(p,data)
  if root==data and name in ignore:continue
  s=os.lstat(p)
  if not (stat.S_ISREG(s.st_mode) or stat.S_ISDIR(s.st_mode) or stat.S_ISLNK(s.st_mode)):raise RuntimeError('unsupported PGDATA object: '+relative)
  content=''
  if stat.S_ISREG(s.st_mode):
   fhash=hashlib.sha256()
   with open(p,'rb') as f:
    while True:
     chunk=f.read(1024*1024)
     if not chunk:break
     fhash.update(chunk)
   content=fhash.hexdigest();size+=s.st_size;count+=1
  elif stat.S_ISLNK(s.st_mode):content=os.readlink(p)
  link=os.readlink(p) if stat.S_ISLNK(s.st_mode) else ''
  entry=[relative,s.st_mode,s.st_uid,s.st_gid,s.st_size if stat.S_ISREG(s.st_mode) else 0,content]
  encoded=json.dumps(entry,separators=(',',':'),ensure_ascii=True).encode()+b'\n'
  h.update(encoded)
  metadata=[relative,s.st_mode,s.st_uid,s.st_gid,s.st_size if stat.S_ISREG(s.st_mode) else 0,link]
  metadata_entries.append(metadata)
  metadata_h.update(json.dumps(metadata,separators=(',',':'),ensure_ascii=True).encode()+b'\n')
  content_h.update(json.dumps([relative,content],separators=(',',':'),ensure_ascii=True).encode()+b'\n')
with open(os.path.join(data,'global','pg_control'),'rb') as f:control_hash=hashlib.sha256(f.read()).hexdigest()
owner=os.stat(data)
print(json.dumps({'system_id':identifier[1],'file_digest':h.hexdigest(),'content_digest':content_h.hexdigest(),'metadata_digest':metadata_h.hexdigest(),'metadata_entries':metadata_entries,'control_digest':control_hash,'files':count,'bytes':size,'data_path':data,'uid':owner.st_uid,'gid':owner.st_gid,'version':open(os.path.join(data,'PG_VERSION')).read().strip()}))
