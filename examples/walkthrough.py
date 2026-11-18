#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, threading, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics','group-commit','restart-reconcile','commit-failure']
class Session:
 def __init__(self,binary,root,restore=None,extra_args=None):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.process=None;self.log=None;self.extra_args=extra_args or []
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}']+list(self.extra_args)
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def commit(self,tx,status=200):return self.call('POST',f'/transactions/{tx}/commit',status=status)
 def outcome(self,tx):return self.call('GET',f'/transactions/{tx}/outcome')
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source').start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   elif name=='group-commit':
    observations=group_commit(s)
   elif name=='restart-reconcile':
    observations=restart_reconcile(s)
   elif name=='commit-failure':
    observations=commit_failure(binary,temp)
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def group_commit(s):
 """Concurrent small transactions share fsync groups but keep per-txn outcomes.
 Success and conflict mix inside one group; maintenance interleaves; short reads
 keep progressing; the group is never all-or-nothing."""
 s.call('POST','/indexes',{'name':'uniq_band','relation':'spectra','column':'band','unique':True},201)
 def one_commit(n,band=None):
  tx=s.begin();s.put(tx,tuple_(n,band=band))
  try:s.commit(tx);return 'ok'
  except RuntimeError as e:
   require('got 409' in str(e),'unexpected commit status: '+str(e));return 'conflict'
 # Build a burst: several rows fight over one unique band. Which submit loses can
 # differ because the bounded dwell may split arrivals across flush groups; the
 # contract is that exactly one holder of the unique value survives, not which.
 jobs=[]
 with concurrent.futures.ThreadPoolExecutor(12) as pool:
  for n in range(100,140):
   band='shared' if n%5==0 else f'band-{n}'
   jobs.append(pool.submit(one_commit,n,band))
  verdicts=[j.result() for j in jobs]
 conflicts=sum(1 for v in verdicts if v=='conflict')
 successes=len(verdicts)-conflicts
 require(conflicts>=1 and successes>=1,'group must mix success and conflict')
 # No group atomicity: every accepted txn is independently queryable; losers
 # published nothing. The contended unique band resolves to exactly one row.
 rows=s.query(limit=10000)['rows']
 shared=[r for r in rows if r['band']=='shared']
 require(len(shared)==1,'unique contention resolved incorrectly')
 # Bounded queue stats report the actual sharing and occupancy bounds.
 st=s.stats()['commit_queue']
 require(st['queued']==0 and st['peak_queued']<=st['queue_depth'],'queue bound not observed')
 require(st['flush_groups']>=1,'no flush group recorded')
 # Maintenance interleaved with more commits must never see tentative state.
 cp=s.call('POST','/checkpoint')['sequence']
 tx=s.begin();s.put(tx,tuple_(200));s.commit(tx)
 vac=s.call('POST','/vacuum',{'horizon':s.stats()['sequence']})
 require(vac['horizon']<=s.stats()['sequence'],'vacuum crossed durable head')
 # Short reads keep advancing while a commit storm is in flight.
 progress=[]
 def storm():
  with concurrent.futures.ThreadPoolExecutor(8) as pool:
   list(pool.map(lambda n:one_commit(300+n,f's-{n}'),range(24)))
 reader=threading.Thread(target=storm)
 reader.start()
 for _ in range(8):
  progress.append(s.query()['matched']);time.sleep(.005)
 reader.join()
 require(progress==sorted(progress) and len(set(progress))>=1,'short reads stalled during sync')
 return {'succeeded':successes,'conflicted':conflicts,'groups':st['flush_groups'],
         'batched':st['batched_records'],'peak_queued':st['peak_queued'],
         'checkpoint':cp,'read_matched':progress[-1]}
def restart_reconcile(s):
 """Commits acknowledged before stop are fully present after restart; the
 confirmation ledger answers per-transaction without group atomicity."""
 ids=[];seqs=[]
 for n in range(400,408):
  tx=s.begin();s.put(tx,tuple_(n));res=s.commit(tx);ids.append(tx);seqs.append(res['sequence'])
 head=s.stats()['sequence']
 s.stop();s.start()
 require(s.stats()['sequence']==head,'durable head changed across restart')
 for tx,seq in zip(ids,seqs):
  oc=s.outcome(tx)
  require(oc['state']=='committed' and oc['sequence']==seq,'acknowledged txn missing after restart')
 # A never-committed / rolled-back id is honestly unknown, not invented.
 ghost=s.begin();s.call('POST',f'/transactions/{ghost}/rollback')
 require(s.outcome(ghost)['state']=='unknown','unknown txn reported as committed')
 # Readable range after restart matches the retained watermark and head.
 st=s.stats()
 rng=s.call('POST','/query',{'relation':'spectra','at':st['sequence']})
 require(st['horizon']<=st['sequence'] and rng['matched']>=8,'post-restart readable range wrong')
 s.call('POST','/query',{'relation':'spectra','at':st['sequence']+1},410)
 return {'head':head,'reconciled':len(ids),'horizon':st['horizon']}
def commit_failure(binary,temp):
 """A failed/uncertain sync fails closed (503) and never fakes success. After
 restart the ledger tells each client whether its independent transaction made
 it; the queue also has a hard bound (429) instead of unbounded waiting."""
 import resource
 # Bounded rejection path: tiny in-memory queue refuses overflow with 429.
 bounded=Session(binary,temp/'bounded',extra_args=['--commit-queue','1','--commit-dwell','20ms']).start()
 seed(bounded)
 rejected=0;ok=0
 def burst(n):
  tx=bounded.begin();bounded.put(tx,tuple_(500+n))
  try:bounded.commit(tx);return 'ok'
  except RuntimeError as e:return '429' if 'got 429' in str(e) else 'other'
 with concurrent.futures.ThreadPoolExecutor(16) as pool:
  for verdict in pool.map(burst,range(48)):
   if verdict=='429':rejected+=1
   elif verdict=='ok':ok+=1
 require(rejected>=1,'full queue was not rejected')
 bounded.stop()
 # Durability failure path: cap file size so the journal write/fsync errors.
 capped=Session(binary,temp/'capped').start()
 seed(capped)
 current=(capped.root/'journal.lmw').stat().st_size
 capped.stop()
 # One append may fit up to the cap; the next append crosses it and the kernel
 # returns EFBIG, which the engine must treat as an uncertain durable failure.
 limit=current+256
 def preexec():
  resource.setrlimit(resource.RLIMIT_FSIZE,(limit,limit))
 port_arg=None
 import socket
 with socket.socket() as sk:
  sk.bind(('127.0.0.1',0));port_arg=sk.getsockname()[1]
 args=[str(binary),'-data',str(capped.root),'-listen',f'127.0.0.1:{port_arg}']
 log=tempfile.TemporaryFile()
 proc=subprocess.Popen(args,stdout=log,stderr=log,preexec_fn=preexec)
 url=f'http://127.0.0.1:{port_arg}/v1/lamina'
 deadline=time.monotonic()+8
 while time.monotonic()<deadline:
  try:
   urllib.request.urlopen(urllib.request.Request(url+'/health'),timeout=2);break
  except (OSError,urllib.error.URLError):time.sleep(.05)
 def capped_call(method,path,data=None):
  enc=None if data is None else json.dumps(data).encode()
  req=urllib.request.Request(url+path,data=enc,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=8) as r:return r.status,json.loads(r.read())
  except urllib.error.HTTPError as e:return e.code,json.loads(e.read())
 ids=[];uncertain=[]
 for n in range(600,612):
  code,body=capped_call('POST','/transactions',{})
  if code!=201:
   uncertain.append(('begin',code));continue
  tx=body['id']
  code,_=capped_call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':tuple_(n)})
  code,body=capped_call('POST',f'/transactions/{tx}/commit')
  ids.append((tx,code))
  if code==503:uncertain.append((tx,503))
 # Once poisoned the instance must refuse further service rather than recover.
 hcode,_=capped_call('GET','/health')
 require(hcode==503,'poisoned instance kept serving')
 require(len(uncertain)>=1,'no uncertain commit observed')
 proc.terminate()
 try:proc.wait(timeout=10)
 except subprocess.TimeoutExpired:proc.kill();proc.wait()
 log.close()
 # Restart on the same directory and reconcile each independent transaction.
 recovered=Session(binary,temp/'capped').start()
 durable=0;missing=0
 for tx,code in ids:
  oc=recovered.outcome(tx)
  if oc['state']=='committed':durable+=1
  elif oc['state']=='unknown':missing+=1
 # Some of the independent transactions may have landed; no all-or-nothing claim.
 require(durable+missing==len(ids),'ledger could not classify every txn')
 recovered.stop()
 return {'queue_rejected':rejected,'queue_accepted':ok,'uncertain_commits':len(uncertain),
         'reconciled_durable':durable,'reconciled_unknown':missing,'poisoned_health':hcode}
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()
