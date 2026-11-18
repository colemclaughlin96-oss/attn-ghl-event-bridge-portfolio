import test from 'node:test';
import assert from 'node:assert/strict';
import {Bridge,LOCATION,SERVER_WORKER_OWNER} from '../src/bridge.mjs';
import {PROVIDER,priceAnswer} from '../src/sales-policy.mjs';
import {GhlAdapter} from '../src/ghl-adapter.mjs';
import {SendReconciler,recordReconciledProspectSend} from '../src/send-reconciler.mjs';

const C='synthetic_contact',V='synthetic_thread',OUT='synthetic_outbound',CUT='2026-10-22T15:00:00.000Z';
function fixture(t,{phase='queued',batch=false,sendResponseRecorded=false}={}){
 let now=Date.parse(CUT)+1000;
 const b=new Bridge({dbPath:':memory:',vaultKey:Buffer.alloc(32,95).toString('base64'),enabled:true,now:()=>now,processingMode:'server',serverWorkerNotBefore:CUT,operatorOwner:'synthetic_operator',prospectV1:{ingress:true,enabled:true,notBefore:CUT}}),workers=[];
 t.after(async()=>{for(const r of workers)await r.stop();b.close();});
 const m=(id='in_1',change={})=>({id,locationId:LOCATION,contactId:C,conversationId:V,conversationProviderId:PROVIDER,messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound',body:'What does Google Ads cost?',dateAdded:new Date(Date.parse(CUT)+100).toISOString(),attachments:[],...change});
 const ms=batch?[m('in_2',{body:'And setup?',dateAdded:new Date(Date.parse(CUT)+200).toISOString()}),m()]:[m()];
 const snapshot=(messages=ms,detail=null)=>{
  const read=(operationId,params,data)=>({operationId,locationId:LOCATION,params,result:{operationId,status:200,success:true,data}});
  const reads=[read('search-conversation',{query:{contactId:C,limit:2}},{total:1,conversations:[{id:V,contactId:C,locationId:LOCATION}]}),read('get-contact',{path:{contactId:C}},{contact:{id:C,locationId:LOCATION,type:'lead',dnd:false,tags:[]}}),read('get-messages',{path:{conversationId:V},query:{limit:50}},{messages:{messages:[...messages].sort((a,b)=>Date.parse(b.dateAdded)-Date.parse(a.dateAdded)),nextPage:false,lastMessageId:messages.at(-1).id}}),read('get-appointments-for-contact',{path:{contactId:C}},{events:[]}),read('search-opportunity',{query:{contactId:C,status:'all',limit:100}},{opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}}),read('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:C,limit:100,offset:0}},{data:[],totalCount:0})];
  if(detail)reads.push(read('get-message',{path:{id:detail.id}},{message:detail}));
  return {read_started_at:new Date(now).toISOString(),read_completed_at:new Date(now).toISOString(),reads,assessments:messages.filter(m=>m.direction==='inbound').map(m=>({messageId:m.id,kind:'normal'}))};
 };
 const e=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:C,direction:'inbound',loopMessageId:'synthetic_loop'},'loop_workflow'),job=b.reserveServerJobs()[0],claim=b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true});
 const call=fn=>b.withClaim(SERVER_WORKER_OWNER,e.eventId,claim.claim_token,fn);
 const prep=call(()=>b.prospectV1.prepare(SERVER_WORKER_OWNER,e.eventId,snapshot()));assert.equal(prep.prepared,true);
 const draft={text:priceAnswer('google'),intent:'pricing',priceKey:'google',answersMessageId:prep.message_id,answersMessageIds:prep.message_ids,policyReviewed:true,untrustedContentIgnored:true};
 const permit=call(()=>b.prospectV1.begin(SERVER_WORKER_OWNER,e.eventId,{snapshot:snapshot(ms,ms[0]),draft,message_id:prep.message_id,thread_version:prep.thread_version}));assert.equal(permit.permitted,true);
 const outbound=m(OUT,{direction:'outbound',source:'api',body:draft.text,status:phase==='queued'?'pending':phase,dateAdded:new Date(now).toISOString()});
 const result={attempt_id:permit.attempt_id,outcome:phase,send_response:{messageId:OUT,conversationId:V},outbound};
 call(()=>b.prospectV1.record(SERVER_WORKER_OWNER,e.eventId,phase==='unknown'?{attempt_id:permit.attempt_id,outcome:'unknown',...(sendResponseRecorded?{send_response:result.send_response}:{})}:result));
 b.processed(SERVER_WORKER_OWNER,e.eventId,'observed',{claim_token:claim.claim_token});b.finishServerJob(e.eventId,job.job_token,{category:'replied',reason:phase,reconciliationRequired:phase!=='sent'&&phase!=='delivered'});now+=20000;
 const calls=[];let response={...outbound,status:'delivered'};
 const ghl=new GhlAdapter({token:'synthetic_private_integration_token',now:()=>now,fetchImpl:async(url,init)=>{calls.push({url,init});assert.equal(init.method,'GET');assert.equal(new URL(url).pathname,'/conversations/messages/'+OUT);return response instanceof Response?response:new Response(JSON.stringify(response));}});
 const make=(extra={})=>{const r=new SendReconciler({bridge:b,ghl,...extra});workers.push(r);return r;};
 return {b,l:b.prospectV1,e,permit,claim,m,ms,snapshot,outbound,result,calls,ghl,make,get now(){return now;},advance:ms=>now+=ms,setResponse:r=>response=r,rows:()=>b.prospectV1.rows(C),args:changes=>({eventId:e.eventId,attemptId:permit.attempt_id,outboundId:OUT,result:{...result,outcome:'delivered',outbound:{...outbound,status:'delivered'}},...changes})};
}

test('queued batch reconciles from exact persisted ID, hash and provider with only GETs; no new attempt or claim',async t=>{
 const f=fixture(t,{batch:true}),beforeClaim={...f.b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(f.e.eventId)},beforeRows=f.rows(),r=f.make();
 assert.equal(r.running,false);assert.equal(f.calls.length,0);assert.equal((await r.tick()).checked,1);assert.equal(f.calls.length,2);assert.ok(f.rows().every(row=>row.phase==='delivered'&&row.outboundId===OUT));
 assert.deepEqual({...f.b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(f.e.eventId)},beforeClaim);
 for(const row of f.rows()){const before=beforeRows.find(x=>x.messageId===row.messageId),{reconciliationReceipt,...rest}=row;assert.equal(reconciliationReceipt.outcome,'delivered');assert.deepEqual({...rest,phase:before.phase},before);}
 f.advance(15000);await r.tick();assert.equal(f.calls.length,2);assert.equal(r.status().queued,0);
});

test('verified sent resumes eligibility only for a genuine new question while original consumed row remains',async t=>{
 const f=fixture(t);f.setResponse({...f.outbound,status:'sent'});await f.make().tick();assert.equal(f.rows()[0].phase,'sent');
 const question=f.m('new_question',{body:'How do I book?',dateAdded:new Date(f.now-1).toISOString()}),event=f.b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:C,direction:'inbound',loopMessageId:'new_loop'},'loop_workflow'),claim=f.b.claim(SERVER_WORKER_OWNER,event.eventId,{fenced:true});
 const p=f.b.withClaim(SERVER_WORKER_OWNER,event.eventId,claim.claim_token,()=>f.l.prepare(SERVER_WORKER_OWNER,event.eventId,f.snapshot([question,{...f.outbound,status:'sent'},...f.ms])));
 assert.equal(p.prepared,true);assert.deepEqual(p.message_ids,['new_question']);assert.equal(f.rows().find(r=>r.messageId==='in_1').attemptId,f.permit.attempt_id);
});

test('queued status backs off durably, caps reads and survives reconciler restart without send retry',async t=>{
 const f=fixture(t);f.setResponse({...f.outbound,status:'pending'});const a=f.make({maxChecks:2});await a.tick();assert.equal(f.calls.length,2);
 const restarted=f.make({maxChecks:2});await restarted.tick();assert.equal(f.calls.length,2);f.advance(15000);await restarted.tick();assert.equal(f.calls.length,4);f.advance(600000);await restarted.tick();assert.equal(f.calls.length,4);assert.equal(f.rows()[0].phase,'queued');assert.equal(f.b.db.prepare('SELECT state,checks FROM server_send_reconciliation').get().state,'needs_review');
});

test('unknown sends without recorded ID, already sent/delivered, and explicit failed sends are never polled',async t=>{
 for(const phase of ['unknown','failed','sent','delivered']){const f=fixture(t,{phase});await f.make().tick();assert.equal(f.calls.length,0);assert.equal(f.rows()[0].phase,phase==='unknown'?'attempt_unknown':phase);}
});

test('successful response identity survives verification failure and reconciles the whole unknown batch without resend',async t=>{
 const f=fixture(t,{phase:'unknown',batch:true,sendResponseRecorded:true}),before=f.rows(),claim={...f.b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(f.e.eventId)},settled=[];
 assert.ok(before.every(r=>r.phase==='attempt_unknown'&&r.outboundId===OUT&&r.sendResponseRecorded===true&&r.sendResponseConversationId===V));
 f.b.settleReconciliationJob=(eventId,result)=>settled.push({eventId,...result});await f.make().tick();
 assert.equal(f.calls.length,2);assert.ok(f.rows().every(r=>r.phase==='delivered'&&r.outboundId===OUT));
 for(const row of f.rows()){const old=before.find(r=>r.messageId===row.messageId),{reconciliationReceipt,...rest}=row;assert.equal(reconciliationReceipt.outcome,'delivered');assert.deepEqual({...rest,phase:old.phase},old);}
 assert.deepEqual({...f.b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(f.e.eventId)},claim);
 assert.deepEqual(settled,[{eventId:f.e.eventId,outcome:'delivered',reason:'verified_delivered'}]);
});

test('unknown identity without complete durable receipt scope or with mixed/stale batch cannot be reconciled',async t=>{
 for(const change of [{sendResponseRecorded:false},{sendResponseConversationId:'other'},{sendResponseConversationId:null},{outboundId:null},{phase:'queued'},{phase:'failed'}]){
  const f=fixture(t,{phase:'unknown',batch:true,sendResponseRecorded:true}),row=f.rows()[0];f.l.write('sales_messages',f.l.key(C,row.messageId),{...row,...change});
  const before=f.rows();await f.make().tick();assert.equal(f.calls.length,0);assert.deepEqual(f.rows(),before);assert.throws(()=>recordReconciledProspectSend(f.b,f.args()));
 }
});

test('actual response ID missing briefly from provider retries only GETs before a verified delivery',async t=>{
 for(const status of [404,500,502,503]){
 const f=fixture(t,{phase:'unknown',sendResponseRecorded:true}),settled=[];f.b.settleReconciliationJob=(id,r)=>settled.push(r);f.setResponse(new Response('private error ignored',{status}));const r=f.make({maxChecks:2});
 await r.tick();assert.equal(f.calls.length,1);assert.equal(f.rows()[0].phase,'attempt_unknown');assert.equal(f.b.db.prepare('SELECT state FROM server_send_reconciliation').get().state,'pending');assert.equal(settled.length,0);
 f.advance(15000);f.setResponse({...f.outbound,status:'delivered'});await r.tick();assert.equal(f.calls.length,3);assert.equal(f.rows()[0].phase,'delivered');assert.equal(settled[0].outcome,'delivered');
 }
});

test('bounded exact-ID not-found retries exhaust visibly without resetting consumed rows or sending',async t=>{
 const f=fixture(t,{phase:'unknown',sendResponseRecorded:true}),settled=[];let reads=0;f.b.settleReconciliationJob=(id,r)=>settled.push(r);
 const ghl={execute:async()=>{reads++;const e=new Error('ghl_exact_message_not_found');e.reason=e.message;throw e;},verify:async()=>assert.fail('unavailable message cannot verify'),send:async()=>assert.fail('never resend')};
 const r=f.make({ghl,maxChecks:2}),before=f.rows();await r.tick();f.advance(15000);await r.tick();f.advance(600000);await r.tick();
 assert.equal(reads,2);assert.deepEqual(f.rows(),before);assert.deepEqual(settled,[{outcome:'exhausted',reason:'reconciliation_checks_exhausted'}]);
 const job=f.b.db.prepare('SELECT state,reason,checks FROM server_send_reconciliation').get();assert.equal(job.state,'needs_review');assert.equal(job.reason,'reconciliation_checks_exhausted');assert.equal(job.checks,2);
});

test('expired reconciliation window stops read work and retains queued state for owner review',async t=>{
 const f=fixture(t);f.advance(86400000);await f.make().tick();assert.equal(f.calls.length,0);assert.equal(f.rows()[0].phase,'queued');assert.equal(f.b.db.prepare('SELECT reason FROM server_send_reconciliation').get().reason,'reconciliation_window_elapsed');
});

test('scope, ID, provider, body hash and timestamp mismatch cannot reconcile or send',async t=>{
 for(const change of [{id:'wrong'},{contactId:'other'},{locationId:'other'},{conversationId:'other'},{conversationProviderId:'other'},{messageType:'TYPE_SMS'},{direction:'inbound'},{body:'different'},{dateAdded:new Date(Date.parse(CUT)-1).toISOString()},{dateAdded:'2099-01-01T00:00:00.000Z'}]){
  const f=fixture(t);f.setResponse({...f.outbound,status:'delivered',...change});await f.make().tick();assert.equal(f.rows()[0].phase,'queued');assert.equal(f.calls.length,1);assert.equal(f.b.db.prepare('SELECT state FROM server_send_reconciliation').get().state,'needs_review');
 }
});

test('narrow persistence helper rejects forged receipt, absent consumed claim, stale batch, and false status atomically',t=>{
 for(const kind of ['receipt','status','scope','hash','claim','batch','failed']){
  const f=fixture(t,{batch:true}),args=f.args();
  if(kind==='receipt')args.result.send_response.messageId='unrecorded_id';
  if(kind==='status')args.result.outbound.status='sent';
  if(kind==='scope')args.result.outbound.contactId='other';
  if(kind==='hash')args.result.outbound.body='changed';
  if(kind==='claim')f.b.db.prepare('UPDATE processing SET send_reserved=0').run();
  if(kind==='batch'||kind==='failed'){const row=f.rows()[0];f.l.write('sales_messages',f.l.key(C,row.messageId),{...row,...(kind==='failed'?{phase:'failed'}:{outboundId:'different'})});}
  const before=f.rows();assert.throws(()=>recordReconciledProspectSend(f.b,args));assert.deepEqual(f.rows(),before);
 }
});

test('new explicit failure during network verification cannot be overwritten by late delivery read',async t=>{
 const f=fixture(t),ghl={execute:f.ghl.execute,verify:async(...args)=>{const result=await f.ghl.verify(...args),row=f.rows()[0];f.l.write('sales_messages',f.l.key(C,row.messageId),{...row,phase:'failed'});return result;}};
 await f.make({ghl}).tick();assert.equal(f.rows()[0].phase,'failed');assert.equal(f.calls.length,2);
});

test('reconciliation never clears human handoff, external lock, STOP or manual takeover state',async t=>{
 const f=fixture(t),entry=f.l.read('prospect_entries',f.l.key(C)),state=f.l.contactState(entry);state.handoff={reason:'owner_requested',eventId:f.e.eventId};state.externalLock={reason:'unverified_external_sender'};state.suppressed=true;state.manualUntil=f.now+86400000;f.l.write('sales_contacts',f.l.key(C),state);
 await f.make().tick();assert.equal(f.rows()[0].phase,'delivered');assert.deepEqual(f.l.contactState(entry),state);
});

test('429 stops all reads and persists cooldown across a new reconciler instance',async t=>{
 const f=fixture(t);f.setResponse(new Response('sensitive provider error',{status:429,headers:{'Retry-After':'60'}}));const a=f.make();await a.tick();assert.equal(f.calls.length,1);assert.equal(a.lastError,'provider_rate_limited');
 f.advance(15000);await f.make().tick();assert.equal(f.calls.length,1);f.setResponse({...f.outbound,status:'delivered'});f.advance(45000);await f.make().tick();assert.equal(f.calls.length,3);assert.equal(f.rows()[0].phase,'delivered');
});

test('network reads are bounded, one reconciliation at a time, and no send function is ever invoked',async t=>{
 const f=fixture(t);let reads=0,sends=0;const ghl={execute:async()=>{reads++;return new Promise(()=>{});},verify:async()=>{throw new Error('not reached');},send:async()=>{sends++;}};
 const r=f.make({ghl,readTimeoutMs:10});await Promise.all([r.tick(),r.tick()]);assert.equal(reads,1);assert.equal(sends,0);assert.equal(r.busy,false);assert.equal(r.lastError,'ghl_request_timeout');assert.equal(f.rows()[0].phase,'queued');
 assert.throws(()=>f.make({intervalMs:1000}),e=>e.reason==='reconciliation_configuration');
});

test('stale scheduling lease cannot overwrite a newer process diagnostic',t=>{
 const f=fixture(t),r=f.make();r.discover();const old=r.reserve();f.advance(60001);const current=r.reserve();assert.equal(current.checks,old.checks+1);recordReconciledProspectSend(f.b,f.args());r.finish(current,'completed','verified_delivered');r.finish(old,'needs_review','stale_failure');assert.equal(f.b.db.prepare('SELECT state,reason FROM server_send_reconciliation').get().state,'completed');
});

test('real reconciliation job settles only after verified ledger status and preserves unrelated job state',async t=>{
 for(const phase of ['queued','unknown']){
  const f=fixture(t,{phase,sendResponseRecorded:phase==='unknown'});assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'reconciliation_pending');
  await f.make().tick();assert.equal(f.rows()[0].phase,'delivered');assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'completed');assert.equal(f.b.db.prepare('SELECT count(*) n FROM server_alerts').get().n,0);
 }
 const f=fixture(t);f.b.db.prepare("UPDATE server_jobs SET state='needs_review',reason='unrelated_owner_review'").run();await f.make().tick();assert.equal(f.rows()[0].phase,'delivered');assert.deepEqual({...f.b.db.prepare('SELECT state,reason FROM server_jobs').get()},{state:'needs_review',reason:'unrelated_owner_review'});
});

test('resumed journal at maximum checks settles real bridge job after reservation transaction commits',async t=>{
 const f=fixture(t),r=f.make({maxChecks:2});r.discover();f.b.db.prepare('UPDATE server_send_reconciliation SET checks=2,lease_until=NULL').run();const before=f.rows();
 assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'reconciliation_pending');
 assert.equal((await r.tick()).checked,0);assert.equal(f.calls.length,0);assert.deepEqual(f.rows(),before);
 assert.deepEqual({...f.b.db.prepare('SELECT state,reason FROM server_send_reconciliation').get()},{state:'needs_review',reason:'reconciliation_checks_exhausted'});
 assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'needs_review');assert.equal(f.b.db.prepare('SELECT count(*) n FROM server_alerts').get().n,1);
});

test('failure after bridge settlement rolls back both journal and job atomically',t=>{
 const f=fixture(t),r=f.make();r.discover();const job=r.reserve(),beforeJournal={...f.b.db.prepare('SELECT * FROM server_send_reconciliation').get()},beforeJob={...f.b.db.prepare('SELECT * FROM server_jobs').get()},settle=f.b.settleReconciliationJob.bind(f.b);
 f.b.settleReconciliationJob=(...args)=>{settle(...args);throw new Error('synthetic failure before outer commit');};
 assert.throws(()=>r.finish(job,'needs_review','reconciliation_checks_exhausted'),/synthetic failure/);
 assert.deepEqual({...f.b.db.prepare('SELECT * FROM server_send_reconciliation').get()},beforeJournal);assert.deepEqual({...f.b.db.prepare('SELECT * FROM server_jobs').get()},beforeJob);assert.equal(f.b.db.prepare('SELECT count(*) n FROM server_alerts').get().n,0);
 f.b.settleReconciliationJob=settle;r.finish(job,'needs_review','reconciliation_checks_exhausted');assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'needs_review');assert.equal(f.b.db.prepare('SELECT state FROM server_send_reconciliation').get().state,'needs_review');assert.equal(f.b.db.prepare('SELECT count(*) n FROM server_alerts').get().n,1);
});

test('restart completes a journal from the exact saved terminal receipt after crash between ledger and finish',async t=>{
 for(const outcome of ['sent','delivered','failed']){
  const f=fixture(t,{batch:true}),r=f.make({maxChecks:1});r.discover();r.reserve();const args=f.args();args.result.outcome=outcome;args.result.outbound.status=outcome;recordReconciledProspectSend(f.b,args);
  const rows=f.rows(),claim={...f.b.db.prepare('SELECT * FROM processing').get()};assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'reconciliation_pending');
  f.advance(60001);const restarted=f.make({maxChecks:1});assert.equal((await restarted.tick()).checked,0);assert.equal(f.calls.length,0);assert.deepEqual(f.rows(),rows);assert.deepEqual({...f.b.db.prepare('SELECT * FROM processing').get()},claim);
  assert.equal(f.b.db.prepare('SELECT state FROM server_send_reconciliation').get().state,'completed');assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,outcome==='failed'?'needs_review':'completed');assert.equal(f.b.db.prepare('SELECT count(*) n FROM server_alerts').get().n,outcome==='failed'?1:0);
 }
});

test('terminal recovery rejects changed original claim, receipt scope/body/ID, or mixed batch without changing ledger',async t=>{
 for(const kind of ['claim','scope','hash','id','mixed']){
  const f=fixture(t,{batch:true}),r=f.make();r.discover();r.reserve();recordReconciledProspectSend(f.b,f.args());
  if(kind==='claim')f.b.db.prepare('UPDATE processing SET send_reserved=0').run();
  else{const rows=f.rows();for(const row of kind==='mixed'?rows.slice(0,1):rows){const changed=structuredClone(row);if(kind==='scope')changed.reconciliationReceipt.outbound.contactId='other';if(kind==='hash')changed.reconciliationReceipt.outbound.bodyHash='0'.repeat(64);if(kind==='id')changed.reconciliationReceipt.send_response.messageId='other';if(kind==='mixed')delete changed.reconciliationReceipt;f.l.write('sales_messages',f.l.key(C,row.messageId),changed);}}
  const before=f.rows();f.advance(60001);await f.make().tick();assert.equal(f.calls.length,0);assert.deepEqual(f.rows(),before);assert.equal(f.b.db.prepare('SELECT state FROM server_jobs').get().state,'needs_review');assert.equal(f.b.db.prepare('SELECT state FROM server_send_reconciliation').get().state,'needs_review');
 }
});

test('queued reconciliation after adapter restart uses explicit scoped history exact read and never guessed message matching',async t=>{
 const f=fixture(t),calls=[];
 const ghl=new GhlAdapter({token:'synthetic_private_integration_token',messageLookup:'conversation_history',now:()=>f.now,fetchImpl:async(url,init)=>{
  calls.push({url,init});assert.equal(init.method,'GET');const p=new URL(url).pathname;
  if(p==='/conversations/search')return new Response(JSON.stringify({conversations:[{id:V,contactId:C,locationId:LOCATION}],total:1}));
  assert.equal(p,'/conversations/'+V+'/messages');return new Response(JSON.stringify({messages:{messages:[{...f.outbound,status:'delivered'},...f.ms],nextPage:false,lastMessageId:f.ms.at(-1).id}}));
 }});
 await f.make({ghl}).tick();assert.equal(f.rows()[0].phase,'delivered');assert.equal(calls.length,4);assert.equal(calls.filter(c=>new URL(c.url).pathname==='/conversations/search').length,2);assert.equal(f.calls.length,0);
});

