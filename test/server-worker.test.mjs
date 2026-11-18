import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,EVENT,LOCATION,SERVER_WORKER_OWNER,PROCESSING_LEASE_MS} from '../src/bridge.mjs';
import {ServerWorker} from '../src/server-worker.mjs';
import {PROVIDER,priceAnswer} from '../src/sales-policy.mjs';
import {recordReconciledProspectSend} from '../src/send-reconciler.mjs';

const OPERATOR='operator_fixture',CONTACT='prospect_one',THREAD='thread_one',CUT='2026-10-21T15:00:00.000Z';
function fixture(t,{mode='server',replies=true}={}){
  let now=Date.parse(CUT)+1000;const dir=mkdtempSync(path.join(tmpdir(),'attn-server-worker-')),handles=new Set(),workers=[],callbacks=[];
  const opts={dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,97).toString('base64'),enabled:true,now:()=>now,processingMode:mode,serverWorkerNotBefore:mode==='server'?CUT:null,operatorOwner:OPERATOR,prospectV1:{ingress:true,enabled:replies,notBefore:'2026-10-21T14:00:00.000Z'},post:async(url,body)=>{callbacks.push(JSON.parse(body));return {status:200,body:JSON.stringify({challenge:JSON.parse(body).challenge})};}};
  let b=new Bridge(opts);handles.add(b);
  const message=(id='in_1',extra={})=>({id,locationId:LOCATION,contactId:CONTACT,conversationId:THREAD,conversationProviderId:PROVIDER,messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound',body:'What does Google Ads cost?',attachments:[],dateAdded:new Date(now-100).toISOString(),...extra});
  const inbound=message();
  const snapshot=({candidateId=null}={})=>{
    const read=(operationId,params,data)=>({operationId,locationId:LOCATION,params,result:{operationId,status:200,success:true,data}});
    const reads=[read('search-conversation',{query:{contactId:CONTACT,limit:2}},{total:1,conversations:[{id:THREAD,contactId:CONTACT,locationId:LOCATION}]}),read('get-contact',{path:{contactId:CONTACT}},{contact:{id:CONTACT,locationId:LOCATION,type:'lead',dnd:false,tags:['imessage']}}),read('get-messages',{path:{conversationId:THREAD},query:{limit:50}},{messages:{messages:[inbound],nextPage:false,lastMessageId:inbound.id}}),read('get-appointments-for-contact',{path:{contactId:CONTACT}},{events:[]}),read('search-opportunity',{query:{contactId:CONTACT,status:'all',limit:100}},{opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}}),read('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:CONTACT,limit:100,offset:0}},{data:[],totalCount:0})];
    if(candidateId)reads.push(read('get-message',{path:{id:candidateId}},{message:inbound}));
    return {read_started_at:new Date(now).toISOString(),read_completed_at:new Date(now).toISOString(),reads,assessments:[{messageId:inbound.id,kind:'normal'}]};
  };
  const draft={text:priceAnswer('google'),intent:'pricing',priceKey:'google',answersMessageId:'in_1',policyReviewed:true,untrustedContentIgnored:true};
  const adapters={snapshot:async x=>snapshot(x),generate:async()=>({finishReason:'stop',complete:true,draft}),send:async()=>({messageId:'out_1',conversationId:THREAD}),verify:async()=>({outcome:'sent',send_response:{messageId:'out_1',conversationId:THREAD},outbound:message('out_1',{direction:'outbound',body:draft.text,status:'sent',dateAdded:new Date(now).toISOString()})})};
  const f={get b(){return b;},opts,callbacks,adapters,draft,snapshot,message,get now(){return now;},advance:ms=>now+=ms,setNow:at=>now=at,
    event:(id='loop_1',contact=CONTACT)=>b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:contact,direction:'inbound',loopMessageId:id},'loop_workflow'),
    subscribe:()=>b.subscribe(OPERATOR,{name:EVENT,arguments:{location_id:LOCATION,scope:'attn_incoming_prospects'},delivery:{mode:'webhook',url:'https://callback.example.com/',secret:'whsec_'+Buffer.alloc(32,22).toString('base64')}}),
    worker:(extra={})=>{const w=new ServerWorker({bridge:b,adapters,now:()=>now,...extra});workers.push(w);return w;},
    restart:()=>{b.close();handles.delete(b);b=new Bridge(opts);handles.add(b);},fork:()=>{const other=new Bridge(opts);handles.add(other);return other;}};
  t.after(async()=>{for(const worker of workers)await worker.stop();for(const handle of handles)handle.close();rmSync(dir,{recursive:true,force:true});});return f;
}
const job=(b,id)=>b.db.prepare('SELECT * FROM server_jobs WHERE event_id=?').get(id);
const fakeFactory=handler=>options=>({run:async(eventId,{recover})=>{
  const claim=options.bridge.claim(options.owner,eventId,{fenced:true,recover});assert.equal(claim.claimed,true);
  if(handler){const result=await handler(options,eventId,claim);if(result)return result;}
  options.bridge.processed(options.owner,eventId,'observed',{claim_token:claim.claim_token});return {category:'suppressed',reason:'offline_fixture_completed'};
}});

test('server ingress is durable without a native subscription and deduplicates once',async t=>{
  const f=fixture(t),e=f.event();assert.equal(e.accepted,true);assert.equal(e.worker_enqueued,true);assert.equal(f.event().duplicate,true);assert.equal(f.b.db.prepare('SELECT count(*) n FROM server_jobs').get().n,1);assert.equal(f.b.db.prepare('SELECT count(*) n FROM deliveries').get().n,0);
  assert.throws(()=>f.b.eventForOwner(OPERATOR,e.eventId),x=>x.reason==='server_owned_event');assert.throws(()=>f.b.claim(OPERATOR,e.eventId),x=>x.reason==='native_processing_disabled');await f.b.drain();assert.equal(f.callbacks.length,0);
  const worker=f.worker({runnerFactory:fakeFactory()}).start({poll:false});await worker.idle();assert.equal(job(f.b,e.eventId).state,'completed');
});
test('native mode remains default and requires its original subscription path',t=>{
  const f=fixture(t,{mode:'native'});assert.equal(f.event().reason,'no_active_subscription');assert.throws(()=>f.worker(),/server_worker_mode_required/);
});
test('cutover is immutable, rejects pre-cutoff ingress, and never reassigns historical native events',async t=>{
  const f=fixture(t,{mode:'native'});await f.subscribe();const historical=f.event('historical');assert.equal(historical.accepted,true);f.advance(1000);f.opts.processingMode='server';f.opts.serverWorkerNotBefore=new Date(f.now-1).toISOString();f.restart();
  const duplicate=f.event('historical');assert.equal(duplicate.worker_enqueued,false);assert.equal(job(f.b,historical.eventId),undefined);const fresh=f.event('fresh');assert.equal(fresh.worker_enqueued,true);
  assert.equal(f.b.reserveServerJobs({limit:8}).length,1);await f.b.drain();assert.equal(f.callbacks.length,1);
  assert.throws(()=>new Bridge({...f.opts,serverWorkerNotBefore:new Date(f.now).toISOString()}),/server_worker_release_immutable/);
  const g=fixture(t);g.setNow(Date.parse(CUT));assert.equal(g.event().reason,'before_server_worker_cutoff');g.advance(1);assert.equal(g.event().worker_enqueued,true);
});
test('worker starts immediately, bounds global concurrency, and serializes each contact',async t=>{
  const f=fixture(t),first=f.event('a1');f.advance(1);const second=f.event('a2');f.advance(1);const other=f.event('b','prospect_two'),started=[],releases=new Map();let active=0,maximum=0;
  const worker=f.worker({concurrency:2,runnerFactory:fakeFactory(async(o,id)=>{started.push(id);active++;maximum=Math.max(maximum,active);await new Promise(resolve=>releases.set(id,resolve));active--;})}).start({poll:false});
  await Promise.resolve();await Promise.resolve();assert.deepEqual(new Set(started),new Set([first.eventId,other.eventId]));assert.equal(started.includes(second.eventId),false);
  releases.get(other.eventId)();for(let i=0;i<8;i++)await Promise.resolve();assert.equal(started.includes(second.eventId),false);
  releases.get(first.eventId)();for(let i=0;i<12;i++)await Promise.resolve();assert.equal(started.includes(second.eventId),true);releases.get(second.eventId)();await worker.idle();assert.equal(maximum,2);assert.equal(job(f.b,second.eventId).state,'completed');
});
test('canary allowlist and replies-off mode leave other jobs durably queued',async t=>{
  const f=fixture(t,{replies:false}),blocked=f.event('other','prospect_two'),allowed=f.event('owner',CONTACT),worker=f.worker({allowedContacts:[CONTACT],runnerFactory:fakeFactory()}).start({poll:false});await worker.idle();assert.equal(job(f.b,allowed.eventId).state,'pending');
  f.b.prospectV1.enabled=true;worker.wake();await worker.idle();assert.equal(job(f.b,allowed.eventId).state,'completed');assert.equal(job(f.b,blocked.eventId).state,'pending');assert.equal(worker.status().canary_restricted,true);assert.equal(f.b.serverWorkerStatus(OPERATOR).jobs.some(x=>x.state==='pending'),true);
});
test('queued jobs survive restart and run once without a native callback',async t=>{
  const f=fixture(t),e=f.event();f.restart();let runs=0;const worker=f.worker({runnerFactory:fakeFactory(async()=>{runs++;})}).start({poll:false});await worker.idle();worker.wake();await worker.idle();assert.equal(runs,1);assert.equal(job(f.b,e.eventId).state,'completed');assert.equal(f.callbacks.length,0);
});
test('job reservations serialize across processes and an expired pre-claim lease can recover',t=>{
  const f=fixture(t),a=f.event('a');f.advance(1);f.event('a2');f.advance(1);const b=f.event('b','prospect_two'),first=f.b.reserveServerJobs({limit:1})[0],other=f.fork(),next=other.reserveServerJobs({limit:2});
  assert.equal(first.event_id,a.eventId);assert.deepEqual(next.map(x=>x.event_id),[b.eventId]);f.advance(PROCESSING_LEASE_MS+1);const recovered=f.b.reserveServerJobs({limit:1})[0];assert.equal(recovered.event_id,a.eventId);assert.equal(recovered.recover,false);assert.throws(()=>f.b.finishServerJob(first.event_id,first.job_token,{}),x=>x.reason==='server_job_fence');
});
test('global concurrency reservation is bounded across independent process connections',t=>{
  const f=fixture(t);f.event('a','a');f.advance(1);f.event('b','b');f.advance(1);f.event('c','c');const other=f.fork();assert.equal(f.b.reserveServerJobs({limit:1,globalLimit:2}).length,1);assert.equal(other.reserveServerJobs({limit:8,globalLimit:2}).length,1);assert.deepEqual(f.b.reserveServerJobs({limit:8,globalLimit:2}),[]);assert.equal(f.b.db.prepare("SELECT count(*) n FROM server_jobs WHERE state='running'").get().n,2);
});
test('expired pre-send processing claims recover with a new fence after restart',t=>{
  const f=fixture(t),e=f.event(),reserved=f.b.reserveServerJobs()[0],old=f.b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true});assert.equal(old.claimed,true);f.advance(PROCESSING_LEASE_MS+1);f.restart();
  const recovered=f.b.reserveServerJobs()[0];assert.equal(recovered.recover,true);const current=f.b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true,recover:true});assert.equal(current.generation,2);assert.throws(()=>f.b.assertClaim(SERVER_WORKER_OWNER,e.eventId,{claimToken:old.claim_token}),x=>x.reason==='processing_claim_fence');assert.notEqual(reserved.job_token,recovered.job_token);
});
test('server processing deferral requeues its durable job without callback subscription',async t=>{
  const f=fixture(t),e=f.event();let runs=0;const worker=f.worker({runnerFactory:fakeFactory(async(o,id,claim)=>{
    runs++;if(runs===1){const recovery=o.bridge.deferProcessing(o.owner,id,{claim_token:claim.claim_token,reason:'temporary_tool_failure'});return {category:'deferred',reason:'temporary_tool_failure',recovery};}
  })}).start({poll:false});await worker.idle();assert.equal(runs,1);assert.equal(job(f.b,e.eventId).state,'pending');assert.equal(f.b.db.prepare('SELECT count(*) n FROM deliveries').get().n,0);
  worker.wake();await worker.idle();assert.equal(runs,1);f.advance(1000);worker.wake();await worker.idle();assert.equal(runs,2);assert.equal(job(f.b,e.eventId).state,'completed');
});
test('crashed consumed send is never reprocessed, even after lease expiry and restart',t=>{
  const f=fixture(t),e=f.event();f.b.reserveServerJobs();const claim=f.b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true});f.b.withClaim(SERVER_WORKER_OWNER,e.eventId,claim.claim_token,()=>f.b.reserveProcessingSend(SERVER_WORKER_OWNER,e.eventId));f.advance(PROCESSING_LEASE_MS+1);f.restart();
  assert.deepEqual(f.b.reserveServerJobs(),[]);assert.equal(job(f.b,e.eventId).state,'needs_review');assert.equal(job(f.b,e.eventId).reason,'consumed_send_requires_reconciliation');
});
test('actual runner sends one answer on the server and uncertain sends stay consumed',async t=>{
  for(const uncertain of [false,true]){
    const f=fixture(t),e=f.event();let sends=0;const adapters={...f.adapters,send:async()=>{sends++;if(uncertain)throw new Error('offline provider timeout');return {messageId:'out_1',conversationId:THREAD};}},worker=f.worker({adapters}).start({poll:false});await worker.idle();
    assert.equal(sends,1);assert.equal(job(f.b,e.eventId).state,uncertain?'needs_review':'completed');await worker.stop();f.advance(PROCESSING_LEASE_MS+1);f.restart();const next=f.worker({adapters}).start({poll:false});await next.idle();assert.equal(sends,1);assert.equal(f.b.prospectV1.rows(CONTACT)[0].phase,uncertain?'attempt_unknown':'sent');
  }
});

test('verified queued and known-ID verification delays await reconciliation without false failure alerts',async t=>{
 for(const pending of ['queued','not_visible']){
  const f=fixture(t),e=f.event();let sends=0;
  const adapters={...f.adapters,send:async()=>{sends++;return {messageId:'out_1',conversationId:THREAD};},verify:async()=>{
   if(pending==='not_visible')throw Object.assign(new Error('not visible'),{reason:'ghl_exact_message_not_found'});
   const r=await f.adapters.verify();return {...r,outcome:'queued',outbound:{...r.outbound,status:'queued'}};
  }};
  const worker=f.worker({adapters}).start({poll:false});await worker.idle();
  assert.equal(job(f.b,e.eventId).state,'reconciliation_pending');assert.equal(f.b.listServerAlerts(OPERATOR).alerts.length,0);assert.equal(sends,1);
  assert.equal(f.b.prospectV1.rows(CONTACT)[0].outboundId,'out_1');await worker.stop();f.advance(PROCESSING_LEASE_MS+1);f.restart();
  assert.deepEqual(f.b.reserveServerJobs(),[]);assert.equal(job(f.b,e.eventId).state,'reconciliation_pending');assert.equal(sends,1);
 }
});

test('later same-contact inbound waits for reconciliation instead of exhausting retries before provider visibility',async t=>{
 const f=fixture(t),e=f.event();const adapters={...f.adapters,verify:async()=>{const r=await f.adapters.verify();return {...r,outcome:'queued',outbound:{...r.outbound,status:'queued'}};}};
 const worker=f.worker({adapters}).start({poll:false});await worker.idle();await worker.stop();f.advance(1000);const later=f.event('later_question');
 assert.deepEqual(f.b.reserveServerJobs(),[]);assert.equal(job(f.b,later.eventId).attempts,0);assert.equal(job(f.b,later.eventId).state,'pending');
 assert.throws(()=>f.b.settleReconciliationJob(e.eventId,{outcome:'sent',reason:'verified_sent'}),x=>x.reason==='reconciliation_settlement_mismatch');
 const row=f.b.prospectV1.rows(CONTACT)[0],result=await f.adapters.verify();
 recordReconciledProspectSend(f.b,{eventId:e.eventId,attemptId:row.attemptId,outboundId:'out_1',result});
 const settled=f.b.settleReconciliationJob(e.eventId,{outcome:'sent',reason:'verified_sent'});assert.equal(settled.state,'completed');assert.equal(f.b.listServerAlerts(OPERATOR).alerts.length,0);
 assert.deepEqual(f.b.reserveServerJobs().map(x=>x.event_id),[later.eventId]);
 assert.equal(f.b.settleReconciliationJob(e.eventId,{outcome:'exhausted',reason:'late_stale_result'}).recorded,false);assert.equal(job(f.b,e.eventId).state,'completed');
});

test('terminal reconciliation failure becomes one technical alert while provider success completes quietly',async t=>{
 for(const outcome of ['delivered','failed','exhausted','window_elapsed','validation_failure']){
  const f=fixture(t),e=f.event(),adapters={...f.adapters,verify:async()=>{const r=await f.adapters.verify();return {...r,outcome:'queued',outbound:{...r.outbound,status:'queued'}};}};
  const worker=f.worker({adapters}).start({poll:false});await worker.idle();await worker.stop();
  if(['delivered','failed'].includes(outcome)){
   const row=f.b.prospectV1.rows(CONTACT)[0],result=await f.adapters.verify();result.outcome=outcome;result.outbound.status=outcome;
   recordReconciledProspectSend(f.b,{eventId:e.eventId,attemptId:row.attemptId,outboundId:'out_1',result});
  }
  const result=f.b.settleReconciliationJob(e.eventId,{outcome,reason:'reconciliation_'+outcome});
  assert.equal(result.state,outcome==='delivered'?'completed':'needs_review');const alerts=f.b.listServerAlerts(OPERATOR).alerts;
  assert.equal(alerts.length,outcome==='delivered'?0:1);if(alerts.length){assert.equal(alerts[0].kind,'technical_failure');assert.equal(alerts[0].reason,'reconciliation_'+outcome);}
 }
});
test('server handoff is durable and owner-readable without pretending it notified a person',async t=>{
  const f=fixture(t),e=f.event(),adapters={...f.adapters,generate:async()=>({finishReason:'stop',complete:true,handoff:{reason:'owner_requested',decision_needed:'Cole to answer the question.'}})};
  const worker=f.worker({adapters}).start({poll:false});await worker.idle();assert.equal(job(f.b,e.eventId).state,'needs_review');const listed=f.b.listServerAlerts(OPERATOR);assert.equal(listed.alerts.length,1);assert.equal(listed.alerts[0].notification_status,'stored_not_notified');assert.equal(listed.alerts[0].owner_read_verified,false);assert.equal(listed.alerts[0].exact_question,'What does Google Ads cost?');
  assert.throws(()=>f.b.listServerAlerts('foreign'),x=>x.reason==='server_operator_required');assert.throws(()=>f.b.readServerAlert('foreign',listed.alerts[0].id),x=>x.reason==='server_operator_required');await worker.stop();f.restart();assert.equal(f.b.readServerAlert(OPERATOR,listed.alerts[0].id).notification_status,'stored_not_notified');
});
test('configured notification success or failure updates the stored alert without claiming human delivery',async t=>{
  for(const success of [true,false]){
    const f=fixture(t);f.event();const adapters={...f.adapters,generate:async()=>({finishReason:'stop',complete:true,handoff:{reason:'owner_requested',decision_needed:'Review.'}}),notify:async()=>success?{success:true,notificationId:'actual_action_receipt'}:{success:false,reason:'notification_transport_failed'}},worker=f.worker({adapters}).start({poll:false});await worker.idle();
    const alert=f.b.listServerAlerts(OPERATOR).alerts[0];assert.equal(alert.notification_status,success?'notification_action_succeeded':'notification_failed');assert.equal(alert.owner_read_verified,false);assert.equal(alert.push_delivery_verified,false);if(success)assert.equal(alert.notification_receipt.id,'actual_action_receipt');else assert.equal(alert.notification_failure_reason,'notification_transport_failed');
  }
});
test('server deferral exhaustion is bounded and does not spin or create send attempts',async t=>{
  const f=fixture(t),e=f.event();let runs=0;const worker=f.worker({runnerFactory:fakeFactory(async(o,id,claim)=>{runs++;const recovery=o.bridge.deferProcessing(o.owner,id,{claim_token:claim.claim_token,reason:'temporary_tool_failure'});return {category:'deferred',reason:'temporary_tool_failure',recovery};})}).start({poll:false});
  await worker.idle();for(let i=0;i<4;i++){f.advance(1000);worker.wake();await worker.idle();}assert.equal(runs,3);assert.equal(job(f.b,e.eventId).state,'failed');assert.equal(job(f.b,e.eventId).reason,'processing_deferral_exhausted');assert.equal(f.b.prospectV1.rows(CONTACT).length,0);
});
test('legacy consumed message evidence blocks recovery even if the processing send marker is absent',t=>{
  const f=fixture(t),e=f.event();f.b.reserveServerJobs();f.b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true});f.b.prospectV1.write('sales_messages',f.b.prospectV1.key(CONTACT,'consumed'),{eventId:e.eventId,contactId:CONTACT,messageId:'consumed',phase:'attempt_unknown',attemptId:'persisted_attempt'});f.advance(PROCESSING_LEASE_MS+1);assert.deepEqual(f.b.reserveServerJobs(),[]);assert.equal(job(f.b,e.eventId).state,'needs_review');
});
test('retention cannot erase a still-active server job claim or its event fence',t=>{
  const f=fixture(t),e=f.event();f.b.reserveServerJobs();f.b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true});f.advance(31*86400000);f.b.cleanup();assert.ok(f.b.db.prepare('SELECT event_id FROM processing WHERE event_id=?').get(e.eventId));assert.ok(f.b.db.prepare('SELECT id FROM events WHERE id=?').get(e.eventId));assert.ok(job(f.b,e.eventId));
});

