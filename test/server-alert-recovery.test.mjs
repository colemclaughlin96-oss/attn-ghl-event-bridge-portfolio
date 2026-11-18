import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,LOCATION,SERVER_WORKER_OWNER,MAX_PROCESSING_LIFETIME_MS} from '../src/bridge.mjs';
import {createRuntime,serverOptions} from '../src/server-runtime.mjs';
import {OWNER_ALERT_EVENT} from '../src/owner-alert-events.mjs';

const OWNER='synthetic_recovery_operator',CUT='2026-10-21T15:00:00.000Z';
function fixture(t){
 let now=Date.parse(CUT)+1000,networkCalls=0,b,runtime;
 const dir=mkdtempSync(path.join(tmpdir(),'attn-alert-recovery-')),callbacks=[];
 const env={PROCESSING_MODE:'server',SERVER_WORKER_ENABLED:'true',SERVER_ALLOWED_CONTACTS:'',PROSPECT_V1_REPLIES_ENABLED:'true',SITE_OWNER_SUBJECT:OWNER,OPENAI_MODEL:'gpt-6-luna',OPENAI_API_KEY:'synthetic_openai_secret',GHL_PRIVATE_INTEGRATION_TOKEN:'synthetic_ghl_secret',SERVER_WORKER_NOT_BEFORE:CUT};
 const opts={dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,85).toString('base64'),enabled:true,processingMode:'server',serverWorkerNotBefore:CUT,operatorOwner:OWNER,now:()=>now,prospectV1:{ingress:true,enabled:true,notBefore:CUT},post:async(url,body)=>{const parsed=JSON.parse(body);callbacks.push(parsed);return {status:200,body:JSON.stringify(parsed.type==='verification'?{challenge:parsed.challenge}:{ok:true})};}};
 const open=()=>{b=new Bridge(opts);runtime=createRuntime({bridge:b,env,options:serverOptions(env),fetchImpl:async()=>{networkCalls++;throw new Error('No GHL/OpenAI request belongs in an alert recovery test');}});};open();
 const f={get b(){return b;},get runtime(){return runtime;},get networkCalls(){return networkCalls;},get callbacks(){return callbacks;},get now(){return now;},advance:ms=>now+=ms,
  event:(id,contact=id)=>b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:contact,direction:'inbound',loopMessageId:id},'loop_workflow').eventId,
  drain:async()=>{runtime.drainAlerts();await runtime.waitForAlerts();},
  restart:async()=>{await runtime.waitForAlerts();await runtime.worker.stop();await runtime.reconciler.stop();b.close();open();},
  subscribe:()=>runtime.ownerAlerts.subscribe(OWNER,{name:OWNER_ALERT_EVENT,arguments:{location_id:LOCATION},delivery:{mode:'webhook',url:'https://callback.example.com/recovery',secret:'whsec_'+Buffer.alloc(32,33).toString('base64')}})};
 t.after(async()=>{await runtime.worker.stop();await runtime.reconciler.stop();await runtime.waitForAlerts();b.close();rmSync(dir,{recursive:true,force:true});});return f;
}
const rows=(f,table)=>f.b.db.prepare(`SELECT * FROM ${table} ORDER BY 1`).all();
function storeHandoff(f,eventId){
 const reservation=f.b.reserveServerJobs()[0];assert.equal(reservation.event_id,eventId);
 const claim=f.b.claim(SERVER_WORKER_OWNER,eventId,{fenced:true}),handoff=f.b.withClaim(SERVER_WORKER_OWNER,eventId,claim.claim_token,()=>f.b.prospectV1.handoff(SERVER_WORKER_OWNER,eventId,{reason:'custom_terms',decision_needed:'Cole must decide the proposed terms.'}));
 const saved=f.b.persistServerAlert(SERVER_WORKER_OWNER,handoff.alert);
 f.b.processed(SERVER_WORKER_OWNER,eventId,'escalated',{claim_token:claim.claim_token});f.b.finishServerJob(eventId,reservation.job_token,{category:'handoff',reason:'handoff_queued'});return saved.alert_id;
}

test('restart recovery failures each produce one technical owner alert without retrying a customer job',async t=>{
 const f=fixture(t),consumed=f.event('crashed_consumed'),expired=f.event('expired_presend');
 const reservations=f.b.reserveServerJobs({limit:2});assert.equal(reservations.length,2);
 for(const id of [consumed,expired]){
  const claim=f.b.claim(SERVER_WORKER_OWNER,id,{fenced:true});
  if(id===consumed)f.b.withClaim(SERVER_WORKER_OWNER,id,claim.claim_token,()=>f.b.reserveProcessingSend(SERVER_WORKER_OWNER,id));
 }
 f.advance(MAX_PROCESSING_LIFETIME_MS+1);await f.restart();f.b.refreshServerJobs();
 const jobs=rows(f,'server_jobs');assert.equal(jobs.find(j=>j.event_id===consumed).state,'needs_review');assert.equal(jobs.find(j=>j.event_id===expired).state,'failed');
 const claims=rows(f,'processing');await f.subscribe();f.runtime.drainAlerts();f.runtime.drainAlerts();await f.runtime.waitForAlerts();await f.drain();
 assert.equal(rows(f,'server_alerts').length,2);assert.equal(rows(f,'owner_alert_receipts').length,2);assert.equal(f.callbacks.filter(c=>c.name===OWNER_ALERT_EVENT).length,2);
 assert.ok(f.runtime.ownerAlerts.list(OWNER).alerts.every(a=>a.kind==='technical_failure'&&a.callback_accepted&&!a.owner_read_verified));assert.deepEqual(rows(f,'processing'),claims);assert.deepEqual(rows(f,'server_jobs'),jobs);
 await f.restart();await f.drain();assert.equal(rows(f,'server_alerts').length,2);assert.equal(rows(f,'owner_alert_receipts').length,2);assert.equal(f.callbacks.filter(c=>c.name===OWNER_ALERT_EVENT).length,2);assert.equal(f.networkCalls,0);
});

test('an interrupted handoff enqueue is recovered as the original handoff, not overwritten by a technical alert',async t=>{
 const f=fixture(t),eventId=f.event('business_handoff'),alertId=storeHandoff(f,eventId),before=rows(f,'server_alerts')[0].payload;
 assert.equal(rows(f,'owner_alert_receipts').length,0);await f.restart();await f.subscribe();await f.drain();await f.drain();
 const receipts=rows(f,'owner_alert_receipts');assert.equal(receipts.length,1);assert.equal(receipts[0].alert_id,alertId);assert.equal(receipts[0].kind,'handoff');assert.equal(rows(f,'server_alerts')[0].payload,before);assert.equal(f.callbacks.filter(c=>c.name===OWNER_ALERT_EVENT).length,1);
 const saved=f.b.readServerAlert(OWNER,alertId);assert.equal(saved.reason,'custom_terms');assert.equal(saved.notification_status,'stored_not_notified');assert.equal(f.networkCalls,0);
});

test('cleanup preserves old alert source events and claim evidence so owner read/ack still work',async t=>{
 const f=fixture(t),kept=f.event('retained_handoff'),alertId=storeHandoff(f,kept);await f.drain();
 const source=f.b.db.prepare('SELECT * FROM events WHERE id=?').get(kept),claim=f.b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(kept);
 const discarded=f.event('old_unreferenced'),reservation=f.b.reserveServerJobs()[0],claimed=f.b.claim(SERVER_WORKER_OWNER,discarded,{fenced:true});
 f.b.processed(SERVER_WORKER_OWNER,discarded,'observed',{claim_token:claimed.claim_token});f.b.finishServerJob(discarded,reservation.job_token,{category:'suppressed',reason:'no_pending_inbound'});
 f.advance(31*86400000);f.b.cleanup();
 assert.deepEqual(f.b.db.prepare('SELECT * FROM events WHERE id=?').get(kept),source);assert.deepEqual(f.b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(kept),claim);assert.equal(f.b.db.prepare('SELECT id FROM events WHERE id=?').get(discarded),undefined);
 const read=f.runtime.ownerAlerts.read(OWNER,alertId);assert.equal(read.alert_id,alertId);const ack=f.runtime.ownerAlerts.ack(OWNER,alertId);assert.equal(ack.consumer_acknowledged,true);assert.equal(ack.owner_read_verified,false);assert.equal(f.b.readServerAlert(OWNER,alertId).reason,'custom_terms');assert.equal(f.networkCalls,0);
});

test('alert drain wait joins an in-flight owner callback before the database can be closed',async t=>{
 const f=fixture(t),eventId=f.event('slow_owner_callback');storeHandoff(f,eventId);await f.subscribe();let release,started;
 const began=new Promise(resolve=>started=resolve);f.b.post=async()=>{started();await new Promise(resolve=>release=resolve);return {status:200,body:'{}'};};
 f.runtime.drainAlerts();await began;let finished=false;const waiting=f.runtime.waitForAlerts().then(()=>finished=true);await Promise.resolve();assert.equal(finished,false);release();await waiting;assert.equal(finished,true);assert.equal(rows(f,'owner_alert_deliveries')[0].status,'accepted');
});

