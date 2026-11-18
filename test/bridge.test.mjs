import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,LOCATION,EVENT,PROCESSING_LEASE_MS,MAX_PROCESSING_RECOVERIES,MAX_PROCESSING_LIFETIME_MS} from '../src/bridge.mjs';
import {rpc,createServer} from '../src/server.mjs';

const OWNER='site-owner-fixture',CONTACT='owner_test_contact_fixture';
const SECRET='whsec_'+Buffer.alloc(32,7).toString('base64');
function verifyStandard(headers,body,secret=SECRET) {
  const expected=crypto.createHmac('sha256',Buffer.from(secret.slice(6),'base64')).update(`${headers['webhook-id']}.${headers['webhook-timestamp']}.${body}`).digest('base64');
  assert.ok(headers['webhook-signature'].split(' ').includes('v1,'+expected));
}
function fixture(t) {
  const folder=mkdtempSync(path.join(tmpdir(),'attn-test-'));
  let now=Date.parse('2026-10-20T18:00:00Z'),answer=200;
  const calls=[];
  const opts={dbPath:path.join(folder,'test.sqlite'),vaultKey:Buffer.alloc(32,8).toString('base64'),testContactId:CONTACT,now:()=>now,post:async(url,body,headers)=>{
    calls.push({url,body:JSON.parse(body),raw:body,headers});
    const value=JSON.parse(body);
    return {status:answer,body:JSON.stringify(value.type==='verification'?{challenge:value.challenge}:{ok:true})};
  }};
  let bridge=new Bridge(opts);
  const handles=new Set([bridge]);
  const f={get bridge(){return bridge;},calls,opts,advance:n=>now+=n,setAnswer:n=>answer=n,restart:()=>{bridge.close();handles.delete(bridge);bridge=new Bridge(opts);handles.add(bridge);},fork:()=>{const other=new Bridge(opts);handles.add(other);return other;},
    params:(extra={})=>({name:EVENT,arguments:{location_id:LOCATION,contact_id:CONTACT},delivery:{mode:'webhook',url:'https://callback.example.com/incoming',secret:SECRET},cursor:null,...extra}),
    event:(extra={})=>({type:'InboundMessage',locationId:LOCATION,contactId:CONTACT,conversationId:'conversation_fixture',messageId:'message_fixture',direction:'inbound',messageTypeString:'TYPE_CUSTOM_PROVIDER_SMS',conversationProviderId:'loop_fixture',dateAdded:new Date(now).toISOString(),body:'PRIVATE CUSTOMER MESSAGE DO NOT STORE',phone:'+15555550123',...extra})};
  t.after(()=>{for(const handle of handles)handle.close();rmSync(folder,{recursive:true,force:true});});return f;
}
test('discovery advertises MCP 2.0 events and minimal payload schema',async t=>{
  const f=fixture(t);const result=await rpc(f.bridge,OWNER,{jsonrpc:'2.0',id:1,method:'server/discover'});
  assert.deepEqual(result.result.supportedVersions,['2026-07-28']);assert.ok(result.result.capabilities.events);
  assert.equal(f.bridge.catalog()[0].payloadSchema.properties.body,undefined);
});
test('subscribe verifies signed callback and deduplicates canonical/concurrent requests',async t=>{
  const f=fixture(t),p=f.params();
  const [a,b]=await Promise.all([f.bridge.subscribe(OWNER,p),f.bridge.subscribe(OWNER,{...p,arguments:{contact_id:CONTACT,location_id:LOCATION}})]);
  assert.equal(a.id,b.id);assert.equal(f.calls.length,1);verifyStandard(f.calls[0].headers,f.calls[0].raw);
  assert.equal(f.bridge.status(OWNER).active_subscriptions,1);
});
test('failed or redirected callback cannot create subscription',async t=>{
  const f=fixture(t);f.setAnswer(302);
  await assert.rejects(f.bridge.subscribe(OWNER,f.params()),e=>e.code===-32015&&e.reason==='challenge_failed');
  assert.equal(f.bridge.status(OWNER).active_subscriptions,0);
});
test('wrong challenge and timeout produce categorized failures',async t=>{
  const f=fixture(t);f.bridge.post=async()=>({status:200,body:'{"challenge":"wrong"}'});
  await assert.rejects(f.bridge.subscribe(OWNER,f.params()),e=>e.code===-32015);
  f.bridge.post=async()=>{throw new Error('timeout');};
  await assert.rejects(f.bridge.subscribe(OWNER,f.params()),e=>e.reason==='timeout');
});
test('scope, argument, secret and replay checks fail closed',async t=>{
  const f=fixture(t);
  for(const p of [f.params({name:'other'}),f.params({arguments:{location_id:'other',contact_id:CONTACT}}),f.params({arguments:{location_id:LOCATION,contact_id:'prospect'}}),f.params({arguments:{location_id:LOCATION,contact_id:CONTACT,extra:true}}),f.params({cursor:'unimplemented-replay'}),f.params({delivery:{mode:'webhook',url:'https://callback.example.com/',secret:'whsec_a'}})])
    await assert.rejects(f.bridge.subscribe(OWNER,p));
  assert.equal(f.calls.length,0);
});
test('new events are durable; duplicates across restarts are not re-enqueued',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());
  const a=f.bridge.ingest(f.event(),'ghl_signed');assert.equal(a.duplicate,false);
  f.restart();const b=f.bridge.ingest(f.event(),'ghl_workflow');assert.equal(b.duplicate,true);assert.equal(a.eventId,b.eventId);
  await f.bridge.drain();assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,1);
});
test('business content, phone and callbacks are absent from persisted plaintext',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());f.bridge.ingest(f.event(),'ghl_signed');
  const rows=JSON.stringify([f.bridge.db.prepare('SELECT * FROM subscriptions').all(),f.bridge.db.prepare('SELECT * FROM events').all(),f.bridge.db.prepare('SELECT * FROM audit').all()]);
  for(const forbidden of ['PRIVATE CUSTOMER','+15555550123','https://callback',SECRET,'loop_fixture'])assert.equal(rows.includes(forbidden),false);
});
test('non-ATTN, other contact, outbound, calls and reactions are ignored',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());
  for(const patch of [{locationId:'client_account'},{contactId:'prospect'},{direction:'outbound'},{messageTypeString:'TYPE_CALL'},{messageTypeString:'TYPE_SMS_REACTION'}])assert.equal(f.bridge.ingest(f.event(patch),'ghl_signed').accepted,false);
  assert.equal(f.bridge.db.prepare('SELECT count(*) n FROM events').get().n,0);
});
test('missing identifiers, stale events and future events are rejected',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());
  for(const patch of [{messageId:undefined},{dateAdded:'2020-01-01T00:00:00Z'},{dateAdded:'2030-01-01T00:00:00Z'}])assert.throws(()=>f.bridge.ingest(f.event(patch),'ghl_signed'));
});
test('ingress without an active subscription returns a retryable failure',t=>{
  const f=fixture(t);assert.equal(f.bridge.ingest(f.event(),'ghl_workflow').status,503);
});
test('transient retries keep event ID/body and refresh signing time',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());f.bridge.ingest(f.event(),'ghl_signed');f.setAnswer(503);await f.bridge.drain();
  f.advance(60000);f.setAnswer(200);await f.bridge.drain();
  const [a,b]=f.calls.filter(c=>c.body.name===EVENT);assert.equal(a.raw,b.raw);assert.notEqual(a.headers['webhook-timestamp'],b.headers['webhook-timestamp']);
  verifyStandard(a.headers,a.raw);verifyStandard(b.headers,b.raw);
  assert.equal(f.bridge.status(OWNER).deliveries[0].status,'accepted');
});
test('410 and 413 terminate delivery without retry',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());f.bridge.ingest(f.event(),'ghl_signed');f.setAnswer(413);await f.bridge.drain();f.advance(60000);await f.bridge.drain();
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,1);
  f.bridge.ingest(f.event({messageId:'message_second'}),'ghl_signed');f.setAnswer(410);await f.bridge.drain();assert.equal(f.bridge.status(OWNER).active_subscriptions,0);
});
test('retry count is bounded at eight',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());f.bridge.ingest(f.event(),'ghl_signed');f.setAnswer(503);
  for(let i=0;i<10;i++){await f.bridge.drain();f.advance(128500);}
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,8);assert.equal(f.bridge.status(OWNER).deliveries[0].status,'dead');
});
test('subscription expiry stops pending delivery, renewal persists',async t=>{
  const f=fixture(t);const sub=await f.bridge.subscribe(OWNER,f.params({ttlMs:60000}));f.bridge.ingest(f.event(),'ghl_signed');f.advance(61000);await f.bridge.drain();
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,0);
  const renew=await f.bridge.subscribe(OWNER,f.params());assert.equal(renew.id,sub.id);f.restart();assert.equal(f.bridge.status(OWNER).active_subscriptions,1);
});
test('rotation signs with both keys briefly and re-verifies',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());
  const replacement='whsec_'+Buffer.alloc(32,9).toString('base64');const p=f.params();p.delivery.secret=replacement;await f.bridge.subscribe(OWNER,p);
  f.bridge.ingest(f.event(),'ghl_signed');await f.bridge.drain();const first=f.calls.at(-1);assert.equal(first.headers['webhook-signature'].split(' ').length,2);verifyStandard(first.headers,first.raw,replacement);verifyStandard(first.headers,first.raw,SECRET);
  f.advance(300001);f.bridge.ingest(f.event({messageId:'after_rotation'}),'ghl_signed');await f.bridge.drain();assert.equal(f.calls.at(-1).headers['webhook-signature'].split(' ').length,1);
});
test('unsubscribe is idempotent, authorized by principal and cancels pending delivery',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());f.bridge.ingest(f.event(),'ghl_signed');
  await f.bridge.unsubscribe('different_owner',f.params());assert.equal(f.bridge.status(OWNER).active_subscriptions,1);
  await f.bridge.unsubscribe(OWNER,f.params());await f.bridge.unsubscribe(OWNER,f.params());await f.bridge.drain();assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,0);
});
test('HTTP acknowledgement does not count as chat processing; claims prevent repeat handling',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');await f.bridge.drain();
  assert.deepEqual(f.bridge.status(OWNER).chat_acknowledgements,[]);
  assert.equal(f.bridge.claim(OWNER,eventId).claimed,true);f.restart();assert.equal(f.bridge.claim(OWNER,eventId).claimed,false);
  assert.throws(()=>f.bridge.processed('different_owner',eventId,'observed'));
  f.bridge.processed(OWNER,eventId,'observed');assert.equal(f.bridge.status(OWNER).chat_acknowledgements[0].n,1);
  assert.equal(f.bridge.processed(OWNER,eventId,'test_reply_sent').outcome,'observed');
});
test('global pause blocks notifications and delivery',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());f.bridge.ingest(f.event(),'ghl_signed');f.bridge.enabled=false;await f.bridge.drain();
  assert.equal(f.bridge.ingest(f.event(),'ghl_signed').status,503);assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,0);
});

test('expired pre-send claim recovers with a new fence and preserves completed outcomes',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');
  const first=f.bridge.claim(OWNER,eventId,{fenced:true});assert.equal(first.generation,1);
  assert.equal(f.bridge.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'claim_active');
  assert.throws(()=>f.bridge.withClaim(OWNER,eventId,undefined,()=>{}),e=>e.reason==='processing_claim_fence');
  f.advance(PROCESSING_LEASE_MS);f.restart();
  assert.throws(()=>f.bridge.withClaim(OWNER,eventId,first.claim_token,()=>{}),e=>e.reason==='processing_claim_expired');
  const second=f.bridge.claim(OWNER,eventId,{fenced:true,recover:true});assert.equal(second.claimed,true);assert.equal(second.generation,2);assert.notEqual(second.claim_token,first.claim_token);
  assert.throws(()=>f.bridge.processed(OWNER,eventId,'observed',{claim_token:first.claim_token}),e=>e.reason==='processing_claim_fence');
  assert.throws(()=>f.bridge.withClaim(OWNER,eventId,first.claim_token,()=>f.bridge.reserveProcessingSend(OWNER,eventId)),e=>e.reason==='processing_claim_fence');
  f.bridge.withClaim(OWNER,eventId,second.claim_token,()=>assert.equal(f.bridge.assertClaim(OWNER,eventId).generation,2));
  f.bridge.processed(OWNER,eventId,'skipped',{claim_token:second.claim_token,stage:'suppressed',reason:'manual_takeover_24h'});
  assert.equal(f.bridge.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'already_processed');
  assert.equal(f.bridge.processed(OWNER,eventId,'observed',{claim_token:second.claim_token}).outcome,'skipped');
});

test('recovery is bounded, atomic across connections, and unavailable to legacy claims',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');
  f.bridge.claim(OWNER,eventId,{fenced:true});const other=f.fork();
  for(let i=0;i<MAX_PROCESSING_RECOVERIES;i++){
    f.advance(PROCESSING_LEASE_MS+1);assert.equal(f.bridge.claim(OWNER,eventId,{fenced:true,recover:true}).claimed,true);
    assert.equal(other.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'claim_active');
  }
  f.advance(PROCESSING_LEASE_MS+1);assert.equal(other.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'claim_recovery_limit');
  const old=f.bridge.ingest(f.event({messageId:'legacy_claim'}),'ghl_signed');f.bridge.claim(OWNER,old.eventId);f.advance(PROCESSING_LEASE_MS+1);
  assert.equal(other.claim(OWNER,old.eventId,{fenced:true,recover:true}).reason,'legacy_claim_not_recoverable');
  assert.equal(f.bridge.processed(OWNER,old.eventId,'observed').recorded,true);
});

test('reserved or legacy consumed sends never recover and expiry never permits another send',async t=>{
  for(const phase of ['marker','attempt_unknown','queued','sent','delivered','failed']){
    const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed'),claim=f.bridge.claim(OWNER,eventId,{fenced:true});
    if(phase==='marker')f.bridge.withClaim(OWNER,eventId,claim.claim_token,()=>{f.bridge.db.exec('BEGIN IMMEDIATE');try{f.bridge.reserveProcessingSend(OWNER,eventId);f.bridge.db.exec('COMMIT');}catch(e){f.bridge.db.exec('ROLLBACK');throw e;}});
    else f.bridge.prospectV1.write('sales_messages','synthetic_consumed_'+phase,{eventId,contactId:CONTACT,phase,attemptId:'preserved_attempt'});
    f.advance(PROCESSING_LEASE_MS);f.restart();assert.equal(f.bridge.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'send_attempt_consumed');
    assert.throws(()=>f.bridge.withClaim(OWNER,eventId,claim.claim_token,()=>f.bridge.reserveProcessingSend(OWNER,eventId),{allowExpired:true}),e=>e.reason==='processing_claim_expired');
    assert.equal(f.bridge.processed(OWNER,eventId,'escalated',{claim_token:claim.claim_token,stage:'failed',reason:'send_result_unknown'}).recorded,true);
  }
});

test('stage diagnostics persist sanitized reasons without exposing claim tokens or message data',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed'),claim=f.bridge.claim(OWNER,eventId,{fenced:true});
  f.bridge.recordStage(OWNER,eventId,{claim_token:claim.claim_token,stage:'generation',reason:'draft_started'});
  assert.throws(()=>f.bridge.recordStage(OWNER,eventId,{claim_token:claim.claim_token,stage:'failed',reason:'Private message +15555550123'}),e=>e.reason==='invalid_processing_diagnostic');
  f.restart();const status=f.bridge.status(OWNER);assert.equal(status.recent_processing[0].reason,'draft_started');
  assert.deepEqual(f.bridge.status('different_owner').recent_processing,[]);
  const stored=JSON.stringify([f.bridge.db.prepare('SELECT * FROM processing').all(),status]);
  for(const text of [claim.claim_token,'PRIVATE CUSTOMER MESSAGE','+15555550123'])assert.equal(stored.includes(text),false);
});

test('recovery deadline prevents reviving an old event and is never extended',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');f.bridge.claim(OWNER,eventId,{fenced:true});
  const original=f.bridge.db.prepare('SELECT recovery_deadline FROM processing WHERE event_id=?').get(eventId).recovery_deadline;
  f.advance(PROCESSING_LEASE_MS);f.bridge.claim(OWNER,eventId,{fenced:true,recover:true});
  assert.equal(f.bridge.db.prepare('SELECT recovery_deadline FROM processing WHERE event_id=?').get(eventId).recovery_deadline,original);
  f.advance(MAX_PROCESSING_LIFETIME_MS-PROCESSING_LEASE_MS);f.restart();
  assert.equal(f.bridge.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'claim_recovery_window_elapsed');
  assert.equal(f.bridge.db.prepare('SELECT generation FROM processing WHERE event_id=?').get(eventId).generation,2);
});

test('pre-send deferral reuses durable delivery and fences the old run until bounded recovery',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');await f.bridge.drain();
  let claim=f.bridge.claim(OWNER,eventId,{fenced:true});
  for(let n=1;n<=2;n++){
    const deferred=f.bridge.deferProcessing(OWNER,eventId,{claim_token:claim.claim_token,reason:'inbound_not_visible',retryAfterMs:1000});assert.equal(deferred.retry_number,n);
    assert.throws(()=>f.bridge.processed(OWNER,eventId,'observed',{claim_token:claim.claim_token}),e=>e.reason==='processing_deferred_requires_recovery');
    assert.throws(()=>f.bridge.withClaim(OWNER,eventId,claim.claim_token,()=>f.bridge.reserveProcessingSend(OWNER,eventId)),e=>e.reason==='processing_claim_expired');
    assert.equal(f.bridge.claim(OWNER,eventId,{fenced:true,recover:true}).reason,'processing_deferred_until');
    f.restart();f.advance(1000);await f.bridge.drain();claim=f.bridge.claim(OWNER,eventId,{fenced:true,recover:true});assert.equal(claim.claimed,true);
  }
  const exhausted=f.bridge.deferProcessing(OWNER,eventId,{claim_token:claim.claim_token,reason:'inbound_not_visible'});
  assert.equal(exhausted.reason,'processing_deferral_exhausted');assert.equal(exhausted.acknowledged,true);
  assert.equal(f.bridge.db.prepare('SELECT count(*) n FROM subscriptions').get().n,1);
  assert.equal(f.bridge.db.prepare('SELECT count(*) n FROM deliveries').get().n,1);
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,3);
  assert.equal(f.bridge.status(OWNER).recent_processing[0].stage,'failed');
});

test('deferral refuses consumed attempts and survives a callback acceptance race',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed'),claim=f.bridge.claim(OWNER,eventId,{fenced:true});
  f.bridge.post=async()=>{f.bridge.deferProcessing(OWNER,eventId,{claim_token:claim.claim_token,reason:'thread_changed'});return {status:200,body:'{}'};};
  await f.bridge.drain();assert.equal(f.bridge.db.prepare('SELECT status FROM deliveries WHERE event_id=?').get(eventId).status,'pending');
  f.advance(1000);const recovered=f.bridge.claim(OWNER,eventId,{fenced:true,recover:true});
  f.bridge.withClaim(OWNER,eventId,recovered.claim_token,()=>f.bridge.reserveProcessingSend(OWNER,eventId));
  assert.throws(()=>f.bridge.deferProcessing(OWNER,eventId,{claim_token:recovered.claim_token,reason:'thread_changed'}),e=>e.reason==='send_attempt_consumed');
  assert.equal(f.bridge.db.prepare('SELECT completed FROM processing WHERE event_id=?').get(eventId).completed,null);
});

test('existing drain requeues an accepted callback after a claimed worker crashes before send',async t=>{
  const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');await f.bridge.drain();
  const first=f.bridge.claim(OWNER,eventId,{fenced:true});f.advance(PROCESSING_LEASE_MS);f.restart();await f.bridge.drain();
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,2);
  assert.equal(f.bridge.status(OWNER).recent_processing[0].reason,'expired_pre_send_claim');
  assert.throws(()=>f.bridge.processed(OWNER,eventId,'observed',{claim_token:first.claim_token}),e=>e.reason==='processing_deferred_requires_recovery');
  const recovered=f.bridge.claim(OWNER,eventId,{fenced:true,recover:true});assert.equal(recovered.claimed,true);assert.equal(recovered.generation,2);
  f.bridge.processed(OWNER,eventId,'observed',{claim_token:recovered.claim_token});f.advance(PROCESSING_LEASE_MS);await f.bridge.drain();
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,2);
});

test('crash watchdog never requeues a consumed attempt and bounds an unresponsive native consumer',async t=>{
  for(const consumed of [false,true]){
    const f=fixture(t);await f.bridge.subscribe(OWNER,f.params());const {eventId}=f.bridge.ingest(f.event(),'ghl_signed');await f.bridge.drain();
    const claim=f.bridge.claim(OWNER,eventId,{fenced:true});
    if(consumed)f.bridge.withClaim(OWNER,eventId,claim.claim_token,()=>f.bridge.reserveProcessingSend(OWNER,eventId));
    for(let i=0;i<4;i++){f.advance(PROCESSING_LEASE_MS);await f.bridge.drain();}
    assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,consumed?1:3);
    const row=f.bridge.db.prepare('SELECT outcome,completed,deferrals FROM processing WHERE event_id=?').get(eventId);
    if(consumed){assert.equal(row.completed,null);assert.equal(row.deferrals,0);}
    else {assert.equal(row.outcome,'escalated');assert.equal(row.deferrals,2);assert.equal(f.bridge.status(OWNER).recent_processing[0].reason,'processing_recovery_exhausted');}
  }
});
test('HTTP authentication, Ed25519 ingress and workflow shared secret are independent',async t=>{
  const f=fixture(t),keys=crypto.generateKeyPairSync('ed25519');
  const server=createServer({bridge:f.bridge,proxyToken:'p'.repeat(40),workflowToken:'w'.repeat(40),proxyPrincipal:OWNER,publicKey:keys.publicKey});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>new Promise(r=>server.close(r)));
  const base=`http://127.0.0.1:${server.address().port}`;
  const request=(route,body,headers={})=>fetch(base+route,{method:'POST',headers:{'content-type':'application/json',...headers},body:JSON.stringify(body)});
  assert.equal((await request('/mcp',{jsonrpc:'2.0',id:1,method:'server/discover'})).status,401);
  assert.equal((await request('/mcp',{}, {authorization:'Bearer '+'p'.repeat(40),'x-attn-principal':'wrong'})).status,401);
  const validHeaders={authorization:'Bearer '+'p'.repeat(40),'x-attn-principal':OWNER};
  assert.equal((await request('/mcp',{jsonrpc:'2.0',id:1,method:'server/discover'},validHeaders)).status,200);
  await f.bridge.subscribe(OWNER,f.params());const input=f.event(),raw=JSON.stringify(input),sig=crypto.sign(null,Buffer.from(raw),keys.privateKey).toString('base64');
  assert.equal((await request('/webhooks/ghl',input,{'x-ghl-signature':sig})).status,200);
  assert.equal((await request('/webhooks/ghl',{...input,body:'tampered'},{'x-ghl-signature':sig})).status,401);
  assert.equal((await request('/webhooks/ghl-workflow',input,{'x-attn-webhook-key':'p'.repeat(40)})).status,401);
  assert.equal((await request('/webhooks/ghl-workflow',input,{'x-attn-webhook-key':'w'.repeat(40)})).status,200);
});


