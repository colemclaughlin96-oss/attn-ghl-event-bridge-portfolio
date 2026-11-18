import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,LOCATION} from '../src/bridge.mjs';
import {OwnerAlertEvents,OWNER_ALERT_EVENT} from '../src/owner-alert-events.mjs';
import {rpc} from '../src/server.mjs';

const OWNER='synthetic_operator',SECRET='whsec_'+Buffer.alloc(32,31).toString('base64');
function verify(call,secret=SECRET){const signature=crypto.createHmac('sha256',Buffer.from(secret.slice(6),'base64')).update(`${call.headers['webhook-id']}.${call.headers['webhook-timestamp']}.${call.raw}`).digest('base64');assert.ok(call.headers['webhook-signature'].split(' ').includes('v1,'+signature));}
function fixture(t){
  const dir=mkdtempSync(path.join(tmpdir(),'attn-owner-events-'));let now=Date.parse('2026-10-21T15:00:00Z'),answer=200,custom=null;const calls=[];
  const opts={dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,32).toString('base64'),now:()=>now,post:async(url,raw,headers)=>{const body=JSON.parse(raw),call={url,raw,body,headers};calls.push(call);return custom?custom(call):{status:answer,body:JSON.stringify(body.type==='verification'?{challenge:body.challenge}:{ok:true})};}};
  let b=new Bridge(opts),events=new OwnerAlertEvents({bridge:b,operatorOwner:OWNER});const extra=[];
  const params=(overrides={})=>({name:OWNER_ALERT_EVENT,arguments:{location_id:LOCATION},delivery:{mode:'webhook',url:'https://callback.example.com/owner',secret:SECRET},cursor:null,...overrides});
  const store=(alertId='alert_one',kind='handoff')=>{const eventId='evt_'+alertId;const at=now;const event={eventId,name:'attn.message.incoming',timestamp:new Date(at).toISOString(),data:{location_id:LOCATION,contact_id:'private_contact',source:'loop_workflow'}};b.db.prepare('INSERT INTO events(id,payload,created) VALUES(?,?,?)').run(eventId,b.vault.seal(event,eventId),at);const payload={event_id:eventId,kind,contact_id:'private_contact',exact_question:'PRIVATE CUSTOMER QUESTION',decision_needed:'PRIVATE DECISION',phone:'+12065551234'};b.db.prepare('INSERT INTO server_alerts(id,event_id,created,updated,payload) VALUES(?,?,?,?,?)').run(alertId,eventId,at,at,b.vault.seal(payload,alertId));return alertId;};
  t.after(()=>{for(const x of extra)x.close();b.close();rmSync(dir,{recursive:true,force:true});});
  return {get b(){return b;},get events(){return events;},get now(){return now;},calls,params,store,advance:n=>now+=n,setAnswer:n=>answer=n,setPost:fn=>custom=fn,restart:()=>{b.close();b=new Bridge(opts);events=new OwnerAlertEvents({bridge:b,operatorOwner:OWNER});},fork:()=>{const other=new Bridge(opts);extra.push(other);return new OwnerAlertEvents({bridge:other,operatorOwner:OWNER});}};
}
const rejects=(fn,expected)=>assert.rejects(fn,e=>e.reason===expected);

test('owner catalog exposes only minimal scoped alert identifiers',t=>{
  const f=fixture(t),event=f.events.catalog()[0];assert.equal(event.name,OWNER_ALERT_EVENT);
  assert.deepEqual(Object.keys(event.payloadSchema.properties),['alert_id','location_id','kind']);assert.deepEqual(event.payloadSchema.properties.kind.enum,['handoff','technical_failure']);
  assert.equal(f.events.status(OWNER).active_subscriptions,0);
});

test('pinned owner, location, kinds and callback scope fail closed before network',async t=>{
  const f=fixture(t);
  await rejects(()=>f.events.subscribe('another_owner',f.params()),'owner_alert_operator_required');
  for(const params of [f.params({name:'attn.message.incoming'}),f.params({arguments:{location_id:'other'}}),f.params({arguments:{location_id:LOCATION,kinds:['unknown']}}),f.params({arguments:{location_id:LOCATION,contact_id:'customer'}}),f.params({cursor:'replay'}),f.params({delivery:{mode:'webhook',url:'http://callback.example.com/',secret:SECRET}}),f.params({delivery:{mode:'webhook',url:'https://127.0.0.1/',secret:SECRET}}),f.params({delivery:{mode:'webhook',url:'https://callback.example.com/',secret:'bad'}})])await assert.rejects(f.events.subscribe(OWNER,params));
  assert.equal(f.calls.length,0);assert.throws(()=>new OwnerAlertEvents({bridge:f.b,operatorOwner:'another_owner'}),e=>e.reason==='owner_alert_operator_immutable');
  assert.throws(()=>f.events.list('another_owner'),e=>e.reason==='owner_alert_operator_required');
});

test('subscription challenges are signed, concurrent renewals deduplicate and TTL is capped at 15 minutes',async t=>{
  const f=fixture(t);const [a,b]=await Promise.all([f.events.subscribe(OWNER,f.params({ttlMs:99999999})),f.events.subscribe(OWNER,f.params())]);
  assert.equal(a.id,b.id);assert.equal(Date.parse(a.refreshBefore)-f.now,900000);assert.equal(f.calls.length,1);verify(f.calls[0]);assert.equal(f.events.status(OWNER).active_subscriptions,1);
  f.advance(300001);await f.events.subscribe(OWNER,f.params());assert.equal(f.calls.length,2);
});

test('MCP metadata is accepted through subscribe/unsubscribe routing without changing subscription identity',async t=>{
  const f=fixture(t);f.b.runtime={ownerAlerts:f.events};
  const meta={protocolVersion:'2026-07-28',clientCapabilities:{events:{}},progressToken:'synthetic_progress'};
  const call=(method,params)=>rpc(f.b,OWNER,{jsonrpc:'2.0',id:1,method,params});
  const first=await call('events/subscribe',f.params({_meta:meta}));assert.equal(first.error,undefined);assert.ok(first.result.id);
  const second=await call('events/subscribe',f.params({_meta:{...meta,progressToken:'different_progress'}}));assert.equal(second.result.id,first.result.id);assert.equal(f.calls.length,1);verify(f.calls[0]);
  assert.equal(f.events.identity(OWNER,f.params()).id,first.result.id);assert.equal(f.events.identity(OWNER,f.params({_meta:Object.create(null)})).id,first.result.id);
  const row=f.b.db.prepare('SELECT state FROM owner_alert_subscriptions WHERE id=?').get(first.result.id),stored=f.b.vault.open(row.state,first.result.id);assert.equal(Object.hasOwn(stored,'_meta'),false);
  const removed=await call('events/unsubscribe',f.params({_meta:meta}));assert.equal(removed.error,undefined);assert.equal(f.events.status(OWNER).active_subscriptions,0);
});

test('malformed metadata and unknown top-level fields fail before network for subscribe and unsubscribe',async t=>{
  const f=fixture(t);
  for(const value of [null,undefined,[],false,12,'metadata',new Date(),Object.create({unexpected:true})]){
    await rejects(()=>f.events.subscribe(OWNER,f.params({_meta:value})),'owner_alert_metadata_invalid');
    await rejects(()=>f.events.unsubscribe(OWNER,f.params({_meta:value})),'owner_alert_metadata_invalid');
  }
  await rejects(()=>f.events.subscribe(OWNER,f.params({_meta:{},unsupported:'PRIVATE_VALUE'})),'owner_alert_event_required');
  await rejects(()=>f.events.unsubscribe(OWNER,f.params({_meta:{},unsupported:'PRIVATE_VALUE'})),'owner_alert_event_required');
  assert.equal(f.calls.length,0);assert.equal(f.events.status(OWNER).active_subscriptions,0);
});

test('forged metadata cannot override trusted owner, location, arguments or callback and never leaks into rejection audit',async t=>{
  const f=fixture(t),meta={owner:OWNER,location_id:LOCATION,arguments:{location_id:LOCATION},delivery:{mode:'webhook',url:'https://PRIVATE_METADATA.example.com/',secret:'PRIVATE_METADATA_SECRET'}};
  await rejects(()=>f.events.subscribe('another_owner',f.params({_meta:meta})),'owner_alert_operator_required');
  await rejects(()=>f.events.subscribe(OWNER,f.params({_meta:meta,arguments:{location_id:'other_location'}})),'owner_alert_location_required');
  await rejects(()=>f.events.subscribe(OWNER,f.params({_meta:meta,delivery:{mode:'webhook',url:'http://PRIVATE_CALLBACK.example.com/',secret:SECRET}})),'invalid_callback');
  const audit=f.b.db.prepare('SELECT action,ref,code FROM audit ORDER BY id').all();
  assert.deepEqual(audit.map(a=>a.action),['owner_alert_subscribe_rejected_owner_alert_operator_required','owner_alert_subscribe_rejected_owner_alert_location_required','owner_alert_subscribe_rejected_invalid_callback']);
  assert.equal(new Set(audit.map(a=>a.ref)).size,1);assert.ok(audit.every(a=>a.code===-32602));assert.equal(JSON.stringify(audit).includes('PRIVATE'),false);assert.equal(JSON.stringify(audit).includes(OWNER),false);assert.equal(f.calls.length,0);
  const actual=await f.events.subscribe(OWNER,f.params({_meta:{owner:'another_owner',location_id:'other_location',delivery:{url:'https://ignored.example.com/'}}}));assert.ok(actual.id);assert.equal(f.calls[0].url,'https://callback.example.com/owner');
});

test('failed challenge cannot replace a previously verified owner destination',async t=>{
  const f=fixture(t);await f.events.subscribe(OWNER,f.params());f.setPost(()=>({status:200,body:'{"challenge":"wrong"}'}));
  await rejects(()=>f.events.subscribe(OWNER,f.params({delivery:{mode:'webhook',url:'https://new.example.com/owner',secret:SECRET}})),'challenge_failed');
  assert.equal(f.events.status(OWNER).active_subscriptions,1);f.setPost(null);f.events.enqueue(f.store(),'handoff');await f.events.drain();assert.equal(f.calls.at(-1).url,'https://callback.example.com/owner');
});

test('alert enqueue is durable without a subscription and a later subscription backfills',async t=>{
  const f=fixture(t),alertId=f.store();const added=f.events.enqueue(alertId,'handoff');assert.equal(added.stored,true);assert.equal(added.success,false);assert.equal(f.calls.length,0);
  f.restart();assert.equal(f.events.status(OWNER).unacknowledged,1);await f.events.subscribe(OWNER,f.params());await f.events.drain();
  const delivery=f.calls.find(c=>c.body.name===OWNER_ALERT_EVENT);assert.ok(delivery);verify(delivery);
  assert.deepEqual(delivery.body.data,{alert_id:alertId,location_id:LOCATION,kind:'handoff'});
  for(const privateText of ['private_contact','PRIVATE CUSTOMER','PRIVATE DECISION','+12065551234'])assert.equal(delivery.raw.includes(privateText),false);
  const meta=f.events.read(OWNER,alertId);assert.equal(meta.callback_accepted,true);assert.equal(meta.consumer_acknowledged_at,null);assert.equal(meta.owner_read_verified,false);
});

test('only a verified existing source alert and matching kind can be enqueued',t=>{
  const f=fixture(t);assert.throws(()=>f.events.enqueue('not_real','handoff'),e=>e.reason==='server_alert_not_found');
  const id=f.store('failure','technical_failure');assert.throws(()=>f.events.enqueue(id,'handoff'),e=>e.reason==='owner_alert_reference_mismatch');
  f.events.enqueue(id,'technical_failure');assert.equal(f.events.list(OWNER).alerts[0].kind,'technical_failure');
  f.b.db.prepare('UPDATE server_alerts SET created=created+1 WHERE id=?').run(id);assert.throws(()=>f.events.enqueue(id,'technical_failure'),e=>e.reason==='owner_alert_reference_immutable');
});

test('source location is checked rather than supplied by enqueue caller',t=>{
  const f=fixture(t),id=f.store(),eventId='evt_'+id,event={eventId,data:{location_id:'other_location'}};
  f.b.db.prepare('UPDATE events SET payload=? WHERE id=?').run(f.b.vault.seal(event,eventId),eventId);
  assert.throws(()=>f.events.enqueue(id,'handoff'),e=>e.reason==='owner_alert_location_required');
});

test('changed source references after enqueue cannot be delivered or acknowledged',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');await f.events.subscribe(OWNER,f.params());f.events.read(OWNER,id);
  f.b.db.prepare('UPDATE server_alerts SET created=created+1 WHERE id=?').run(id);
  await rejects(()=>f.events.drain(),'owner_alert_reference_immutable');assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,0);
  assert.throws(()=>f.events.read(OWNER,id),e=>e.reason==='owner_alert_reference_immutable');assert.throws(()=>f.events.ack(OWNER,id),e=>e.reason==='owner_alert_reference_immutable');
});

test('a crash after reservation waits for its durable lease, then reuses the same event identity',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');const sub=await f.events.subscribe(OWNER,f.params());
  const reserved=f.events.reserve(id,sub.id),event=f.events.envelope(reserved.row);assert.equal(reserved.attempt,1);f.restart();await f.events.drain();assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,0);
  f.advance(15001);await f.events.drain();assert.deepEqual(f.calls.at(-1).body,event);
  assert.equal(f.events.read(OWNER,id).deliveries[0].attempts,2);
});

test('duplicates, restart and renewal do not replay accepted callbacks or customer jobs',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');await f.events.subscribe(OWNER,f.params());await f.events.drain();
  f.restart();assert.equal(f.events.enqueue(id,'handoff').duplicate,true);await f.events.subscribe(OWNER,f.params());await f.events.drain();
  assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,1);
  assert.equal(f.b.db.prepare('SELECT COUNT(*) AS n FROM processing').get().n,0);assert.equal(f.b.db.prepare('SELECT COUNT(*) AS n FROM server_jobs').get().n,0);assert.equal(f.b.db.prepare('SELECT COUNT(*) AS n FROM deliveries').get().n,0);
});

test('kind filter and one replacement destination apply only to matching alert refs',async t=>{
  const f=fixture(t);f.events.enqueue(f.store('handoff'),'handoff');f.events.enqueue(f.store('technical','technical_failure'),'technical_failure');
  await f.events.subscribe(OWNER,f.params({arguments:{location_id:LOCATION,kinds:['technical_failure']}}));await f.events.drain();
  assert.deepEqual(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).map(c=>c.body.data.kind),['technical_failure']);
  await f.events.subscribe(OWNER,f.params({arguments:{location_id:LOCATION,kinds:['handoff']},delivery:{mode:'webhook',url:'https://replacement.example.com/',secret:SECRET}}));await f.events.drain();assert.equal(f.events.status(OWNER).active_subscriptions,1);assert.equal(f.calls.at(-1).body.data.kind,'handoff');
});

test('transient retries have stable body/event ID, fresh signatures and bounded attempts',async t=>{
  const f=fixture(t);f.events.enqueue(f.store(),'handoff');await f.events.subscribe(OWNER,f.params());f.setAnswer(503);
  for(let attempt=0;attempt<9;attempt++){await f.events.drain();f.advance(70000);}
  const calls=f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT);assert.equal(calls.length,8);assert.equal(new Set(calls.map(c=>c.raw)).size,1);assert.notEqual(calls[0].headers['webhook-timestamp'],calls[1].headers['webhook-timestamp']);for(const c of calls)verify(c);
  assert.equal(f.events.status(OWNER).deliveries[0].status,'dead');f.setAnswer(200);await f.events.subscribe(OWNER,f.params());await f.events.drain();assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,8);
});

test('HTTP 410 disables destination; expiry and unsubscribe stop future callbacks',async t=>{
  const f=fixture(t);const id=f.store();f.events.enqueue(id,'handoff');await f.events.subscribe(OWNER,f.params({ttlMs:60000}));f.advance(60001);await f.events.drain();assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,0);
  await f.events.subscribe(OWNER,f.params());f.setAnswer(410);await f.events.drain();assert.equal(f.events.status(OWNER).active_subscriptions,0);
  f.setAnswer(200);await f.events.subscribe(OWNER,f.params());f.events.enqueue(f.store('next'),'handoff');await f.events.unsubscribe(OWNER,f.params());await f.events.drain();assert.equal(f.calls.filter(c=>c.body.data?.alert_id==='next').length,0);
});

test('read-before-ack is required; authenticated acknowledgment is idempotent and does not claim human read',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');assert.throws(()=>f.events.ack(OWNER,id),e=>e.reason==='owner_alert_read_required');
  f.events.read(OWNER,id);const first=f.events.ack(OWNER,id),second=f.events.ack(OWNER,id);assert.equal(first.notificationId,second.notificationId);assert.equal(second.already_acknowledged,true);assert.equal(first.owner_read_verified,false);assert.equal(first.push_delivery_verified,false);
  assert.throws(()=>f.events.ack('intruder',id),e=>e.reason==='owner_alert_operator_required');
  await f.events.subscribe(OWNER,f.params());await f.events.drain();assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,0);assert.equal(f.events.status(OWNER).unacknowledged,0);
});

test('callback may read and acknowledge before its HTTP response without receipt being overwritten',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');await f.events.subscribe(OWNER,f.params());
  f.setPost(call=>{if(call.body.name===OWNER_ALERT_EVENT){f.events.read(OWNER,id);f.events.ack(OWNER,id);}return {status:200,body:'{}'};});await f.events.drain();
  assert.equal(f.events.status(OWNER).deliveries[0].status,'acknowledged');assert.equal(f.events.status(OWNER).unacknowledged,0);
});

test('callback replacement backfills unacknowledged refs with same event identity but skips acknowledged refs',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');await f.events.subscribe(OWNER,f.params());await f.events.drain();const first=f.calls.at(-1).body;
  await f.events.subscribe(OWNER,f.params({delivery:{mode:'webhook',url:'https://replacement.example.com/',secret:SECRET}}));await f.events.drain();assert.deepEqual(f.calls.at(-1).body,first);
  f.events.read(OWNER,id);f.events.ack(OWNER,id);await f.events.subscribe(OWNER,f.params({delivery:{mode:'webhook',url:'https://third.example.com/',secret:SECRET}}));await f.events.drain();assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,2);
});

test('separate instances reserve a callback once; receipt metadata and encrypted secrets survive restart',async t=>{
  const f=fixture(t),id=f.store();f.events.enqueue(id,'handoff');await f.events.subscribe(OWNER,f.params());const other=f.fork();
  await Promise.all([f.events.drain(),other.drain()]);assert.equal(f.calls.filter(c=>c.body.name===OWNER_ALERT_EVENT).length,1);
  const plaintext=JSON.stringify([f.b.db.prepare('SELECT * FROM owner_alert_subscriptions').all(),f.b.db.prepare('SELECT * FROM owner_alert_deliveries').all(),f.b.db.prepare('SELECT * FROM owner_alert_receipts').all()]);
  for(const hidden of [SECRET,'https://callback.example.com','PRIVATE CUSTOMER','+12065551234'])assert.equal(plaintext.includes(hidden),false);
  f.events.read(OWNER,id);const receipt=f.events.ack(OWNER,id);f.restart();assert.equal(f.events.read(OWNER,id).notificationId,receipt.notificationId);
});

test('secret rotation verifies new callback key and signs with both during overlap',async t=>{
  const f=fixture(t);await f.events.subscribe(OWNER,f.params());const next='whsec_'+Buffer.alloc(32,33).toString('base64');
  await f.events.subscribe(OWNER,f.params({delivery:{...f.params().delivery,secret:next}}));verify(f.calls.at(-1),next);
  f.events.enqueue(f.store(),'handoff');await f.events.drain();verify(f.calls.at(-1),next);verify(f.calls.at(-1),SECRET);
});

