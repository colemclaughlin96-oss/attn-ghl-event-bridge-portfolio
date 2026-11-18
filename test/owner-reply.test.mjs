import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import path from 'node:path';
import {tmpdir} from 'node:os';
import {Bridge,LOCATION,EVENT} from '../src/bridge.mjs';
import {OWNER_SCOPE as S,BASELINE,TEST_MARKER,TEST_REPLY} from '../src/owner-reply.mjs';
import {rpc} from '../src/server.mjs';
const owner='owner_fixture',cut='2026-10-20T20:30:00.000Z';
function fixture(t,{enabled=true}={}) {
  const dir=mkdtempSync(path.join(tmpdir(),'attn-owner-reply-'));let now=Date.parse(cut)+10000;
  const opts={dbPath:path.join(dir,'db.sqlite'),vaultKey:Buffer.alloc(32,29).toString('base64'),testContactId:S.contactId,now:()=>now,ownerReply:{enabled,notBefore:cut},post:async(u,b)=>({status:200,body:JSON.stringify({challenge:JSON.parse(b).challenge})})};
  let b=new Bridge(opts);t.after(()=>{b.close();rmSync(dir,{recursive:true,force:true});});
  const message=(id='new_1',extra={})=>({...S,id,direction:'inbound',messageType:'TYPE_CUSTOM_PROVIDER_SMS',dateAdded:new Date(Date.parse(cut)+1000).toISOString(),body:TEST_MARKER,...extra});
  const snapshot=(messages=[message()],extra={})=>({read_started_at:new Date(now).toISOString(),read_completed_at:new Date(now).toISOString(),contact:{id:S.contactId,locationId:LOCATION,dnd:false},pages:[{request_last_message_id:null,nextPage:true,lastMessageId:BASELINE.id,messages:[...messages,message(BASELINE.id,{dateAdded:BASELINE.dateAdded,body:'ATTN CLOUD TEST 2'})]}],...extra});
  const event=async(id='loop_1')=>{await b.subscribe(owner,{name:EVENT,arguments:{location_id:LOCATION,contact_id:S.contactId},delivery:{mode:'webhook',url:'https://callback.example.com/receive',secret:'whsec_'+Buffer.alloc(32,4).toString('base64')}});const r=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:S.contactId,direction:'inbound',loopMessageId:id},'loop_workflow');b.claim(owner,r.eventId);return r.eventId;};
  return {get b(){return b;},dir,message,snapshot,event,advance:n=>now+=n,restart:()=>{b.close();b=new Bridge(opts);},get now(){return now;}};
}
const blocked=(fn,reason)=>assert.throws(fn,e=>e.reason===reason);
test('default-disabled ledger and MCP preserve read-only listener, one subscription and receipts',async t=>{
  const f=fixture(t,{enabled:false}),id=await f.event();
  const before=f.b.db.prepare('SELECT * FROM processing').all();
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot()),'owner_reply_test_disabled');
  const r=await rpc(f.b,owner,{jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'attn_owner_reply_ledger',arguments:{action:'prepare',event_id:id,evidence:f.snapshot()}}});
  assert.equal(r.error.data.reason,'owner_reply_test_disabled');assert.equal(f.b.status(owner).replies_enabled,false);
  assert.equal(f.b.status(owner).active_subscriptions,1);assert.deepEqual(f.b.db.prepare('SELECT * FROM processing').all(),before);
});
test('unclaimed events, other owner and old event cannot authorize a reply',async t=>{
  const f=fixture(t),id=await f.event();
  blocked(()=>f.b.ownerReply.prepare('other',id,f.snapshot()),'event_not_accessible');
  f.b.db.prepare('DELETE FROM processing WHERE event_id=?').run(id);
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot()),'open_event_claim_required');
  f.b.claim(owner,id);f.b.processed(owner,id,'observed');
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot()),'open_event_claim_required');
});
test('baseline and activation cutoff exclude TEST2, older unread and same-millisecond messages',async t=>{
  const f=fixture(t),id=await f.event();
  const r=f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('old',{dateAdded:cut,body:'old unread'})]));
  assert.equal(r.reason,'no_new_ghl_inbound');assert.equal(f.b.db.prepare('SELECT count(*) n FROM owner_reply_messages').get().n,0);
});
test('chronology uses GHL source dates, never Loop IDs, notification receipt or list order',async t=>{
  const f=fixture(t),id=await f.event('unrelated_loop_uuid');
  const second=f.message('new_2',{dateAdded:'2026-10-20T20:30:02.000Z'});
  const r=f.b.ownerReply.prepare(owner,id,f.snapshot([second,f.message()]));
  assert.equal(r.message_id,'new_1');f.advance(1);
  const permit=f.b.ownerReply.begin(owner,id,f.snapshot([second,f.message()]));
  assert.equal(permit.params.body.replyMessageId,'new_1');assert.equal(permit.params.body.type,'Custom');assert.equal(permit.params.body.conversationProviderId,S.conversationProviderId);assert.equal(permit.params.body.message,TEST_REPLY);
  assert.ok(!JSON.stringify(permit).includes('unrelated_loop_uuid'));
});
test('same timestamp candidates fail closed instead of inventing chronological order',async t=>{
  const f=fixture(t),id=await f.event();blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message(),f.message('tied')])),'ambiguous_chronology');
});
test('foreign location/contact/conversation and unknown provider are rejected',async t=>{
  const f=fixture(t),id=await f.event();
  for(const k of ['locationId','contactId','conversationId']) blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('bad',{[k]:'other'})])),'message_scope');
  for(const x of [{conversationProviderId:'sms_fallback'},{messageType:'TYPE_SMS'}]) blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('bad',x)])),'provider_or_channel_mismatch');
});
test('opt-outs, unknown DND, non-marker bodies and attachments never receive the fixed reply',async t=>{
  const f=fixture(t),id=await f.event();
  for(const dnd of [true,undefined]) blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot(undefined,{contact:{id:S.contactId,locationId:LOCATION,dnd}})),'dnd_unknown_or_active');
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot(undefined,{contact:{id:S.contactId,locationId:LOCATION,dnd:false,dndSettings:{SMS:{status:'active'}}}})),'channel_dnd_or_unknown');
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('a',{attachments:['https://example.com/a']})])),'text_only_required');
  assert.equal(f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('stop',{body:'STOP'})])).prepared,false);
  f.advance(1000);
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('later',{dateAdded:'2026-10-20T20:30:03.000Z'})])),'older_pending_message');
});
test('page cap, completeness boundary, cursor continuity and duplicate IDs are enforced',async t=>{
  const f=fixture(t),id=await f.event(),s=f.snapshot();
  s.pages[0].messages.pop();blocked(()=>f.b.ownerReply.prepare(owner,id,s),'coverage_gap');
  s.pages.push({request_last_message_id:'wrong',nextPage:false,messages:[]});blocked(()=>f.b.ownerReply.prepare(owner,id,s),'pagination_chain');
  s.pages[1].request_last_message_id=BASELINE.id;s.pages[1].messages=[f.message(BASELINE.id,{dateAdded:BASELINE.dateAdded})];
  assert.equal(f.b.ownerReply.prepare(owner,id,s).prepared,true);
  s.pages.push(s.pages[1]);blocked(()=>f.b.ownerReply.prepare(owner,id,s),'snapshot_required');
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message(),f.message()])),'duplicate_or_invalid_message');
});
test('prepare cannot authorize a send; stale or changed second read is blocked',async t=>{
  const f=fixture(t),id=await f.event(),s=f.snapshot();assert.equal(f.b.ownerReply.prepare(owner,id,s).prepared,true);
  f.advance(31000);blocked(()=>f.b.ownerReply.begin(owner,id,s),'fresh_snapshot_required');
  blocked(()=>f.b.ownerReply.begin(owner,id,f.snapshot([f.message('different')])),'candidate_changed');
  const p=f.b.ownerReply.begin(owner,id,f.snapshot());assert.equal(p.permitted,true);
});
test('new outbound between prepare and send is a takeover and blocks the attempt',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());f.advance(1000);
  blocked(()=>f.b.ownerReply.begin(owner,id,f.snapshot([f.message(),f.message('manual',{direction:'outbound',dateAdded:'2026-10-20T20:30:05.000Z'})])),'human_or_other_sender_takeover');
});
test('concurrent event runs share one durable candidate and at most one send permit',async t=>{
  const f=fixture(t),a=await f.event('a'),b=await f.event('b');
  f.b.ownerReply.prepare(owner,a,f.snapshot());f.b.ownerReply.prepare(owner,b,f.snapshot());
  const results=await Promise.allSettled([Promise.resolve().then(()=>f.b.ownerReply.begin(owner,a,f.snapshot())),Promise.resolve().then(()=>f.b.ownerReply.begin(owner,b,f.snapshot()))]);
  assert.equal(results.filter(x=>x.status==='fulfilled'&&x.value.permitted).length,1);
  assert.equal(f.b.ownerReply.begin(owner,b,f.snapshot()).permitted,false);
});
test('crash after durable reservation leaves uncertain result, no automatic retry or new permit',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());const p=f.b.ownerReply.begin(owner,id,f.snapshot());f.restart();
  assert.equal(f.b.ownerReply.begin(owner,id,f.snapshot()).permitted,false);
  assert.equal(f.b.ownerReply.record(owner,id,{attempt_id:p.attempt_id,outcome:'unknown'}).retry_allowed,false);
  const other=await f.event('later');assert.equal(f.b.ownerReply.prepare(owner,other,f.snapshot()).prepared,false);
  assert.equal(f.b.status(owner).active_subscriptions,1);
});
test('success requires actual send response plus matching outbound GHL record; queued is not delivered',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());const p=f.b.ownerReply.begin(owner,id,f.snapshot());
  const outbound=f.message('out',{direction:'outbound',dateAdded:new Date(f.now).toISOString(),body:TEST_REPLY,status:'pending'});
  const r={attempt_id:p.attempt_id,outcome:'queued',outbound,send_response:{messageId:'out',conversationId:S.conversationId}};
  blocked(()=>f.b.ownerReply.record(owner,id,{...r,send_response:{messageId:'other'}}),'send_response_mismatch');
  blocked(()=>f.b.ownerReply.record(owner,id,{...r,outcome:'delivered'}),'delivery_status_mismatch');
  assert.equal(f.b.ownerReply.record(owner,id,r).phase,'queued');assert.equal(f.b.ownerReply.record(owner,id,r).already_recorded,true);
  f.restart();assert.equal(f.b.ownerReply.status().phase,'queued');assert.equal(f.b.ownerReply.begin(owner,id,f.snapshot()).permitted,false);
});
test('per-GHL-ID ledger is encrypted and survives event retention cleanup without reopening budget',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());f.b.ownerReply.begin(owner,id,f.snapshot());
  const disk=JSON.stringify(f.b.db.prepare('SELECT * FROM owner_reply_messages').all());assert.equal(disk.includes('new_1'),false);assert.equal(disk.includes(TEST_MARKER),false);
  f.advance(31*86400000);f.b.cleanup();f.restart();assert.equal(f.b.ownerReply.status().phase,'attempt_unknown');
  assert.equal(f.b.db.prepare('SELECT count(*) n FROM owner_reply_messages').get().n,1);
});
test('invalid ledger MCP action or evidence cannot bypass input gates',async t=>{
  const f=fixture(t),id=await f.event();
  for(const arguments_ of [{action:'send_now',event_id:id,evidence:{}},{action:'prepare',event_id:id,evidence:'invalid'},{action:'prepare',event_id:id,evidence:{},extra:true}]) {
    const r=await rpc(f.b,owner,{jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'attn_owner_reply_ledger',arguments:arguments_}});assert.ok(r.error);
  }
});

test('a newer STOP after the marker blocks send during the final preflight',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());f.advance(1000);
  const stop=f.message('stop_later',{body:'STOP',dateAdded:'2026-10-20T20:30:02.000Z'});
  blocked(()=>f.b.ownerReply.begin(owner,id,f.snapshot([stop,f.message()])),'candidate_changed');
  assert.equal(f.b.ownerReply.status().phase,'prepared');
});
test('two SQLite connections cannot both consume a send attempt',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());
  const second=new Bridge({dbPath:path.join(f.dir,'db.sqlite'),vaultKey:Buffer.alloc(32,29).toString('base64'),testContactId:S.contactId,now:()=>f.now,ownerReply:{enabled:true,notBefore:cut}});
  try {assert.equal(second.ownerReply.begin(owner,id,f.snapshot()).permitted,true);assert.equal(f.b.ownerReply.begin(owner,id,f.snapshot()).permitted,false);}finally{second.close();}
});
test('queued outbound can be verified delivered later without issuing another attempt',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());const p=f.b.ownerReply.begin(owner,id,f.snapshot());
  const outbound=f.message('out',{direction:'outbound',dateAdded:new Date(f.now).toISOString(),body:TEST_REPLY,status:'pending'});
  const r={attempt_id:p.attempt_id,outcome:'queued',outbound,send_response:{messageId:'out',conversationId:S.conversationId}};
  f.b.ownerReply.record(owner,id,r);f.advance(1000);
  assert.equal(f.b.ownerReply.record(owner,id,{...r,outcome:'delivered',outbound:{...outbound,status:'delivered'}}).phase,'delivered');
  assert.equal(f.b.ownerReply.begin(owner,id,f.snapshot()).permitted,false);
});

test('owner test expires after 30 minutes without disabling the listener',async t=>{
  const f=fixture(t),id=await f.event();f.advance(31*60000);
  blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot()),'owner_test_window_closed');assert.equal(f.b.enabled,true);
});
test('the new ledger tool executes prepare and reservation through the existing authenticated RPC dispatcher',async t=>{
  const f=fixture(t),id=await f.event();
  const call=(action,evidence)=>rpc(f.b,owner,{jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'attn_owner_reply_ledger',arguments:{action,event_id:id,evidence}}});
  assert.equal((await call('prepare',f.snapshot())).result.structuredContent.prepared,true);
  const p=(await call('begin_attempt',f.snapshot())).result.structuredContent;assert.equal(p.permitted,true);
  assert.equal((await call('record_result',{attempt_id:p.attempt_id,outcome:'unknown'})).result.structuredContent.phase,'attempt_unknown');
});

test('supplied inbound message IDs must be strings, never missing or numeric',async t=>{
  const f=fixture(t),id=await f.event();
  for(const bad of [undefined,null,12345]) blocked(()=>f.b.ownerReply.prepare(owner,id,f.snapshot([f.message('candidate',{id:bad})])),'duplicate_or_invalid_message');
});
test('supplied response pagination cursors must be strings, including the terminal page',async t=>{
  const f=fixture(t),id=await f.event();
  for(const bad of [undefined,null,12345]) {
    const s=f.snapshot();s.pages[0].lastMessageId=bad;
    blocked(()=>f.b.ownerReply.prepare(owner,id,s),'invalid_pagination_cursor');
  }
  const terminal=f.snapshot();terminal.pages[0].nextPage=false;terminal.pages[0].lastMessageId=12345;
  blocked(()=>f.b.ownerReply.prepare(owner,id,terminal),'invalid_pagination_cursor');
});
test('additional-page request cursor must be a supplied string',async t=>{
  const f=fixture(t),id=await f.event();
  for(const bad of [undefined,null,12345]) {
    const s=f.snapshot();s.pages.push({request_last_message_id:bad,nextPage:false,messages:[]});
    blocked(()=>f.b.ownerReply.prepare(owner,id,s),'pagination_chain');
  }
});
test('outbound evidence and returned send IDs reject missing or numeric values without resolving uncertainty',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());const p=f.b.ownerReply.begin(owner,id,f.snapshot());
  const outbound=f.message('out',{direction:'outbound',dateAdded:new Date(f.now).toISOString(),body:TEST_REPLY,status:'pending'});
  const r={attempt_id:p.attempt_id,outcome:'queued',outbound,send_response:{messageId:'out',conversationId:S.conversationId}};
  for(const bad of [undefined,null,12345]) {
    blocked(()=>f.b.ownerReply.record(owner,id,{...r,outbound:{...outbound,id:bad}}),'outbound_evidence_required');
    blocked(()=>f.b.ownerReply.record(owner,id,{...r,send_response:{...r.send_response,messageId:bad}}),'send_response_mismatch');
  }
  assert.equal(f.b.ownerReply.status().phase,'attempt_unknown');
  assert.equal(f.b.ownerReply.begin(owner,id,f.snapshot()).permitted,false);
});

test('sent is recorded distinctly, consumes budget across restart and advances only the same outbound ID',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());const p=f.b.ownerReply.begin(owner,id,f.snapshot());
  const outbound=f.message('sent_out',{direction:'outbound',dateAdded:new Date(f.now).toISOString(),body:TEST_REPLY,status:'sent'});
  const r={attempt_id:p.attempt_id,outcome:'sent',outbound,send_response:{messageId:'sent_out',conversationId:S.conversationId}};
  assert.equal(f.b.ownerReply.record(owner,id,r).phase,'sent');f.restart();
  assert.equal(f.b.ownerReply.status().phase,'sent');assert.equal(f.b.ownerReply.prepare(owner,id,f.snapshot()).reason,'one_attempt_already_consumed');
  assert.equal(f.b.ownerReply.begin(owner,id,f.snapshot()).permitted,false);
  blocked(()=>f.b.ownerReply.record(owner,id,{...r,outcome:'delivered'}),'delivery_status_mismatch');
  blocked(()=>f.b.ownerReply.record(owner,id,{...r,outcome:'delivered',outbound:{...outbound,id:'different',status:'delivered'}}),'outbound_id_changed');
  assert.equal(f.b.ownerReply.record(owner,id,{...r,outcome:'delivered',outbound:{...outbound,status:'delivered'}}).phase,'delivered');
});
test('queued can progress to sent without another permit',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());const p=f.b.ownerReply.begin(owner,id,f.snapshot());
  const outbound=f.message('out',{direction:'outbound',dateAdded:new Date(f.now).toISOString(),body:TEST_REPLY,status:'queued'});
  const r={attempt_id:p.attempt_id,outcome:'queued',outbound,send_response:{messageId:'out',conversationId:S.conversationId}};
  f.b.ownerReply.record(owner,id,r);
  assert.equal(f.b.ownerReply.record(owner,id,{...r,outcome:'sent',outbound:{...outbound,status:'sent'}}).phase,'sent');
});
test('new sales tables and sent support do not migrate historical unknown or escalated audit',async t=>{
  const f=fixture(t),id=await f.event();f.b.ownerReply.prepare(owner,id,f.snapshot());f.b.ownerReply.begin(owner,id,f.snapshot());f.b.processed(owner,id,'escalated');
  const original=f.b.db.prepare('SELECT * FROM processing').all();const audit=f.b.db.prepare('SELECT * FROM audit').all();f.restart();
  assert.equal(f.b.ownerReply.status().phase,'attempt_unknown');assert.deepEqual(f.b.db.prepare('SELECT * FROM processing').all(),original);assert.deepEqual(f.b.db.prepare('SELECT * FROM audit').all(),audit);
  assert.equal(f.b.salesReply.status().enabled,false);assert.equal(f.b.salesReply.status().enrollment_count,0);
});


