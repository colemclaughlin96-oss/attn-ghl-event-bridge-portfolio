import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,LOCATION,EVENT} from '../src/bridge.mjs';
import {createServer,rpc} from '../src/server.mjs';

const OWNER='owner_fixture',CONTACT='owner_contact_fixture';
const SECRET='whsec_'+Buffer.alloc(32,11).toString('base64');
function fixture(t) {
  const dir=mkdtempSync(path.join(tmpdir(),'attn-loop-'));
  let now=Date.parse('2026-10-20T20:00:00Z'),answer=200;
  const calls=[];
  const opts={dbPath:path.join(dir,'bridge.sqlite'),vaultKey:Buffer.alloc(32,12).toString('base64'),testContactId:CONTACT,now:()=>now,
    post:async(url,raw,headers)=>{const body=JSON.parse(raw);calls.push({raw,body,headers});return {status:answer,body:JSON.stringify(body.type==='verification'?{challenge:body.challenge}:{ok:true})};}};
  let bridge=new Bridge(opts);
  t.after(()=>{bridge.close();rmSync(dir,{recursive:true,force:true});});
  return {get bridge(){return bridge;},calls,advance:n=>now+=n,setAnswer:n=>answer=n,
    restart:()=>{bridge.close();bridge=new Bridge(opts);},
    subscribe:()=>bridge.subscribe(OWNER,{name:EVENT,arguments:{location_id:LOCATION,contact_id:CONTACT},delivery:{mode:'webhook',url:'https://callback.example.com/incoming',secret:SECRET}}),
    input:(extra={})=>({type:'LoopInboundMessage',locationId:LOCATION,contactId:CONTACT,direction:'inbound',loopMessageId:'loop_AA-1:opaque.fixture',...extra})};
}
function signature(call) {
  const expected=crypto.createHmac('sha256',Buffer.from(SECRET.slice(6),'base64')).update(`${call.headers['webhook-id']}.${call.headers['webhook-timestamp']}.${call.raw}`).digest('base64');
  assert.ok(call.headers['webhook-signature'].split(' ').includes('v1,'+expected));
}
test('Loop delivery distinguishes source IDs and receipt time from absent GHL evidence',async t=>{
  const f=fixture(t);await f.subscribe();
  const accepted=f.bridge.ingest(f.input(),'loop_workflow');await f.bridge.drain();
  assert.equal(accepted.accepted,true);
  const sent=f.calls.at(-1);signature(sent);
  assert.equal(sent.body.name,EVENT);assert.equal(sent.body.eventId,accepted.eventId);
  assert.deepEqual(sent.body.data,{location_id:LOCATION,contact_id:CONTACT,source:'loop_workflow',source_message_id:'loop_AA-1:opaque.fixture',received_at:'2026-10-20T20:00:00.000Z',timestamp_basis:'bridge_received_at'});
  assert.equal(sent.body.timestamp,sent.body.data.received_at);
  assert.equal('message_id' in sent.body.data,false);assert.equal('conversation_id' in sent.body.data,false);
  assert.equal('source_timestamp' in sent.body.data,false);assert.equal('channel' in sent.body.data,false);
  assert.equal(f.bridge.status(OWNER).replies_enabled,false);
});
test('Loop dedupe, original receipt, outbox, subscription and claim persist across restart',async t=>{
  const f=fixture(t);const sub=await f.subscribe();
  const first=f.bridge.ingest(f.input(),'loop_workflow');f.setAnswer(503);await f.bridge.drain();
  f.restart();f.advance(60000);
  const second=f.bridge.ingest(f.input(),'loop_workflow');assert.equal(second.eventId,first.eventId);assert.equal(second.duplicate,true);
  assert.equal(f.bridge.status(OWNER).active_subscriptions,1);
  assert.equal((await f.subscribe()).id,sub.id);
  f.setAnswer(200);await f.bridge.drain();
  const sent=f.calls.filter(c=>c.body.name===EVENT);assert.equal(sent.length,2);assert.equal(sent[0].raw,sent[1].raw);
  signature(sent[0]);signature(sent[1]);assert.notEqual(sent[0].headers['webhook-timestamp'],sent[1].headers['webhook-timestamp']);
  assert.deepEqual(f.bridge.status(OWNER).chat_acknowledgements,[]);
  assert.equal(f.bridge.claim(OWNER,first.eventId).claimed,true);f.restart();assert.equal(f.bridge.claim(OWNER,first.eventId).claimed,false);
  assert.throws(()=>f.bridge.processed('other_owner',first.eventId,'observed'));
  assert.equal(f.bridge.processed(OWNER,first.eventId,'escalated').outcome,'escalated');
  assert.equal(f.bridge.processed(OWNER,first.eventId,'observed').outcome,'escalated');
  assert.equal(f.bridge.db.prepare('SELECT count(*) n FROM events').get().n,1);
  assert.equal(f.bridge.db.prepare('SELECT count(*) n FROM deliveries').get().n,1);
});
test('Loop gates reject other locations, contacts, outbound events and unresolved IDs',async t=>{
  const f=fixture(t);await f.subscribe();
  for(const extra of [{locationId:'other_location'},{locationId:undefined},{contactId:'prospect'},{contactId:'{{contact.id}}'},{contactId:undefined},{direction:'outbound'},{type:'InboundMessage'}])
    assert.equal(f.bridge.ingest(f.input(extra),'loop_workflow').accepted,false);
  for(const loopMessageId of [undefined,null,1,'','{{loop_message_inbound.message_id}}','space here','id\n','x'.repeat(257)])
    assert.throws(()=>f.bridge.ingest(f.input({loopMessageId}),'loop_workflow'),e=>e.reason==='invalid_loop_message_id');
  assert.equal(f.bridge.db.prepare('SELECT count(*) n FROM events').get().n,0);
});
test('Loop contract rejects invented GHL fields, source times and business content',async t=>{
  const f=fixture(t);await f.subscribe();
  for(const extra of [{messageId:'fake_ghl'},{conversationId:'fake_conversation'},{dateAdded:'2026-10-20T20:00:00Z'},{body:'private'},{contact:'+15555550123'},{messageTypeString:'TYPE_SMS'},{conversationProviderId:'assumed_provider'}])
    assert.throws(()=>f.bridge.ingest(f.input(extra),'loop_workflow'),e=>e.reason==='invalid_loop_payload');
  f.bridge.ingest(f.input(),'loop_workflow');
  const persisted=JSON.stringify([f.bridge.db.prepare('SELECT * FROM events').all(),f.bridge.db.prepare('SELECT * FROM audit').all()]);
  assert.equal(persisted.includes('loop_AA-1:opaque.fixture'),false);assert.equal(persisted.includes(CONTACT),false);
});
test('Loop IDs have their own namespace and preserve case; legacy GHL route stays unchanged',async t=>{
  const f=fixture(t);await f.subscribe();
  const loop=f.bridge.ingest(f.input({loopMessageId:'same_id'}),'loop_workflow');
  const changedCase=f.bridge.ingest(f.input({loopMessageId:'SAME_id'}),'loop_workflow');
  const ghl={type:'InboundMessage',locationId:LOCATION,contactId:CONTACT,conversationId:'conversation_fixture',messageId:'same_id',direction:'inbound',messageTypeString:'TYPE_CUSTOM_PROVIDER_SMS',dateAdded:'2026-10-20T19:59:00Z'};
  const legacy=f.bridge.ingest(ghl,'ghl_signed');const repeat=f.bridge.ingest(ghl,'ghl_workflow');
  assert.notEqual(loop.eventId,legacy.eventId);assert.notEqual(loop.eventId,changedCase.eventId);assert.equal(repeat.eventId,legacy.eventId);assert.equal(repeat.duplicate,true);
  assert.equal(f.bridge.eventForOwner(OWNER,legacy.eventId).timestamp,'2026-10-20T19:59:00.000Z');
  assert.equal(f.bridge.eventForOwner(OWNER,legacy.eventId).data.message_id,'same_id');
  assert.equal(f.bridge.ingest(f.input(),'ghl_workflow').accepted,false);
  assert.throws(()=>f.bridge.ingest(ghl,'untrusted_source'),e=>e.reason==='unknown_source');
});
test('Loop ingress respects missing subscriptions, pause and unsubscribe',async t=>{
  const f=fixture(t);assert.equal(f.bridge.ingest(f.input(),'loop_workflow').status,503);
  await f.subscribe();const result=f.bridge.ingest(f.input(),'loop_workflow');f.bridge.enabled=false;await f.bridge.drain();
  assert.equal(f.bridge.ingest(f.input(),'loop_workflow').status,503);
  assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,0);
  f.bridge.enabled=true;
  await f.bridge.unsubscribe(OWNER,{name:EVENT,arguments:{location_id:LOCATION,contact_id:CONTACT},delivery:{mode:'webhook',url:'https://callback.example.com/incoming',secret:SECRET}});
  await f.bridge.drain();assert.equal(f.calls.filter(c=>c.body.name===EVENT).length,0);
  assert.equal(f.bridge.db.prepare('SELECT status FROM deliveries WHERE event_id=?').get(result.eventId).status,'cancelled');
});
test('Loop HTTP route shares the existing workflow secret and cannot bypass MCP owner auth',async t=>{
  const f=fixture(t),proxyToken='p'.repeat(40),workflowToken='w'.repeat(40);
  const server=createServer({bridge:f.bridge,proxyToken,workflowToken,proxyPrincipal:OWNER});
  await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>new Promise(r=>server.close(r)));
  const request=(route,body,headers={})=>fetch(`http://127.0.0.1:${server.address().port}`+route,{method:'POST',headers:{'content-type':'application/json',...headers},body:JSON.stringify(body)});
  for(const headers of [{},{'x-attn-webhook-key':proxyToken},{authorization:'Bearer '+proxyToken}])
    assert.equal((await request('/webhooks/loop-workflow',f.input(),headers)).status,401);
  assert.equal((await request('/mcp',{}, {authorization:'Bearer '+proxyToken,'x-attn-principal':'other_owner'})).status,401);
  const auth={'x-attn-webhook-key':workflowToken};
  assert.equal((await request('/webhooks/loop-workflow',f.input(),auth)).status,503);
  await f.subscribe();
  assert.equal((await request('/webhooks/loop-workflow',f.input(),{...auth,origin:'https://browser.example.com'})).status,403);
  assert.equal((await request('/webhooks/loop-workflow',f.input({messageId:'invented'}),auth)).status,422);
  const other=await request('/webhooks/loop-workflow',f.input({contactId:'prospect'}),auth);assert.equal((await other.json()).accepted,false);
  const valid=await request('/webhooks/loop-workflow',f.input(),auth);assert.equal(valid.status,200);const event=await valid.json();assert.equal(event.accepted,true);
  const duplicate=await request('/webhooks/loop-workflow',f.input(),auth);assert.equal((await duplicate.json()).duplicate,true);
  const claimed=await rpc(f.bridge,OWNER,{jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'attn_claim_event',arguments:{event_id:event.eventId}}});assert.equal(claimed.result.structuredContent.claimed,true);
  const ack=await rpc(f.bridge,OWNER,{jsonrpc:'2.0',id:2,method:'tools/call',params:{name:'attn_record_processed',arguments:{event_id:event.eventId,outcome:'escalated'}}});assert.equal(ack.result.structuredContent.recorded,true);
  assert.equal(f.bridge.status(OWNER).active_subscriptions,1);assert.equal(f.bridge.status(OWNER).replies_enabled,false);
});


