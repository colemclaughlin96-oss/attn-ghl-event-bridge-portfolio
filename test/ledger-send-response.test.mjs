import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,V,owner,result} from './helpers/prospect-fixture.mjs';

async function preparedBatch(t){
 const f=fixture(t),id=await f.event(),second=f.m('in_2',{body:'Does that include advertising spend?',dateAdded:'2026-10-21T15:00:02.000Z'}),messages=[second,f.m()];
 const prep=f.prepare(id,f.snap(messages)),draft={...f.draft,answersMessageId:'in_2',answersMessageIds:['in_1','in_2']};
 const permit=f.begin(id,prep,f.snap(messages,{detail:second}),draft);return {f,id,permit,messages};
}
const record=(f,id,permit,send_response)=>f.l.record(owner,id,{attempt_id:permit.attempt_id,outcome:'unknown',...(send_response===undefined?{}:{send_response})});
const bytes=f=>f.b.db.prepare('SELECT id,payload FROM prospect_messages ORDER BY id').all();

test('a successful send response preserves the exact ID across the consumed batch without claiming delivery',async t=>{
 const {f,id,permit,messages}=await preparedBatch(t),response={messageId:'out1',conversationId:V};
 const saved=record(f,id,permit,response);assert.equal(saved.phase,'attempt_unknown');assert.equal(saved.send_response_recorded,true);assert.equal(saved.retry_allowed,false);
 assert.deepEqual(new Set(saved.message_ids),new Set(['in_1','in_2']));
 for(const row of f.l.rows(C)){assert.equal(row.phase,'attempt_unknown');assert.equal(row.outboundId,'out1');assert.equal(row.sendResponseRecorded,true);assert.equal(row.sendResponseConversationId,V);assert.equal(row.attemptId,permit.attempt_id);}
 const stored=bytes(f);record(f,id,permit,response);assert.deepEqual(bytes(f),stored);f.restart();assert.equal(f.prepare(id,f.snap(messages)).reason,'prior_send_unresolved_or_failed');
 const verified=f.l.record(owner,id,result(f,permit,'sent'));assert.equal(verified.phase,'sent');assert.ok(f.l.rows(C).every(r=>r.phase==='sent'&&r.sendResponseRecorded&&r.outboundId==='out1'));
});

test('a transport timeout without a successful provider response saves no guessed ID or reconciliation marker',async t=>{
 const {f,id,permit}=await preparedBatch(t),before=bytes(f),saved=record(f,id,permit);assert.equal(saved.retry_allowed,false);assert.deepEqual(bytes(f),before);
 for(const row of f.l.rows(C)){assert.equal(row.phase,'attempt_unknown');assert.equal(row.outboundId,undefined);assert.equal(row.sendResponseRecorded,undefined);assert.equal(row.sendResponseConversationId,undefined);}
});

test('receipt identity mismatch, inbound IDs and conflicting response IDs are rejected atomically',async t=>{
 const {f,id,permit}=await preparedBatch(t);
 for(const response of [null,{messageId:123,conversationId:V},{messageId:'out1',conversationId:'another_thread'},{messageId:'in_1',conversationId:V},{messageId:'in_2',conversationId:V}]){
  const before=bytes(f);assert.throws(()=>record(f,id,permit,response),e=>e.reason==='send_response_or_outbound_mismatch');assert.deepEqual(bytes(f),before);
 }
 record(f,id,permit,{messageId:'out1',conversationId:V});const before=bytes(f);assert.throws(()=>record(f,id,permit,{messageId:'out2',conversationId:V}),e=>e.reason==='send_response_or_outbound_mismatch');assert.deepEqual(bytes(f),before);
 const conflicting=result(f,permit,'sent');conflicting.outbound.id='out2';conflicting.send_response.messageId='out2';assert.throws(()=>f.l.record(owner,id,conflicting),e=>e.reason==='send_response_or_outbound_mismatch');assert.deepEqual(bytes(f),before);
});

test('partial or inconsistent batch receipt markers cannot be silently overwritten',async t=>{
 for(const change of [{outboundId:'out1'},{sendResponseRecorded:true},{sendResponseConversationId:'another_thread'}]){
  const {f,id,permit}=await preparedBatch(t),key=f.l.key(C,'in_2'),row=f.l.read('sales_messages',key);f.l.write('sales_messages',key,{...row,...change});const before=bytes(f);
  assert.throws(()=>record(f,id,permit,{messageId:'out1',conversationId:V}),e=>e.reason==='batch_result_state_mismatch');assert.deepEqual(bytes(f),before);
 }
});

