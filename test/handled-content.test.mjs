import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,owner,result} from './helpers/prospect-fixture.mjs';

// Historical terminal rows are seeded offline to cover legacy/reconciled state;
// production has no action that resets or freely rephases these rows.
function markHistorical(f,id,phase){const key=f.l.key(C,id),row=f.l.read('sales_messages',key);assert.ok(row);f.l.write('sales_messages',key,{...row,phase});}
const latest=f=>f.m('fresh_question',{body:'How does the ads review work?',dateAdded:new Date(f.now-1).toISOString()});
const draft=(f,p)=>({...f.draft,text:'We review your goals and current ads, then discuss practical next steps.',intent:'answer',answersMessageId:p.message_id,answersMessageIds:p.message_ids});

test('terminal historical attachment/provider rows do not poison a new supported question or preflight',async t=>{
 for(const phase of ['sent','delivered','ignored_system','suppressed'])for(const bad of [{attachments:['https://example.invalid/old-file']},{conversationProviderId:'legacy_provider',messageType:'TYPE_EMAIL'}]){
  const f=fixture(t),first=await f.event('old_event'),old=f.m('old_record',bad);
  assert.equal(f.prepare(first,f.snap([old])).prepared,false);markHistorical(f,old.id,phase);f.b.processed(owner,first,'skipped');f.advance(1000);
  const next=await f.event('new_event'),question=latest(f),history=[question,old],p=f.prepare(next,f.snap(history));
  assert.equal(p.prepared,true,phase);assert.deepEqual(p.message_ids,[question.id]);
  assert.equal(f.begin(next,p,f.snap(history,{detail:question}),draft(f,p)).permitted,true);
  assert.equal(f.l.rows(C).find(r=>r.messageId===old.id).phase,phase);
 }
});

test('ignored system content stays ignored on a later normal classification without blocking a fresh question',async t=>{
 const f=fixture(t),id=await f.event(),old=f.m('provider_event',{body:'Delivery notification',conversationProviderId:'system',messageType:'TYPE_SMS',attachments:['https://example.invalid/old']}),s=f.snap([old]);s.assessments[0].kind='system';
 assert.equal(f.prepare(id,s).reason,'no_pending_inbound');assert.equal(f.l.rows(C)[0].phase,'ignored_system');f.b.processed(owner,id,'skipped');f.advance(1000);
 const next=await f.event('next_event'),question=latest(f),p=f.prepare(next,f.snap([question,old]));assert.equal(p.prepared,true);assert.deepEqual(p.message_ids,[question.id]);
});

test('genuine pending unsupported content still blocks the entire unanswered batch',async t=>{
 for(const [bad,reason] of [[{attachments:['https://example.invalid/pending']},'unsupported_message_content'],[{conversationProviderId:'other'},'provider_mismatch']]){
  const f=fixture(t),id=await f.event(),old=f.m('pending_record',bad);assert.equal(f.prepare(id,f.snap([old])).reason,reason);f.b.processed(owner,id,'skipped');f.advance(1000);
  const next=await f.event('new_event'),question=latest(f);assert.equal(f.prepare(next,f.snap([question,old])).reason,reason);assert.ok(f.l.rows(C).every(r=>r.attemptId===undefined));
 }
});

test('skipping consumed content never clears uncertain, queued or failed attempts',async t=>{
 for(const phase of ['attempt_unknown','queued','failed']){
  const f=fixture(t),id=await f.event(),old=f.m('old_record',{attachments:['https://example.invalid/old']});f.prepare(id,f.snap([old]));markHistorical(f,old.id,phase);f.b.processed(owner,id,'skipped');f.advance(1000);
  const next=await f.event('next_event');assert.equal(f.prepare(next,f.snap([latest(f),old])).reason,'prior_send_unresolved_or_failed');assert.equal(f.l.rows(C).find(r=>r.messageId===old.id).phase,phase);
 }
});

test('handled rows still preserve STOP and existing durable opt-out protection',async t=>{
 for(const phase of ['sent','delivered','ignored_system','suppressed']){
  const f=fixture(t),id=await f.event(),old=f.m('old_stop',{body:'STOP',attachments:['https://example.invalid/old'],conversationProviderId:'other'});
  assert.equal(f.prepare(id,f.snap([old])).reason,'opt_out_or_dnd');markHistorical(f,old.id,phase);f.b.processed(owner,id,'skipped');f.advance(1000);
  const next=await f.event('next_event');assert.equal(f.prepare(next,f.snap([latest(f),old])).reason,'opt_out_or_dnd');assert.equal(f.prepare(next,f.snap([latest(f)])).reason,'opt_out_or_dnd');
 }
});

test('changed handled message identity/content still fails evidence validation',async t=>{
 const f=fixture(t),id=await f.event(),old=f.m('old_file',{attachments:['https://example.invalid/original']});f.prepare(id,f.snap([old]));markHistorical(f,old.id,'delivered');f.b.processed(owner,id,'skipped');f.advance(1000);
 const next=await f.event('next_event');assert.throws(()=>f.prepare(next,f.snap([latest(f),{...old,attachments:[]}])) ,e=>['message_evidence_changed','history_checkpoint_mismatch'].includes(e.reason));
});

test('recorded server/API reply is known automation; unrecognized API reply still requires owner resolution',async t=>{
 const f=fixture(t),id=await f.event(),p=f.begin(id,f.prepare(id)),sent=result(f,p);sent.outbound.source='api';f.l.record(owner,id,sent);f.b.processed(owner,id,'observed');f.advance(1000);
 const next=await f.event('known_event'),question=latest(f);assert.equal(f.prepare(next,f.snap([question,sent.outbound,f.m()])).prepared,true);f.b.processed(owner,next,'observed');f.advance(1000);
 const unknown=f.m('unrecognized_send',{direction:'outbound',source:'api',body:'Manual message',dateAdded:new Date(f.now-1).toISOString()}),unknownEvent=await f.event('unknown_event');
 assert.equal(f.prepare(unknownEvent,f.snap([unknown,question,sent.outbound,f.m()])).reason,'unverified_external_sender');f.b.processed(owner,unknownEvent,'skipped');f.advance(2*86400000);
 const later=await f.event('later_event');assert.equal(f.prepare(later,f.snap([{...latest(f),id:'later_question'},unknown,question,sent.outbound,f.m()])).reason,'unverified_external_sender');
});

