import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,V,owner} from './helpers/prospect-fixture.mjs';
import {CHECKOUT_LINK} from '../src/sales-policy.mjs';

const reply=(f,p,text,intent='acknowledge')=>({...f.draft,text,intent,answersMessageId:p.message_id,answersMessageIds:p.message_ids,...(intent==='checkout_link'?{buyingIntentVerified:true}:{})});
function recordSent(f,eventId,permit,outboundId){
 const outbound=f.m(outboundId,{direction:'outbound',source:'api',status:'sent',body:permit.params.body.message,dateAdded:new Date(f.now).toISOString()});
 assert.equal(f.l.record(owner,eventId,{attempt_id:permit.attempt_id,outcome:'sent',send_response:{messageId:outboundId,conversationId:V},outbound}).phase,'sent');
 f.b.processed(owner,eventId,'observed');
 return outbound;
}

test('identical benign replies to distinct new messages are allowed while each message stays single-use',async t=>{
 const f=fixture(t),first=f.m('first',{body:"I'll look over that."}),a=await f.event('first_event');
 const p=f.prepare(a,f.snap([first])),d=reply(f,p,'Sounds good.');
 const permit=f.begin(a,p,f.snap([first],{detail:first}),d);assert.equal(permit.permitted,true);
 assert.equal(f.begin(a,p,f.snap([first],{detail:first}),d).permitted,false);
 const out=recordSent(f,a,permit,'ack_one');f.advance(1000);
 const second=f.m('second',{body:"I'll get back to you tomorrow.",dateAdded:new Date(f.now-1).toISOString()}),history=[second,out,first],b=await f.event('second_event');
 const q=f.prepare(b,f.snap(history)),same=reply(f,q,'Sounds good.');
 assert.deepEqual(q.message_ids,['second']);
 const next=f.begin(b,q,f.snap(history,{detail:second}),same);assert.equal(next.permitted,true);assert.notEqual(next.attempt_id,permit.attempt_id);
 assert.equal(f.begin(b,q,f.snap(history,{detail:second}),same).permitted,false);
 const out2=recordSent(f,b,next,'ack_two');f.advance(1000);
 const duplicateEvent=await f.event('duplicate_notification');
 assert.equal(f.prepare(duplicateEvent,f.snap([out2,...history])).reason,'no_pending_inbound');
 assert.equal(f.l.rows(C).filter(r=>r.attemptId).length,2);
});

test('checkout intent in the unresolved batch survives a later package clarification',async t=>{
 const f=fixture(t),a=await f.event(),request=f.m('request',{body:'Send payment link'}),clarification=f.m('package',{body:'For Google',dateAdded:'2026-10-21T15:00:02.000Z'}),history=[clarification,request];
 const p=f.prepare(a,f.snap(history));assert.equal(p.message_id,'package');assert.deepEqual(p.message_ids,['request','package']);
 const permit=f.begin(a,p,f.snap(history,{detail:clarification}),reply(f,p,'Here is the link: '+CHECKOUT_LINK,'checkout_link'));
 assert.equal(permit.permitted,true);assert.equal(permit.params.body.replyMessageId,'package');
 assert.equal(new Set(f.l.rows(C).map(r=>r.attemptId)).size,1);
 assert.ok(f.l.rows(C).every(r=>r.phase==='attempt_unknown'));
});

test('answered buying intent cannot authorize checkout for a fresh unrelated question',async t=>{
 const f=fixture(t),a=await f.event('buy_event'),request=f.m('request',{body:'Send payment link'}),p=f.prepare(a,f.snap([request]));
 const permit=f.begin(a,p,f.snap([request],{detail:request}),reply(f,p,'Here is the link: '+CHECKOUT_LINK,'checkout_link'));
 const out=recordSent(f,a,permit,'checkout_one');f.advance(1000);
 const question=f.m('new_question',{body:'What does the ads review cover?',dateAdded:new Date(f.now-1).toISOString()}),history=[question,out,request],b=await f.event('question_event'),q=f.prepare(b,f.snap(history));
 assert.deepEqual(q.message_ids,['new_question']);
 assert.throws(()=>f.l.begin(owner,b,{snapshot:f.snap(history,{detail:question}),draft:reply(f,q,'Here is the link: '+CHECKOUT_LINK,'checkout_link'),message_id:q.message_id,message_ids:['request','new_question'],thread_version:q.thread_version}),e=>e.reason==='relevant_buying_intent_required');
 assert.equal(f.l.rows(C).find(r=>r.messageId==='new_question').attemptId,undefined);
 assert.equal(f.l.rows(C).find(r=>r.messageId==='request').phase,'sent');
});

test('batch checkout still respects current-customer and approved-URL gates',async t=>{
 for(const kind of ['customer','url']){
  const f=fixture(t),a=await f.event(),request=f.m('request',{body:'Send payment link'}),clarification=f.m('package',{body:'For Google',dateAdded:'2026-10-21T15:00:02.000Z'}),history=[clarification,request],p=f.prepare(a,f.snap(history));
  const fresh=f.snap(history,{detail:clarification}),d=reply(f,p,'Here is the link: '+(kind==='url'?'https://unapproved.example/pay':CHECKOUT_LINK),'checkout_link');
  if(kind==='customer')fresh.reads.find(r=>r.operationId==='get-contact').result.data.contact.type='customer';
  if(kind==='url')assert.throws(()=>f.begin(a,p,fresh,d),e=>e.reason==='unapproved_link');
  else assert.equal(f.begin(a,p,fresh,d).reason,'client_paid_sold_or_conflict');
  assert.ok(f.l.rows(C).every(r=>r.attemptId===undefined));
 }
});

test('allowing repeated text never bypasses an uncertain prior send',async t=>{
 const f=fixture(t),a=await f.event('first_event'),first=f.m('first',{body:"I'll look over that."}),p=f.prepare(a,f.snap([first]));
 const permit=f.begin(a,p,f.snap([first],{detail:first}),reply(f,p,'Sounds good.'));assert.equal(permit.permitted,true);
 assert.equal(f.l.record(owner,a,{attempt_id:permit.attempt_id,outcome:'unknown'}).phase,'attempt_unknown');f.advance(1000);
 const second=f.m('second',{body:'Thanks again.',dateAdded:new Date(f.now-1).toISOString()}),b=await f.event('next_event');
 assert.equal(f.prepare(b,f.snap([second,first])).reason,'prior_send_unresolved_or_failed');
 assert.equal(f.l.rows(C).filter(r=>r.attemptId).length,1);
});

