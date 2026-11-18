import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,owner,result} from './helpers/prospect-fixture.mjs';
import {LOCATION} from '../src/sales-policy.mjs';
import {canonical,hash} from '../src/security.mjs';
import {inspectOwnerHold,resolveOwnerHold,reviewedExternalIds,releasedAlertHash} from '../src/owner-hold-resolution.mjs';
const operator='pinned_human_operator';
const state=f=>f.l.read('sales_contacts',f.l.key(C));
const entry=f=>f.l.read('prospect_entries',f.l.key(C));
const changeState=(f,extra)=>f.l.write('sales_contacts',f.l.key(C),{...state(f),...extra});
const inspect=(f,s=f.snap())=>inspectOwnerHold(f.b,operator,{contactId:C,snapshot:s});
const resolve=(f,inspection=inspect(f),s=f.snap())=>resolveOwnerHold(f.b,operator,{contactId:C,snapshot:s,expectedStateHash:inspection.expected_state_hash,reason:'owner_review_complete',note:'Reviewed this contact and answered the custom terms question.'});
async function held(t){
 const f=fixture(t);f.b.operatorOwner=operator;const id=await f.event();f.prepare(id);
 f.l.handoff(owner,id,{reason:'custom_terms',decision_needed:'Cole needs to answer this question.',snapshot:f.snap([f.m()],{full:false}),message_id:'in_1'});
 f.b.processed(owner,id,'escalated');f.advance(1);return {f,id};
}
const stored=(f,t)=>f.b.db.prepare(`SELECT * FROM ${t} ORDER BY 1`).all();

test('inspection is read-only; an exact reviewed hold resumes only after now without rewriting history',async t=>{
 const {f,id}=await held(t),tables=['prospect_messages','prospect_alerts','events','processing'],before=Object.fromEntries(tables.map(t=>[t,stored(f,t)]));
 const inspection=inspect(f);assert.equal(inspection.eligible,true);assert.equal(inspection.mutated,false);assert.ok(state(f).handoff);
 const applied=resolve(f,inspection);assert.equal(applied.no_send,true);assert.equal(applied.no_replay,true);assert.equal(applied.historical_rows_changed,0);assert.equal(state(f).handoff,null);assert.equal(state(f).externalLock,null);assert.equal(entry(f).resumeAfter,new Date(f.now).toISOString());
 for(const table of tables)assert.deepEqual(stored(f,table),before[table]);
 assert.equal(releasedAlertHash(f.b,entry(f)),hash(canonical(f.l.read('prospect_alerts',f.l.key(C)))));
 assert.throws(()=>f.prepare(id),e=>e.reason==='open_event_claim_required');
 f.advance(1);const newer=f.m('after_review',{body:'How does it work?',dateAdded:new Date(f.now).toISOString()}),next=await f.event('after_review');
 assert.deepEqual(f.prepare(next,f.snap([newer,f.m()])).message_ids,['after_review']);
});

test('only the pinned human operator may inspect or resolve; internal worker is not the owner',async t=>{
 const {f}=await held(t);for(const foreign of ['other_user','attn_server_worker_v1','']){
  assert.throws(()=>inspectOwnerHold(f.b,foreign,{contactId:C,snapshot:f.snap()}),e=>e.reason==='pinned_operator_required');
  assert.throws(()=>resolveOwnerHold(f.b,foreign,{contactId:C}),e=>e.reason==='pinned_operator_required');
 }
});

test('fresh STOP/DND, unknown consent, durable opt-out and client/payment evidence cannot be cleared',async t=>{
 const cases=[
  {setup:f=>changeState(f,{suppressed:true}),reason:'opt_out_or_dnd'},
  {setup:f=>changeState(f,{suppressionEvidence:{reason:'explicit_opt_out'}}),reason:'opt_out_or_dnd'},
  {snapshot:f=>{const s=f.snap();s.reads.find(r=>r.operationId==='get-contact').result.data.contact.dnd=true;return s;},reason:'confirmed_dnd'},
  {snapshot:f=>{const s=f.snap();delete s.reads.find(r=>r.operationId==='get-contact').result.data.contact.dnd;return s;},reason:'consent_evidence_unknown'},
  {snapshot:f=>f.snap([f.m('new_stop',{body:'STOP',dateAdded:new Date(f.now).toISOString()}),f.m()]),reason:'opt_out_or_dnd'},
  {snapshot:f=>{const s=f.snap();s.assessments[0].kind='opt_out';return s;},reason:'opt_out_or_dnd'},
  {snapshot:f=>{const s=f.snap();s.reads.find(r=>r.operationId==='get-contact').result.data.contact.type='customer';return s;},reason:'client_paid_sold_or_conflict'},
  {snapshot:f=>{const s=f.snap();s.reads.find(r=>r.operationId==='get-contact').result.data.contact.tags.push('current customer');return s;},reason:'client_or_suppression_tag'},
  {snapshot:f=>{const s=f.snap();const r=s.reads.find(r=>r.operationId==='list-transactions');r.result.data={data:[{_id:'paid',altType:'location',altId:LOCATION,contactId:C,status:'succeeded',liveMode:true}],totalCount:1};return s;},reason:'client_paid_sold_or_conflict'},
  {snapshot:f=>{const s=f.snap();const r=s.reads.find(r=>r.operationId==='search-opportunity');r.result.data={opportunities:[{id:'won',contactId:C,locationId:LOCATION,status:'won'}],meta:{total:1,nextPageUrl:null,nextPage:''}};return s;},reason:'client_paid_sold_or_conflict'}
 ];
 for(const item of cases){const {f}=await held(t);item.setup?.(f);const s=item.snapshot?.(f)??f.snap(),report=inspect(f,s);assert.equal(report.reason,item.reason);assert.throws(()=>resolve(f,report,s),e=>e.reason===item.reason);assert.ok(state(f).handoff);}
});

test('saved or freshly observed manual replies protect the full 24-hour window',async t=>{
 const {f}=await held(t);changeState(f,{manualUntil:f.now+1});assert.equal(inspect(f).reason,'manual_takeover_24h');
 changeState(f,{manualUntil:0});const manual=f.m('cole_reply',{direction:'outbound',source:'app',userId:'cole_user',dateAdded:new Date(f.now-1000).toISOString(),body:'I can help you with that.'});
 assert.equal(inspect(f,f.snap([manual,f.m()])).reason,'manual_takeover_24h');assert.ok(state(f).handoff);
});

test('uncertain, queued, failed or malformed consumed sends cannot be reset',async t=>{
 for(const phase of ['attempt_unknown','queued','failed','prepared']){
  const {f}=await held(t),k=f.l.key(C,'in_1'),old=f.l.read('sales_messages',k);f.l.write('sales_messages',k,{...old,phase,attemptId:'consumed_attempt',attemptAt:f.now-1});
  assert.equal(inspect(f).reason,'consumed_send_requires_reconciliation');assert.equal(f.l.read('sales_messages',k).attemptId,'consumed_attempt');
 }
});

test('definitive past send history is preserved when reviewing a later separate human hold',async t=>{
 const f=fixture(t);f.b.operatorOwner=operator;const id=await f.event(),prepared=f.prepare(id),permit=f.begin(id,prepared);f.l.record(owner,id,result(f,permit));
 f.l.handoff(owner,id,{reason:'custom_terms',decision_needed:'Review the terms.'});f.b.processed(owner,id,'escalated');f.advance(1);
 const before=stored(f,'prospect_messages');assert.equal(inspect(f).eligible,true);resolve(f);assert.deepEqual(stored(f,'prospect_messages'),before);
});

test('open processing and changed state prevent an owner review racing the worker',async t=>{
 const {f}=await held(t),report=inspect(f);changeState(f,{handoff:{...state(f).handoff,reason:'new_issue'}});
 assert.throws(()=>resolve(f,report),e=>e.reason==='state_changed_since_inspection');
 const next=await f.event('another_event');assert.equal(inspect(f).reason,'processing_still_open');f.b.processed(owner,next,'escalated');
});

test('wrong contact scope, stale snapshots and altered enrollment fail closed',async t=>{
 const {f}=await held(t),foreign=f.snap();foreign.reads.find(r=>r.operationId==='get-contact').result.data.contact.id='different_contact';assert.equal(inspect(f,foreign).reason,'contact_scope');
 const old=f.snap();f.advance(31000);assert.equal(inspect(f,old).reason,'fresh_snapshot_required');
 changeState(f,{enrollmentHash:'changed'});assert.equal(inspect(f).reason,'verified_contact_state_required');
});

test('exact reviewed historical external sender is cleared without recognizing later or altered replies',async t=>{
 const {f}=await held(t);f.advance(2*86400000);
 const manual=f.m('manual_old',{direction:'outbound',source:'api',userId:'cole_user',dateAdded:'2026-10-21T15:00:02.000Z',body:'I answered this personally.'});
 changeState(f,{externalLock:{messageId:manual.id,reason:'unverified_external_sender'},manualUntil:Date.parse(manual.dateAdded)+86400000});
 const s=f.snap([manual,f.m()]),report=inspect(f,s);assert.equal(report.eligible,true);assert.deepEqual(report.reviewed_external_messages.map(r=>r.message_id),[manual.id]);resolve(f,report,s);
 assert.deepEqual(reviewedExternalIds(f.b,entry(f),[manual]),[manual.id]);
 assert.deepEqual(reviewedExternalIds(f.b,entry(f),[{...manual,body:'Changed body'},{...manual,id:'later_reply'},{...manual,userId:'another_user'},{...manual,contactId:'other_contact'}]),[]);
 const receipt=f.l.read('prospect_owner_resolutions',entry(f).ownerHoldResolutionRef);assert.equal(receipt.before.state.externalLock.messageId,manual.id);assert.equal(receipt.note,'Reviewed this contact and answered the custom terms question.');
 f.advance(1);const next=await f.event('after_external_review'),m=f.m('after_review',{body:'What is the Facebook price?',dateAdded:new Date(f.now).toISOString()});
 assert.equal(f.prepare(next,f.snap([m,manual,f.m()])).prepared,true);assert.equal(state(f).externalLock,null);
});

test('external lock requires the exact source message in fresh evidence, not only an expired timer',async t=>{
 const {f}=await held(t);changeState(f,{externalLock:{messageId:'missing_manual',reason:'unverified_external_sender'}});assert.equal(inspect(f).reason,'external_lock_evidence_required');
});

test('the historical one-contact resolution record remains byte-for-byte unchanged',async t=>{
 const {f}=await held(t),k=f.l.key(C);f.l.write('prospect_owner_resolutions',k,{resolution:'historical_7969_receipt',releasedAlertHash:'historical_hash'});
 const prior=f.b.db.prepare('SELECT payload FROM prospect_owner_resolutions WHERE id=?').get(k).payload;
 resolve(f);assert.equal(f.b.db.prepare('SELECT payload FROM prospect_owner_resolutions WHERE id=?').get(k).payload,prior);
 assert.equal(f.b.db.prepare('SELECT count(*) count FROM prospect_owner_resolutions').get().count,2);
});

test('a later real handoff can alert again and each owner review has an immutable separate receipt',async t=>{
 const {f}=await held(t);const first=resolve(f),firstPayload=f.b.db.prepare('SELECT payload FROM prospect_owner_resolutions WHERE id=?').get(first.resolution).payload;
 f.advance(1);const next=await f.event('new_human_question'),m=f.m('new_human_question',{body:'Can Cole approve a custom deal?',dateAdded:new Date(f.now).toISOString()});f.prepare(next,f.snap([m,f.m()]));
 const h=f.l.handoff(owner,next,{reason:'custom_deal',decision_needed:'Cole needs to decide the terms.',snapshot:f.snap([m,f.m()],{full:false}),message_id:m.id});assert.equal(h.should_alert,true);assert.equal(state(f).handoff.eventId,next);
 f.b.processed(owner,next,'escalated');f.advance(1);const s=f.snap([m,f.m()]),second=resolve(f,inspect(f,s),s);assert.notEqual(second.resolution,first.resolution);
 assert.equal(f.b.db.prepare('SELECT payload FROM prospect_owner_resolutions WHERE id=?').get(first.resolution).payload,firstPayload);assert.equal(f.b.db.prepare('SELECT count(*) count FROM prospect_owner_resolutions').get().count,2);
 assert.throws(()=>resolve(f,inspect(f,s),s),e=>e.reason==='no_reviewable_hold');
});

