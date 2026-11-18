import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,owner} from './helpers/prospect-fixture.mjs';
import {LOCATION} from '../src/sales-policy.mjs';
import {ownerEvidence,transactionPaymentEvidence} from '../src/owner-ai-evidence.mjs';
import {OWNER_SCOPE} from '../src/owner-reply.mjs';

const transaction=(change={})=>({_id:'transaction_1',contactId:C,altId:LOCATION,altType:'location',status:'failed',liveMode:true,...change});
const scope={locationId:LOCATION,contactId:C};
function withTransactions(s,rows){const d=s.reads.find(r=>r.operationId==='list-transactions').result.data;d.data=rows;d.totalCount=rows.length;return s;}

test('explicit failed/pending/processing evidence is not paid; real succeeded payment wins over other records',()=>{
 assert.deepEqual(transactionPaymentEvidence([],scope),{paid:false,classification:'no_records'});
 for(const status of ['failed','pending','processing'])for(const liveMode of [true,false,'true','false',undefined])assert.equal(transactionPaymentEvidence([transaction({status,liveMode})],scope).paid,false);
 for(const liveMode of [true,'true'])assert.equal(transactionPaymentEvidence([transaction({status:'succeeded',liveMode})],scope).paid,true);
 assert.equal(transactionPaymentEvidence([transaction(),transaction({_id:'t2',status:'pending'}),transaction({_id:'t3',status:'succeeded'})],scope).paid,true);
 assert.equal(transactionPaymentEvidence([{},transaction({status:'succeeded'})],scope).paid,true);
});

test('unknown scope/status/mode, duplicates and contradictory refund/capture evidence cannot clear payment review',()=>{
 for(const change of [{status:undefined},{status:'success'},{status:'paid'},{status:'refunded'},{status:'succeeded',liveMode:false},{status:'succeeded',liveMode:'false'},{status:'succeeded',liveMode:undefined},{status:'succeeded',liveMode:'TRUE'},{_id:undefined},{contactId:'other'},{altId:'other'},{altType:'agency'},{locationId:'other'},{amountRefunded:10},{amountRefunded:'10'},{amountRefunded:'unknown'},{chargeSnapshot:{status:'succeeded'}}])assert.equal(transactionPaymentEvidence([transaction(change)],scope).paid,null);
 assert.equal(transactionPaymentEvidence([transaction(),transaction()],scope).paid,null);
 for(const value of [undefined,null,{}])assert.equal(transactionPaymentEvidence(value,scope).paid,null);
 for(const amountRefunded of [0,'0','0.00'])assert.equal(transactionPaymentEvidence([transaction({amountRefunded})],scope).paid,false);
});

test('prospect can answer after explicitly unsuccessful payment attempts, including fresh preflight',async t=>{
 for(const status of ['failed','pending','processing']){
  const f=fixture(t),id=await f.event(),rows=[transaction({status})],raw=withTransactions(f.snap(),rows),prep=f.prepare(id,raw);
  assert.equal(prep.prepared,true,status);
  const fresh=withTransactions(f.snap([f.m()],{detail:f.m()}),rows);assert.equal(f.begin(id,prep,fresh).permitted,true,status);
 }
});

test('a payment becoming succeeded between prepare and preflight blocks the send and persists customer hold',async t=>{
 const f=fixture(t),id=await f.event(),prep=f.prepare(id,withTransactions(f.snap(),[transaction({status:'pending'})]));assert.equal(prep.prepared,true);
 const fresh=withTransactions(f.snap([f.m()],{detail:f.m()}),[transaction({status:'succeeded',liveMode:'true'})]);
 assert.equal(f.begin(id,prep,fresh).reason,'client_paid_sold_or_conflict');assert.equal(f.l.rows(C).some(r=>r.attemptId),false);
 f.restart();assert.equal(f.prepare(id,f.snap()).reason,'unresolved_handoff');
});

test('unknown transactions stay reviewable and never become clear leads',async t=>{
 for(const rows of [[transaction({status:'unrecognized'})],[transaction({status:'succeeded',liveMode:'false'})],[transaction({status:'failed',contactId:'other'})]]){
  const f=fixture(t),id=await f.event();assert.equal(f.prepare(id,withTransactions(f.snap(),rows)).reason,'payment_record_requires_review');
 }
});

test('raw real-payment evidence cannot be bypassed by paid:false summary',async t=>{
 const f=fixture(t),id=await f.event(),raw=withTransactions(f.snap(),[transaction({status:'succeeded'})]);
 const event=f.b.eventForOwner(owner,id),a=f.l.adapt(event,raw),entry=f.l.ensureEntry(event,a);a.s.crm.paid=false;
 const observed=f.l.observe(owner,id,a.s);assert.equal(f.l.permission({...observed,entry}),'client_paid_sold_or_conflict');
});

test('negative payment history never overrides current-customer, sold, explicit exclusion or STOP safeguards',async t=>{
 for(const kind of ['customer','tag','sold','stop']){
  const f=fixture(t),id=await f.event(),raw=withTransactions(f.snap(kind==='stop'?[f.m('stop',{body:'STOP'})]:undefined),[transaction()]);
  if(kind==='customer')raw.reads.find(r=>r.operationId==='get-contact').result.data.contact.type='customer';
  if(kind==='tag')raw.reads.find(r=>r.operationId==='get-contact').result.data.contact.tags.push('current customer');
  if(kind==='sold'){const d=raw.reads.find(r=>r.operationId==='search-opportunity').result.data;d.opportunities=[{id:'opp1',locationId:LOCATION,contactId:C,status:'won'}];d.meta.total=1;}
  assert.equal(f.prepare(id,raw).prepared,false,kind);
 }
});

test('original owner-only pilot keeps strict any-transaction classification',t=>{
 const f=fixture(t),raw=withTransactions(f.snap(),[transaction()]);raw.reads=raw.reads.filter(r=>r.operationId!=='search-conversation');
 for(const r of raw.reads){
  if(r.params.path?.contactId)r.params.path.contactId=OWNER_SCOPE.contactId;
  if(r.params.path?.conversationId)r.params.path.conversationId=OWNER_SCOPE.conversationId;
  if(r.params.query?.contactId)r.params.query.contactId=OWNER_SCOPE.contactId;
  const d=r.result.data;
  if(d.contact)d.contact.id=OWNER_SCOPE.contactId;
  for(const m of d.messages?.messages??[]){m.contactId=OWNER_SCOPE.contactId;m.conversationId=OWNER_SCOPE.conversationId;}
  for(const tr of Array.isArray(d.data)?d.data:[])tr.contactId=OWNER_SCOPE.contactId;
 }
 assert.equal(ownerEvidence(raw).crm.paid,true);
});

