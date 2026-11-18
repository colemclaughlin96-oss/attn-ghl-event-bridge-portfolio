import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {ModelBudget} from '../src/model-budget.mjs';
import {createOpenAIAdapter} from '../src/openai-adapter.mjs';
import {LOCATION} from '../src/sales-policy.mjs';

const MODEL='gpt-6-luna',rates={inputUsdPerMillion:0.10,outputUsdPerMillion:0.50};
const request={purpose:'generate',model:MODEL,maxOutputTokens:650,inputBytes:1000};
function fixture(t,options={}){const db=new DatabaseSync(':memory:');t.after(()=>db.close());return {db,b:new ModelBudget({db,model:MODEL,rates,now:()=>Date.parse('2026-10-21T15:00:00Z'),...options})};}
const receipt=(reservationId,extra={})=>({reservationId,model:MODEL,purpose:'generate',usage:{input_tokens:100,output_tokens:50,total_tokens:150,...extra}});

test('application cap defaults to $10 and reserves full input bytes, framing, output maximum and margin',t=>{
  const {b,db}=fixture(t);assert.equal(b.status().monthlyLimitUsd,10);
  const r=b.allowRequest(request);assert.equal(r.allowed,true);const row=db.prepare('SELECT * FROM model_budget_reservations').get();
  assert.equal(row.reserved_micro_usd,Math.ceil(((1000+1024)*0.1+650*0.5)*1.1));
  assert.equal(b.status().accountedAndReservedUsd,row.reserved_micro_usd/1000000);assert.equal(b.status().unresolvedUsage,1);assert.equal(b.status().applicationGuardOnly,true);
});

test('a request crossing the cap is denied before a new reservation exists',t=>{
  const {b,db}=fixture(t,{monthlyLimitUsd:0.001});
  assert.equal(b.allowRequest(request).allowed,true);assert.equal(b.allowRequest(request).allowed,false);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM model_budget_reservations').get().n,1);
});

test('known usage releases excess allowance and counts cached/reasoning tokens once',t=>{
  const {b}=fixture(t);const r=b.allowRequest(request);
  b.recordUsage(receipt(r.reservationId,{cached_input_tokens:80,reasoning_output_tokens:30}));
  assert.equal(b.status().accountedAndReservedUsd,0.000035);assert.equal(b.status().unresolvedUsage,0);
});

test('missing, partial and inconsistent usage retains full allowance, including incomplete requests',t=>{
  const {b}=fixture(t);const r=b.allowRequest(request),before=b.status().accountedAndReservedUsd;
  for(const usage of [null,{input_tokens:100},{input_tokens:100,output_tokens:50,total_tokens:151},{input_tokens:-1,output_tokens:50,total_tokens:49}]){
    assert.equal(b.recordUsage({reservationId:r.reservationId,model:MODEL,purpose:'generate',status:'incomplete',usage}).reservationRetained,true);assert.equal(b.status().accountedAndReservedUsd,before);
  }
  b.recordUsage(receipt(r.reservationId));assert.equal(b.status().accountedAndReservedUsd,0.000035);
});

test('duplicate receipts cannot double charge or lower a previously accounted cost',t=>{
  const {b}=fixture(t);const r=b.allowRequest(request);b.recordUsage(receipt(r.reservationId));b.recordUsage(receipt(r.reservationId));
  b.recordUsage(receipt(r.reservationId,{input_tokens:0,output_tokens:0,total_tokens:0}));assert.equal(b.status().accountedAndReservedUsd,0.000035);
  b.recordUsage({reservationId:r.reservationId,model:MODEL,purpose:'generate',usage:null});assert.equal(b.status().unresolvedUsage,0);
  b.recordUsage(receipt(r.reservationId,{input_tokens:200,output_tokens:100,total_tokens:300}));assert.equal(b.status().accountedAndReservedUsd,0.00007);
});

test('provider usage exceeding the reservation is accounted and stops subsequent requests',t=>{
  const {b}=fixture(t,{monthlyLimitUsd:0.001});const r=b.allowRequest(request);
  b.recordUsage(receipt(r.reservationId,{input_tokens:20000,output_tokens:100,total_tokens:20100}));
  assert.equal(b.status().remainingUsd,0);assert.equal(b.allowRequest(request).allowed,false);assert.equal(b.status().accountedAndReservedUsd,0.00205);
});

test('reservations survive restart and two SQLite connections share one cap',t=>{
  const dir=mkdtempSync(path.join(tmpdir(),'attn-model-budget-'));const file=path.join(dir,'ledger.db');
  const a=new DatabaseSync(file),b=new DatabaseSync(file);t.after(()=>{a.close();b.close();rmSync(dir,{recursive:true,force:true});});
  const options={model:MODEL,rates,monthlyLimitUsd:0.001};const first=new ModelBudget({db:a,...options}),second=new ModelBudget({db:b,...options});
  const reservation=first.allowRequest(request);assert.equal(reservation.allowed,true);assert.equal(second.allowRequest(request).allowed,false);
  const restarted=new ModelBudget({db:b,...options});assert.equal(restarted.status().requests,1);assert.equal(restarted.allowRequest(request).allowed,false);
  restarted.recordUsage(receipt(reservation.reservationId));assert.equal(first.allowRequest(request).allowed,true);
});

test('a competing writer cannot make a reservation while another connection holds the write lock',t=>{
  const dir=mkdtempSync(path.join(tmpdir(),'attn-budget-lock-')),file=path.join(dir,'ledger.db');
  const a=new DatabaseSync(file),b=new DatabaseSync(file);t.after(()=>{a.close();b.close();rmSync(dir,{recursive:true,force:true});});
  const options={model:MODEL,rates,monthlyLimitUsd:0.001},first=new ModelBudget({db:a,...options}),second=new ModelBudget({db:b,...options});
  b.exec('PRAGMA busy_timeout=1');a.exec('BEGIN IMMEDIATE');
  try{assert.throws(()=>second.allowRequest(request),e=>e.reason==='model_budget_store_failed');assert.equal(a.prepare('SELECT COUNT(*) AS n FROM model_budget_reservations').get().n,0);}
  finally{a.exec('ROLLBACK');}
  assert.equal(first.allowRequest(request).allowed,true);assert.equal(second.allowRequest(request).allowed,false);
});

test('monthly accounting changes at UTC month boundary without deleting prior unknown reservations',t=>{
  let now=Date.parse('2026-10-31T23:59:59Z');const {b,db}=fixture(t,{monthlyLimitUsd:0.001,now:()=>now});
  const old=b.allowRequest(request);assert.equal(b.allowRequest(request).allowed,false);now=Date.parse('2026-11-01T00:00:00Z');
  assert.equal(b.status().month,'2026-11');assert.equal(b.allowRequest(request).allowed,true);
  b.recordUsage(receipt(old.reservationId));assert.equal(b.status().requests,1);assert.equal(db.prepare('SELECT COUNT(*) AS n FROM model_budget_reservations').get().n,2);
});

test('model and purpose mismatches cannot settle or reuse another reservation',t=>{
  const {b}=fixture(t);const r=b.allowRequest(request);
  assert.throws(()=>b.allowRequest({...request,model:'unpriced_model'}),e=>e.reason==='model_budget_request_invalid');
  assert.throws(()=>b.recordUsage({...receipt(r.reservationId),purpose:'assess'}),e=>e.reason==='model_budget_reservation_missing');
  assert.throws(()=>b.recordUsage(receipt('nonexistent')),e=>e.reason==='model_budget_reservation_missing');assert.equal(b.status().unresolvedUsage,1);
});

test('rates are explicit and configuration does not silently price unknown models',t=>{
  const {db}=fixture(t);
  assert.throws(()=>new ModelBudget({db,model:MODEL}),e=>e.reason==='model_budget_explicit_rates_required');
  assert.throws(()=>new ModelBudget({db,model:MODEL,rates:{inputUsdPerMillion:0,outputUsdPerMillion:0.5}}),e=>e.reason==='model_budget_explicit_rates_required');
  const disabled=new ModelBudget({db,model:MODEL,rates,monthlyLimitUsd:0});assert.equal(disabled.allowRequest(request).allowed,false);
});

test('adapter hooks integrate without method binding and denial causes zero HTTP calls',async t=>{
  const {b,db}=fixture(t,{monthlyLimitUsd:0.001});let calls=0;
  const adapter=createOpenAIAdapter({apiKey:'sk-synthetic-key',model:MODEL,reasoningEffort:'none',allowRequest:b.allowRequest,recordUsage:b.recordUsage,fetchImpl:async()=>{calls++;throw new Error('unknown transport');}});
  const message={id:'in_1',contactId:'synthetic',locationId:LOCATION,conversationId:'thread',direction:'inbound',dateAdded:'2026-10-21T15:00:00Z',body:'Hello'};
  await assert.rejects(adapter.assess({messages:[message],contactId:'synthetic'}),e=>e.reason==='model_transport_failed');
  assert.equal(calls,1);assert.equal(b.status().unresolvedUsage,1);
  await assert.rejects(adapter.assess({messages:[message],contactId:'synthetic'}),e=>e.reason==='model_budget_exhausted');assert.equal(calls,1);
  const stored=JSON.stringify(db.prepare('SELECT * FROM model_budget_reservations').all());assert.equal(stored.includes('Hello'),false);assert.equal(stored.includes('sk-synthetic-key'),false);
});

