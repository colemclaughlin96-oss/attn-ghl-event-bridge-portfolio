import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,EVENT,LOCATION,PROCESSING_LEASE_MS} from '../src/bridge.mjs';
import {rpc} from '../src/server.mjs';

const OWNER='claim_rpc_owner',CONTACT='claim_rpc_contact';
async function fixture(t){
  const dir=mkdtempSync(path.join(tmpdir(),'claim-rpc-'));let now=Date.parse('2026-10-21T16:00:00.000Z');
  const b=new Bridge({dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,101).toString('base64'),now:()=>now,enabled:true,
    prospectV1:{enabled:true,ingress:true,notBefore:'2026-10-21T15:00:00.000Z'},
    post:async(u,body)=>({status:200,body:JSON.stringify({challenge:JSON.parse(body).challenge})})});
  t.after(()=>{b.close();rmSync(dir,{recursive:true,force:true});});
  await b.subscribe(OWNER,{name:EVENT,arguments:{location_id:LOCATION,scope:'attn_incoming_prospects'},delivery:{mode:'webhook',url:'https://callback.example.com/',secret:'whsec_'+Buffer.alloc(32,102).toString('base64')}});
  const {eventId}=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:CONTACT,direction:'inbound',loopMessageId:'synthetic_claim_rpc'},'loop_workflow');
  const tool=(name,args={},owner=OWNER)=>rpc(b,owner,{jsonrpc:'2.0',id:1,method:'tools/call',params:{name,arguments:{event_id:eventId,...args}}});
  return {b,eventId,tool,advance:ms=>now+=ms};
}
const data=r=>{assert.equal(r.error,undefined,JSON.stringify(r.error));return r.result.structuredContent;};

test('RPC claims pass fences through ledger routing and refuse missing or obsolete tokens',async t=>{
  const f=await fixture(t),first=data(await f.tool('attn_claim_event',{fenced:true}));
  let invoked=0;
  f.b.prospectV1.prepare=(owner,id)=>{invoked++;return {prepared:false,reason:'no_pending_inbound',generation:f.b.assertClaim(owner,id).generation};};
  let r=await f.tool('attn_prospect_v1_ledger',{action:'prepare',evidence:{}});assert.equal(r.error.data.reason,'processing_claim_fence');assert.equal(invoked,0);
  r=await f.tool('attn_prospect_v1_ledger',{action:'prepare',claim_token:first.claim_token,evidence:{}});assert.equal(data(r).generation,1);
  f.advance(PROCESSING_LEASE_MS);
  const second=data(await f.tool('attn_claim_event',{fenced:true,recover:true}));assert.equal(second.generation,2);
  r=await f.tool('attn_prospect_v1_ledger',{action:'prepare',claim_token:first.claim_token,evidence:{}});assert.equal(r.error.data.reason,'processing_claim_fence');
  r=await f.tool('attn_prospect_v1_ledger',{action:'prepare',claim_token:second.claim_token,evidence:{}});assert.equal(data(r).generation,2);
  assert.equal((await f.tool('attn_record_processed',{outcome:'observed'})).error.data.reason,'processing_claim_fence');
  assert.equal(data(await f.tool('attn_record_processed',{outcome:'observed',claim_token:second.claim_token,stage:'completed',reason:'no_eligible_question'})).recorded,true);
});

test('RPC consumed permit cannot recover, but its fenced result can be reconciled after expiry',async t=>{
  const f=await fixture(t),claim=data(await f.tool('attn_claim_event',{fenced:true}));
  f.b.prospectV1.begin=(owner,id)=>{f.b.db.exec('BEGIN IMMEDIATE');try{f.b.reserveProcessingSend(owner,id);f.b.db.exec('COMMIT');return {permitted:true};}catch(e){f.b.db.exec('ROLLBACK');throw e;}};
  f.b.prospectV1.record=(owner,id)=>({recorded:true,generation:f.b.assertClaim(owner,id,{open:false,allowExpired:true}).generation,retry_allowed:false});
  assert.equal(data(await f.tool('attn_prospect_v1_ledger',{action:'begin_attempt',claim_token:claim.claim_token,evidence:{}})).permitted,true);
  f.advance(PROCESSING_LEASE_MS);
  assert.equal(data(await f.tool('attn_claim_event',{fenced:true,recover:true})).reason,'send_attempt_consumed');
  assert.equal((await f.tool('attn_prospect_v1_ledger',{action:'begin_attempt',claim_token:claim.claim_token,evidence:{}})).error.data.reason,'processing_claim_expired');
  assert.equal(data(await f.tool('attn_prospect_v1_ledger',{action:'record_result',claim_token:claim.claim_token,evidence:{}})).retry_allowed,false);
});

test('RPC deferral persists on existing delivery and prevents stale completion',async t=>{
  const f=await fixture(t),claim=data(await f.tool('attn_claim_event',{fenced:true}));
  data(await f.tool('attn_processing_trace',{claim_token:claim.claim_token,stage:'evidence',reason:'inbound_not_visible'}));
  assert.equal(data(await f.tool('attn_defer_processing',{claim_token:claim.claim_token,reason:'inbound_not_visible',retry_after_ms:1000})).deferred,true);
  assert.equal((await f.tool('attn_record_processed',{claim_token:claim.claim_token,outcome:'observed'})).error.data.reason,'processing_deferred_requires_recovery');
  f.advance(1000);const next=data(await f.tool('attn_claim_event',{fenced:true,recover:true}));
  assert.equal((await f.tool('attn_processing_trace',{claim_token:claim.claim_token,stage:'failed',reason:'old_run_stopped'})).error.data.reason,'processing_claim_fence');
  assert.equal(data(await f.tool('attn_record_processed',{claim_token:next.claim_token,outcome:'escalated',stage:'failed',reason:'provider_rate_limited'})).recorded,true);
  assert.equal(f.b.status(OWNER).recent_processing[0].reason,'provider_rate_limited');
  assert.equal(f.b.db.prepare('SELECT count(*) n FROM deliveries').get().n,1);
});

test('RPC legacy claims and acknowledgements retain backwards compatibility',async t=>{
  const f=await fixture(t);assert.equal(data(await f.tool('attn_claim_event')).claimed,true);
  assert.equal(data(await f.tool('attn_claim_event')).claimed,false);
  assert.equal(data(await f.tool('attn_record_processed',{outcome:'observed'})).recorded,true);
  assert.equal(data(await f.tool('attn_record_processed',{outcome:'escalated'})).outcome,'observed');
});

