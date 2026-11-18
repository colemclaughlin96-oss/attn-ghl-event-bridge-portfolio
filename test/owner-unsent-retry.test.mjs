import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,LOCATION,SERVER_WORKER_OWNER as WORKER} from '../src/bridge.mjs';
import {ServerWorker} from '../src/server-worker.mjs';
import {PROVIDER,priceAnswer} from '../src/sales-policy.mjs';
const OPERATOR='synthetic_operator',C='synthetic_contact',V='synthetic_conversation',CUT='2026-10-21T15:00:00.000Z';
function fixture(t){
 let now=Date.parse(CUT)+10000,sends=0;const dir=mkdtempSync(path.join(tmpdir(),'attn-unsent-retry-')),workers=[];
 const b=new Bridge({dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,51).toString('base64'),enabled:true,processingMode:'server',serverWorkerNotBefore:CUT,operatorOwner:OPERATOR,now:()=>now,prospectV1:{enabled:true,ingress:true,notBefore:CUT}}),l=b.prospectV1;
 const m=(id='question',extra={})=>({id,locationId:LOCATION,contactId:C,conversationId:V,conversationProviderId:PROVIDER,messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound',dateAdded:new Date(Date.parse(CUT)+9900).toISOString(),body:'What does Google Ads cost?',attachments:[],...extra});
 const read=(operationId,params,data)=>({operationId,locationId:LOCATION,params,result:{operationId,status:200,success:true,data}});
 const snapshot=({candidateId=null,messages=[m()],contact={}}={})=>{
  const reads=[read('search-conversation',{query:{contactId:C,limit:2}},{total:1,conversations:[{id:V,contactId:C,locationId:LOCATION}]}),read('get-contact',{path:{contactId:C}},{contact:{id:C,locationId:LOCATION,type:'lead',dnd:false,tags:['imessage'],timezone:'America/Los_Angeles',...contact}}),read('get-messages',{path:{conversationId:V},query:{limit:50}},{messages:{messages,nextPage:false,lastMessageId:messages.at(-1).id}}),read('get-appointments-for-contact',{path:{contactId:C}},{events:[]}),read('search-opportunity',{query:{contactId:C,status:'all',limit:100}},{opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}}),read('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:C,limit:100,offset:0}},{data:[],totalCount:0})];
  if(candidateId)reads.push(read('get-message',{path:{id:candidateId}},{message:messages.find(m=>m.id===candidateId)}));
  return {read_started_at:new Date(now).toISOString(),read_completed_at:new Date(now).toISOString(),reads,assessments:messages.filter(m=>m.direction==='inbound').map(m=>({messageId:m.id,kind:'normal'}))};
 };
 const event=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:C,direction:'inbound',loopMessageId:'synthetic_loop'},'loop_workflow').eventId;
 const reservation=b.reserveServerJobs()[0],claim=b.claim(WORKER,event,{fenced:true});
 const prep=b.withClaim(WORKER,event,claim.claim_token,()=>l.prepare(WORKER,event,snapshot()));assert.equal(prep.prepared,true);
 b.recordStage(WORKER,event,{claim_token:claim.claim_token,stage:'generation',reason:'approved_price_facts_required'});
 b.processed(WORKER,event,'escalated',{claim_token:claim.claim_token,stage:'failed',reason:'approved_price_facts_required'});
 b.finishServerJob(event,reservation.job_token,{category:'failed',reason:'approved_price_facts_required'});b.persistTechnicalServerAlert(WORKER,event);now+=1;
 const draft={text:priceAnswer('google'),intent:'pricing',priceKey:'google',answersMessageId:'question',answersMessageIds:['question'],policyReviewed:true,untrustedContentIgnored:true};
 const adapters={snapshot:async x=>snapshot(x),generate:async()=>({finishReason:'stop',complete:true,draft}),send:async()=>{sends++;return {messageId:'outbound',conversationId:V};},verify:async()=>({outcome:'sent',send_response:{messageId:'outbound',conversationId:V},outbound:m('outbound',{direction:'outbound',body:draft.text,status:'sent',dateAdded:new Date(now).toISOString()})})};
 const input=(extra={})=>({action:'inspect',eventId:event,contactId:C,messageId:'question',snapshot:snapshot(),...extra});
 const inspect=(extra={})=>b.reviewUnsentEvent(OPERATOR,input(extra));
 const retry=(inspection=inspect(),extra={})=>b.reviewUnsentEvent(OPERATOR,input({action:'retry',expectedStateHash:inspection.expected_state_hash,reason:'reviewed_pricing_fix',note:'The current unanswered synthetic pricing request is approved for one normal worker retry.',...extra}));
 const state=()=>l.read('sales_contacts',l.key(C)),changeState=x=>l.write('sales_contacts',l.key(C),{...state(),...x});
 const worker=(extra={})=>{const w=new ServerWorker({bridge:b,adapters:{...adapters,...extra},now:()=>now});workers.push(w);return w;};
 t.after(async()=>{for(const w of workers)await w.stop();b.close();rmSync(dir,{recursive:true,force:true});});
 return {b,l,event,claim,reservation,m,snapshot,inspect,retry,state,changeState,input,worker,get now(){return now;},advance:ms=>now+=ms,get sends(){return sends;}};
}
const all=(b,table)=>b.db.prepare(`SELECT * FROM ${table} ORDER BY 1`).all();

test('one reviewed retry preserves history, revokes old fences, and the normal worker sends once',async t=>{
 const f=fixture(t),tables=['prospect_messages','prospect_entries','prospect_contacts','server_alerts','events'],before=Object.fromEntries(tables.map(t=>[t,all(f.b,t)])),claimBefore=all(f.b,'processing')[0],jobBefore=all(f.b,'server_jobs')[0];
 const a=f.inspect();assert.equal(a.eligible,true);assert.equal(a.mutated,false);assert.equal(all(f.b,'operator_unsent_retries').length,0);
 const r=f.retry(a);assert.equal(r.queued,true);assert.equal(r.no_send,true);assert.equal(r.ledger_rows_changed,0);assert.equal(f.sends,0);
 for(const table of tables)assert.deepEqual(all(f.b,table),before[table]);
 const stored=all(f.b,'operator_unsent_retries')[0],receipt=f.b.vault.open(stored.payload,stored.id);assert.deepEqual(receipt.before.claims.find(c=>c?.event_id===f.event),{...claimBefore});assert.deepEqual(receipt.before.jobs.find(j=>j?.event_id===f.event),{...jobBefore});
 assert.throws(()=>f.b.assertClaim(WORKER,f.event,{claimToken:f.claim.claim_token,allowExpired:true}),e=>e.reason==='processing_claim_fence');assert.throws(()=>f.b.finishServerJob(f.event,f.reservation.job_token,{}),e=>e.reason==='server_job_fence');
 const w=f.worker().start({poll:false});await w.idle();assert.equal(f.sends,1);assert.equal(f.l.rows(C)[0].phase,'sent');assert.equal(all(f.b,'processing')[0].generation,claimBefore.generation+1);assert.equal(all(f.b,'processing')[0].recoveries,claimBefore.recoveries+1);assert.equal(all(f.b,'server_jobs')[0].attempts,jobBefore.attempts+1);
 assert.equal(f.inspect().reason,'operator_retry_already_used');assert.throws(()=>f.retry(),e=>e.reason==='operator_retry_already_used');assert.equal(all(f.b,'operator_unsent_retries')[0].payload,stored.payload);w.wake();await w.idle();assert.equal(f.sends,1);
});

test('claim state changes after inspection reject the exact compare-and-swap',t=>{
 const f=fixture(t),a=f.inspect();f.changeState({manualUntil:f.now+86400000});assert.throws(()=>f.retry(a),e=>e.reason==='state_changed_since_inspection');assert.equal(all(f.b,'operator_unsent_retries').length,0);assert.equal(all(f.b,'server_jobs')[0].state,'needs_review');
});

test('current and malformed consumed attempts, including another uncertain conversation send, never requeue',t=>{
 for(const phase of ['attempt_unknown','queued','sent','delivered','failed','prepared']){
  const f=fixture(t),r=f.l.rows(C)[0];f.l.write('sales_messages',f.l.key(C,r.messageId),{...r,phase,attemptId:'permanent_attempt'});assert.equal(f.inspect().reason,'send_attempt_consumed');assert.throws(()=>f.retry(),e=>e.reason==='send_attempt_consumed');
 }
 const f=fixture(t);f.l.write('sales_messages',f.l.key(C,'other_question'),{contactId:C,messageId:'other_question',eventId:'different_event',phase:'queued',attemptId:'other_attempt'});assert.equal(f.inspect().reason,'consumed_send_requires_reconciliation');
 const g=fixture(t);g.b.db.prepare('UPDATE processing SET send_reserved=1 WHERE event_id=?').run(g.event);assert.equal(g.inspect().reason,'send_attempt_consumed');
});

test('durable and fresh human, consent, client and payment protections remain enforced',t=>{
 for(const [setup,reason] of [[f=>f.changeState({handoff:{reason:'needs_cole'}}),'human_hold_requires_review'],[f=>f.changeState({externalLock:{messageId:'manual'}}),'human_hold_requires_review'],[f=>f.changeState({suppressed:true}),'opt_out_or_dnd'],[f=>f.changeState({suppressionEvidence:{reason:'old_stop'}}),'opt_out_or_dnd'],[f=>f.changeState({manualUntil:f.now+1}),'manual_takeover_24h']]){const f=fixture(t);setup(f);assert.equal(f.inspect().reason,reason);}
 for(const [contact,reason] of [[{dnd:true},'confirmed_dnd'],[{dnd:undefined},'consent_evidence_unknown'],[{type:'customer'},'client_paid_sold_or_conflict'],[{tags:['current customer']},'client_or_suppression_tag']]){const f=fixture(t);assert.equal(f.inspect({snapshot:f.snapshot({contact})}).reason,reason);}
 const f=fixture(t),s=f.snapshot();s.reads.find(r=>r.operationId==='list-transactions').result.data={data:[{_id:'paid',altType:'location',altId:LOCATION,contactId:C,status:'succeeded',liveMode:true}],totalCount:1};assert.equal(f.inspect({snapshot:s}).reason,'client_paid_sold_or_conflict');
 const g=fixture(t),stop=g.m('stop',{dateAdded:new Date(g.now).toISOString(),body:'STOP'});assert.equal(g.inspect({snapshot:g.snapshot({messages:[stop,g.m()]})}).reason,'opt_out_or_dnd');
 const h=fixture(t),manual=h.m('manual',{direction:'outbound',source:'app',dateAdded:new Date(h.now).toISOString()});assert.equal(h.inspect({snapshot:h.snapshot({messages:[manual,h.m()]})}).reason,'manual_takeover_24h');
});

test('wrong operator, scope, stale evidence, earlier cutoff and exhausted counters fail closed',t=>{
 const f=fixture(t);for(const owner of ['','other_operator',WORKER])assert.throws(()=>f.b.reviewUnsentEvent(owner,f.input()),e=>e.reason==='pinned_operator_required');
 assert.equal(f.inspect({contactId:'other_contact'}).reason,'current_server_event_required');assert.equal(f.inspect({messageId:'other_message'}).reason,'latest_unanswered_message_required');
 const s=f.snapshot();s.reads.find(r=>r.operationId==='get-contact').result.data.contact.id='other_contact';assert.equal(f.inspect({snapshot:s}).reason,'contact_scope');
 const old=f.snapshot();f.advance(30001);assert.equal(f.inspect({snapshot:old}).reason,'fresh_snapshot_required');
 f.b.serverWorkerNotBefore=new Date(f.now).toISOString();assert.equal(f.inspect().reason,'current_server_event_required');
 const g=fixture(t);g.b.db.prepare('UPDATE processing SET recoveries=2 WHERE event_id=?').run(g.event);assert.equal(g.inspect().reason,'processing_recovery_exhausted');
});

test('a new question or an unrelated old pending batch cannot hitchhike on the reviewed event',t=>{
 const f=fixture(t),newer=f.m('new_question',{dateAdded:new Date(f.now).toISOString()});assert.equal(f.inspect({snapshot:f.snapshot({messages:[newer,f.m()]})}).reason,'latest_unanswered_message_required');
 const g=fixture(t),older=g.m('older_question',{dateAdded:new Date(Date.parse(CUT)+5000).toISOString()});g.l.write('sales_messages',g.l.key(C,older.id),{contactId:C,messageId:older.id,dateAdded:older.dateAdded,phase:'pending',fingerprint:g.l.messageFingerprint(older)});assert.equal(g.inspect({snapshot:g.snapshot({messages:[g.m(),older]})}).reason,'unrelated_pending_batch');
});

test('normal worker checks fresh consent again after a reviewed retry',async t=>{
 const f=fixture(t);f.retry();const w=f.worker({snapshot:async x=>f.snapshot({...x,contact:{dnd:true}})}).start({poll:false});await w.idle();assert.equal(f.sends,0);assert.equal(f.state().suppressed,true);assert.equal(f.l.rows(C)[0].phase,'prepared');assert.equal(all(f.b,'processing')[0].send_reserved,0);
});

test('retry receipts and source fences survive cleanup and cannot grant a second run',async t=>{
 const f=fixture(t),r=f.retry();const w=f.worker().start({poll:false});await w.idle();const stored=all(f.b,'operator_unsent_retries')[0];f.advance(31*86400000);f.b.cleanup();assert.equal(all(f.b,'operator_unsent_retries')[0].payload,stored.payload);assert.equal(all(f.b,'events').length,1);assert.equal(all(f.b,'processing').length,1);assert.equal(all(f.b,'server_jobs').length,1);assert.equal(f.inspect().reason,'operator_retry_already_used');assert.equal(r.no_send,true);
});

