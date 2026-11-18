// Read-only provider reconciliation of sends with durably recorded provider IDs.
// A queued outbound receipt or a validated send response establishes that ID.
// never sends, retries a send, releases a contact hold, or creates an incoming event.
import {SERVER_WORKER_OWNER} from './bridge.mjs';
import {LOCATION,PROVIDER,validId,utc} from './sales-policy.mjs';
import {hash,canonical} from './security.mjs';

const fail=reason=>{const e=new Error(reason);e.reason=reason;throw e;};
const need=(ok,reason)=>{if(!ok)fail(reason);};
const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
const digest=x=>typeof x==='string'&&/^[a-f0-9]{64}$/.test(x);
const safeReason=e=>/^[a-z][a-z0-9_]{0,100}$/.test(e?.reason??'')?e.reason:'reconciliation_failed';
const states=new Map([['sent',['sent']],['delivered',['delivered','read']],['failed',['failed']]]);
const recoverable=row=>row.phase==='queued'||row.phase==='attempt_unknown'&&row.sendResponseRecorded===true&&validId(row.sendResponseConversationId);

function storedAttempt(b,{eventId,attemptId,outboundId},{allowReconciledTerminal=false}={}){
 need(b.processingMode==='server'&&validId(eventId)&&validId(attemptId)&&validId(outboundId),'reconciliation_scope');
 const claim=b.db.prepare('SELECT * FROM processing WHERE event_id=? AND owner=?').get(eventId,SERVER_WORKER_OWNER);
 const job=b.db.prepare('SELECT * FROM server_jobs WHERE event_id=?').get(eventId);
 need(claim?.send_reserved===1&&claim.generation>0&&digest(claim.claim_token_hash)&&job&&job.state!=='running'&&(claim.completed!==null||Number.isFinite(claim.lease_until)&&claim.lease_until<=b.now()),'reconciliation_original_claim_required');
 const event=b.eventForOwner(SERVER_WORKER_OWNER,eventId),contact=event.data.contact_id;
 need(event.data.location_id===LOCATION&&event.data.source==='loop_workflow'&&validId(contact),'reconciliation_scope');
 const ledger=b.prospectV1,entry=ledger.read('prospect_entries',ledger.key(contact));
 need(entry?.contactId===contact&&validId(entry.conversationId),'reconciliation_entry_required');
 const batch=ledger.rows(contact).filter(r=>r.eventId===eventId&&r.attemptId===attemptId);
 need(batch.length>0&&batch.length<=500,'reconciliation_stored_attempt_required');
 const row=batch[0],ids=row.batchMessageIds??[row.messageId],anchor=row.anchorMessageId??row.messageId;
 need(Array.isArray(ids)&&ids.length===batch.length&&new Set(ids).size===ids.length&&ids.every(validId)&&ids.includes(anchor)&&validId(anchor),'reconciliation_batch_mismatch');
 need(Number.isSafeInteger(row.attemptAt)&&row.attemptAt>=0&&row.attemptAt<=b.now()&&digest(row.bodyHash),'reconciliation_stored_attempt_required');
 const terminal=allowReconciledTerminal&&states.has(row.phase)&&object(row.reconciliationReceipt);
 need(batch.every(r=>(recoverable(r)||terminal&&canonical(r.reconciliationReceipt)===canonical(row.reconciliationReceipt))&&r.phase===row.phase&&(r.phase!=='attempt_unknown'||r.sendResponseConversationId===entry.conversationId)&&r.outboundId===outboundId&&r.messageId!==outboundId&&r.contactId===contact&&r.attemptAt===row.attemptAt&&r.bodyHash===row.bodyHash&&ids.includes(r.messageId)&&canonical(r.batchMessageIds??[r.messageId])===canonical(ids)&&(r.anchorMessageId??r.messageId)===anchor),'reconciliation_batch_mismatch');
 if(terminal){
  const receipt=row.reconciliationReceipt,m=receipt.outbound;
  need(receipt.outcome===row.phase&&receipt.send_response?.messageId===outboundId&&receipt.send_response?.conversationId===entry.conversationId&&object(m)&&m.id===outboundId&&m.locationId===LOCATION&&m.contactId===contact&&m.conversationId===entry.conversationId&&m.conversationProviderId===PROVIDER&&m.messageType==='TYPE_CUSTOM_PROVIDER_SMS'&&m.direction==='outbound'&&m.bodyHash===row.bodyHash&&states.get(row.phase).includes(m.status),'reconciliation_terminal_receipt_mismatch');
  need(utc(m.dateAdded)>=row.attemptAt&&utc(m.dateAdded)<=b.now(),'reconciliation_outbound_time');
 }
 return {ledger,entry,event,batch,row,anchor,terminal};
}

function validateOutbound(b,x,m,outboundId){
 need(object(m)&&m.id===outboundId&&m.locationId===LOCATION&&m.contactId===x.entry.contactId&&m.conversationId===x.entry.conversationId&&m.conversationProviderId===PROVIDER&&m.messageType==='TYPE_CUSTOM_PROVIDER_SMS'&&m.direction==='outbound'&&typeof m.body==='string'&&hash(m.body)===x.row.bodyHash,'reconciliation_outbound_mismatch');
 const at=utc(m.dateAdded);need(at>=x.row.attemptAt&&at<=b.now(),'reconciliation_outbound_time');
}

// Internal-only, narrowly scoped counterpart to ledger.record(). The original
// fenced claim token is deliberately not recoverable from its stored hash.
// Validate the same complete result against an unchanged identified batch atomically;
// do not bypass claim fencing for any open processing action or expose this via RPC.
export function recordReconciledProspectSend(bridge,{eventId,attemptId,outboundId,result}={}){
 return bridge.prospectV1.tx(()=>{
  const x=storedAttempt(bridge,{eventId,attemptId,outboundId});
  need(object(result)&&states.has(result.outcome)&&(result.attempt_id===undefined||result.attempt_id===attemptId),'reconciliation_result_required');
  need(result.send_response?.messageId===outboundId&&result.send_response?.conversationId===x.entry.conversationId,'reconciliation_receipt_mismatch');
  validateOutbound(bridge,x,result.outbound,outboundId);
  need(states.get(result.outcome).includes(result.outbound.status),'reconciliation_status_mismatch');
  // Persist the exact verified receipt fields (body hash only) with the terminal
  // ledger write. Restart can then settle an unfinished journal without a new
  // send or treating a successfully verified send as a technical failure.
  const m=result.outbound,reconciliationReceipt={outcome:result.outcome,send_response:{messageId:outboundId,conversationId:x.entry.conversationId},outbound:{id:m.id,locationId:m.locationId,contactId:m.contactId,conversationId:m.conversationId,conversationProviderId:m.conversationProviderId,messageType:m.messageType,direction:m.direction,bodyHash:hash(m.body),dateAdded:m.dateAdded,status:m.status}};
  for(const row of x.batch)x.ledger.write('sales_messages',x.ledger.key(x.entry.contactId,row.messageId),{...row,phase:result.outcome,outboundId,reconciliationReceipt});
  bridge.audit('server_send_reconciled',eventId);
  return {recorded:true,phase:result.outcome,message_ids:x.batch.map(r=>r.messageId),retry_allowed:false};
 });
}

export class SendReconciler {
 constructor({bridge,ghl,recordVerified=null,intervalMs=15000,maxChecks=20,maxAgeMs=86400000,batchSize=2,scanSize=100,readTimeoutMs=8000,now=()=>bridge.now()}={}){
  need(bridge?.processingMode==='server'&&typeof ghl?.execute==='function'&&typeof ghl?.verify==='function','reconciliation_configuration');
  need(Number.isSafeInteger(intervalMs)&&intervalMs>=15000&&intervalMs<=300000&&Number.isSafeInteger(maxChecks)&&maxChecks>=1&&maxChecks<=100&&Number.isSafeInteger(maxAgeMs)&&maxAgeMs>=15000&&maxAgeMs<=7*86400000&&Number.isSafeInteger(batchSize)&&batchSize>=1&&batchSize<=5&&Number.isSafeInteger(scanSize)&&scanSize>=1&&scanSize<=500&&Number.isSafeInteger(readTimeoutMs)&&readTimeoutMs>=1&&readTimeoutMs<=10000&&typeof now==='function'&&(recordVerified===null||typeof recordVerified==='function'),'reconciliation_configuration');
  this.b=bridge;this.ghl=ghl;this.recordVerified=recordVerified??(r=>recordReconciledProspectSend(bridge,r));this.intervalMs=intervalMs;this.maxChecks=maxChecks;this.maxAgeMs=maxAgeMs;this.batchSize=batchSize;this.scanSize=scanSize;this.readTimeoutMs=readTimeoutMs;this.now=now;
  this.running=false;this.busy=false;this.timer=null;this.pending=null;this.scanCursor=0;this.nextTick=0;this.lastError=null;
  bridge.db.exec(`CREATE TABLE IF NOT EXISTS server_send_reconciliation(attempt_key TEXT PRIMARY KEY,message_key TEXT NOT NULL,event_id TEXT NOT NULL,state TEXT NOT NULL,checks INTEGER NOT NULL DEFAULT 0,due INTEGER NOT NULL,lease_until INTEGER,reason TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS server_send_reconciliation_due ON server_send_reconciliation(state,due);
    CREATE TABLE IF NOT EXISTS server_send_reconciliation_meta(id TEXT PRIMARY KEY,paused_until INTEGER NOT NULL);
    INSERT OR IGNORE INTO server_send_reconciliation_meta(id,paused_until) VALUES('provider',0);`);
 }
 start({poll=true}={}){if(this.running)return this;this.running=true;this.wake();if(poll){this.timer=setInterval(()=>this.wake(),this.intervalMs);this.timer.unref?.();}return this;}
 wake(){if(!this.running||this.busy)return;this.pending=this.tick().catch(e=>{this.lastError=safeReason(e);this.b.audit('server_reconciliation_failed');});}
 async idle(){if(this.pending)await this.pending;}
 async stop(){this.running=false;if(this.timer)clearInterval(this.timer);this.timer=null;await this.idle();}
 status(){return {running:this.running,busy:this.busy,poll_interval_ms:this.intervalMs,max_checks_per_attempt:this.maxChecks,last_error:this.lastError,queued:this.b.db.prepare("SELECT count(*) n FROM server_send_reconciliation WHERE state='pending'").get().n};}
 discover(){
  // Bound scanning too: encrypted ledger rows cannot be filtered by phase in SQL.
  // Rotate through existing rows; an unknown record without a receipt stays put.
  const rows=this.b.db.prepare('SELECT rowid,id,payload FROM prospect_messages WHERE rowid>? ORDER BY rowid LIMIT ?').all(this.scanCursor,this.scanSize);
  this.scanCursor=rows.length===this.scanSize?rows.at(-1).rowid:0;
  for(const saved of rows){
   const row=this.b.vault.open(saved.payload,saved.id);
   if(!recoverable(row)||!validId(row.outboundId)||!validId(row.eventId)||!validId(row.attemptId))continue;
   try{storedAttempt(this.b,{eventId:row.eventId,attemptId:row.attemptId,outboundId:row.outboundId});}catch{continue;}
   this.b.db.prepare("INSERT OR IGNORE INTO server_send_reconciliation(attempt_key,message_key,event_id,state,due,reason) VALUES(?,?,?,'pending',?,?)").run(hash(canonical({eventId:row.eventId,attemptId:row.attemptId})),saved.id,row.eventId,this.now(),row.phase==='queued'?'queued_send':'send_verification_pending');
  }
 }
 reserve(){
  return this.b.prospectV1.tx(()=>{
   const at=this.now();if(this.b.db.prepare("SELECT paused_until FROM server_send_reconciliation_meta WHERE id='provider'").get().paused_until>at)return null;
   const row=this.b.db.prepare("SELECT * FROM server_send_reconciliation WHERE state='pending' AND due<=? AND (lease_until IS NULL OR lease_until<=?) ORDER BY due,attempt_key LIMIT 1").get(at,at);
   if(!row)return null;
   // Settlement opens its own bridge transaction, so return the fenced journal
   // row and settle after this reservation transaction has committed.
   if(row.checks>=this.maxChecks)return {...row,exhausted:true};
   this.b.db.prepare('UPDATE server_send_reconciliation SET checks=checks+1,lease_until=? WHERE attempt_key=?').run(at+60000,row.attempt_key);
   return {...row,checks:row.checks+1};
  });
 }
 finish(job,state,reason,due=this.now()){
  // checks is also the lease generation: a late old reader cannot overwrite the
  // diagnostic outcome of a newer process that acquired the expired lease.
  // Commit journal and bridge job together. A crash must not leave a completed
  // journal with a permanently waiting server job. Bridge settlement is a nested
  // savepoint; no network or asynchronous work runs inside this transaction.
  this.b.db.exec('SAVEPOINT send_reconciliation_finish');
  try{
   const changed=this.b.db.prepare('UPDATE server_send_reconciliation SET state=?,due=?,lease_until=NULL,reason=? WHERE attempt_key=? AND checks=?').run(state,due,reason,job.attempt_key,job.checks);
   if(changed.changes&&['completed','needs_review'].includes(state))this.b.settleReconciliationJob?.(job.event_id,{outcome:state==='completed'?reason.replace(/^verified_/,''):reason==='reconciliation_checks_exhausted'?'exhausted':reason==='reconciliation_window_elapsed'?'window_elapsed':'validation_failure',reason});
   this.b.db.exec('RELEASE SAVEPOINT send_reconciliation_finish');
  }catch(e){this.b.db.exec('ROLLBACK TO SAVEPOINT send_reconciliation_finish; RELEASE SAVEPOINT send_reconciliation_finish');throw e;}
 }
 async read(fn){const controller=new AbortController();let timer;try{return await Promise.race([Promise.resolve().then(()=>fn(controller.signal)),new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();const e=new Error('ghl_request_timeout');e.reason='ghl_request_timeout';reject(e);},this.readTimeoutMs);})]);}finally{clearTimeout(timer);}}
 async tick(){
  if(this.busy||this.now()<this.nextTick||!this.b.enabled||this.b.processingMode!=='server')return {checked:0};
  this.busy=true;this.nextTick=this.now()+this.intervalMs;let checked=0;
  try{
   this.discover();
   for(let i=0;i<this.batchSize;i++){
    const job=this.reserve();if(!job)break;
    try{
     const row=this.b.prospectV1.read('sales_messages',job.message_key);
     need(row,'reconciliation_stored_attempt_required');
     const ids={eventId:row.eventId,attemptId:row.attemptId,outboundId:row.outboundId};
     need(hash(canonical({eventId:row.eventId,attemptId:row.attemptId}))===job.attempt_key,'reconciliation_stored_attempt_required');
     const x=storedAttempt(this.b,ids,{allowReconciledTerminal:true});
     if(x.terminal){this.finish(job,'completed','verified_'+x.row.phase);this.lastError=null;continue;}
     if(job.exhausted){this.finish(job,'needs_review','reconciliation_checks_exhausted');continue;}
     if(this.now()-x.row.attemptAt>this.maxAgeMs){this.finish(job,'needs_review','reconciliation_window_elapsed');continue;}
     checked++;
     const raw=await this.read(signal=>typeof this.ghl.readExactMessage==='function'
      ?this.ghl.readExactMessage({id:row.outboundId,contactId:x.entry.contactId,conversationId:x.entry.conversationId,signal})
      :this.ghl.execute({operationId:'get-message',locationId:LOCATION,params:{path:{id:row.outboundId}},signal}));
     need(raw?.operationId==='get-message'&&raw.success===true&&raw.status===200,'reconciliation_read_required');
     const m=raw.data?.message;validateOutbound(this.b,x,m,row.outboundId);
     // The message ID was persisted by the original queued/send-response record.
     // Recover text only from this exact verified body-hash match, never guess it.
     // This expired verification-only permit cannot authorize a new adapter send.
     const permit={permitted:true,attempt_id:row.attemptId,message_id:x.anchor,expires_at:new Date(row.attemptAt+15000).toISOString(),operationId:'send-a-new-message',locationId:LOCATION,idempotencyKey:row.attemptId,params:{body:{type:'Custom',contactId:x.entry.contactId,conversationProviderId:PROVIDER,message:m.body,replyMessageId:x.anchor}}};
     const result=await this.read(signal=>this.ghl.verify({messageId:row.outboundId,conversationId:x.entry.conversationId},{permit,event:x.event,signal}));
     validateOutbound(this.b,x,result?.outbound,row.outboundId);
     if(states.has(result.outcome)){
      const recorded=await this.recordVerified({...ids,result});need(recorded?.recorded===true&&recorded.phase===result.outcome,'reconciliation_record_failed');
      this.finish(job,'completed','verified_'+result.outcome);this.lastError=null;
     }else{
      const retry=result.outcome==='queued'&&['queued','pending'].includes(result.outbound.status);
      this.finish(job,retry&&job.checks<this.maxChecks?'pending':'needs_review',retry?(job.checks<this.maxChecks?'still_queued':'reconciliation_checks_exhausted'):'provider_status_unknown',this.now()+Math.min(300000,this.intervalMs*2**Math.min(job.checks-1,10)));
     }
    }catch(e){
     const reason=safeReason(e);this.lastError=reason;
     if(reason==='provider_rate_limited'||e?.status===429){
      const due=this.now()+60000;this.b.db.prepare("UPDATE server_send_reconciliation_meta SET paused_until=MAX(paused_until,?) WHERE id='provider'").run(due);
      this.finish(job,job.checks<this.maxChecks?'pending':'needs_review','provider_rate_limited',due);break;
     }
     const transient=['ghl_network_failure','ghl_request_timeout','ghl_request_aborted','ghl_exact_message_not_found'].includes(reason)||reason==='ghl_http_error'&&(e?.status===404||e?.status>=500&&e?.status<=599);
     const retry=transient&&job.checks<this.maxChecks;
     this.finish(job,retry?'pending':'needs_review',transient&&!retry?'reconciliation_checks_exhausted':reason,this.now()+this.intervalMs);
    }
   }
   return {checked};
  }finally{this.busy=false;}
 }
}

