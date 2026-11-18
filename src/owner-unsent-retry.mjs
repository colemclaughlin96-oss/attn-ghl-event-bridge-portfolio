// One explicitly reviewed retry of one unanswered server event. No network I/O.
// Prior headers are retained in an immutable encrypted receipt; sends still use
// the normal worker, fresh evidence, and the original per-message permit ledger.
import crypto from 'node:crypto';
import {canonical,hash} from './security.mjs';
import {LOCATION,PROVIDER,validId,utc,blocked,consentEvidence,explicitOptOut} from './sales-policy.mjs';
const WORKER='attn_server_worker_v1',DAY=86400000;
const RECOVERABLE=new Set(['approved_price_facts_required','assessment_schema_invalid','model_response_invalid','generation_schema_invalid']);
const TAKEOVER_TYPES=new Set(['TYPE_SMS','TYPE_SMS_REVIEW_REQUEST','TYPE_SMS_NO_SHOW_REQUEST','TYPE_CAMPAIGN_SMS','TYPE_CAMPAIGN_MANUAL_SMS','TYPE_EMAIL','TYPE_CUSTOM_SMS','TYPE_CUSTOM_EMAIL','TYPE_CUSTOM_PROVIDER_SMS','TYPE_CUSTOM_PROVIDER_EMAIL','TYPE_WHATSAPP','TYPE_FACEBOOK','TYPE_INSTAGRAM','TYPE_GMB','TYPE_WEBCHAT','TYPE_LIVE_CHAT']);
const attempted=r=>['attempt_unknown','queued','sent','delivered','failed'].includes(r.phase)||['attemptId','attemptAt','outboundId','sendResponseRecorded','sendResponseConversationId'].some(k=>Object.hasOwn(r,k));
function capture(b,eventId,contactId){
 const l=b.prospectV1,key=l.key(contactId),source=b.db.prepare('SELECT payload FROM events WHERE id=?').get(eventId);
 const events=b.db.prepare('SELECT id,payload FROM events ORDER BY created,id').all().map(r=>b.vault.open(r.payload,r.id)).filter(e=>e.data.contact_id===contactId);
 return {event:source?b.vault.open(source.payload,eventId):null,entry:l.read('prospect_entries',key),state:l.read('sales_contacts',key),messages:l.rows(contactId).sort((a,z)=>a.messageId.localeCompare(z.messageId)),
  claims:events.map(e=>b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(e.eventId)??null),jobs:events.map(e=>b.db.prepare('SELECT * FROM server_jobs WHERE event_id=?').get(e.eventId)??null),
  priorRetry:b.db.prepare('SELECT * FROM operator_unsent_retries WHERE event_id=?').get(eventId)??null};
}
function verify(b,input,x){
 const {eventId,contactId,messageId,snapshot}=input,l=b.prospectV1,now=b.now(),{event,entry,state,messages:rows}=x;
 if(b.processingMode!=='server'||!b.enabled||!l.enabled)blocked('server_replies_required');
 if(x.priorRetry)blocked('operator_retry_already_used');
 if(!event||event.eventId!==eventId||event.data.contact_id!==contactId||event.data.location_id!==LOCATION||event.data.source!=='loop_workflow'||utc(event.timestamp)<=utc(b.serverWorkerNotBefore)||now-utc(event.timestamp)>DAY)blocked('current_server_event_required');
 const claim=x.claims.find(c=>c?.event_id===eventId),job=x.jobs.find(j=>j?.event_id===eventId);
 if(!claim||claim.owner!==WORKER||claim.completed===null||claim.outcome!=='escalated'||!job||!['failed','needs_review'].includes(job.state)||!RECOVERABLE.has(job.reason))blocked('terminal_unsent_failure_required');
 if(!Number.isSafeInteger(claim.generation)||claim.generation<1||!claim.claim_token_hash||!Number.isSafeInteger(claim.recoveries)||claim.recoveries<0||claim.recoveries>=2||!Number.isSafeInteger(claim.deferrals)||claim.deferrals<0||claim.deferrals>=2||!Number.isSafeInteger(job.attempts)||job.attempts<1||job.attempts>=3)blocked('processing_recovery_exhausted');
 if(claim.send_reserved||b.eventHasConsumedAttempt(eventId)||rows.some(r=>r.eventId===eventId&&attempted(r)))blocked('send_attempt_consumed');
 if(rows.some(r=>['attempt_unknown','queued','failed'].includes(r.phase)||attempted(r)&&!['sent','delivered'].includes(r.phase)))blocked('consumed_send_requires_reconciliation');
 if(x.claims.some(c=>c&&c.completed===null)||x.jobs.some(j=>j&&['running','pending','reconciliation_pending'].includes(j.state)))blocked('processing_still_open');
 if(x.claims.some(c=>c?.send_reserved&&!rows.some(r=>r.eventId===c.event_id&&['sent','delivered'].includes(r.phase))))blocked('consumed_send_requires_reconciliation');
 if(!entry||entry.contactId!==contactId||!state||state.contactId!==contactId||state.enrollmentHash!==hash(canonical(entry)))blocked('verified_contact_state_required');
 if(state.suppressed||state.suppressionEvidence)blocked('opt_out_or_dnd');
 if(state.handoff||state.externalLock)blocked('human_hold_requires_review');
 if(!Number.isFinite(state.manualUntil)||state.manualUntil<0)blocked('manual_state_unknown');
 if(state.manualUntil>now)blocked('manual_takeover_24h');
 const {scope,s}=l.adapt(event,snapshot);
 if(scope.conversationId!==entry.conversationId)blocked('pinned_conversation_changed');
 const {messages}=l.snapshot(event,entry,s),consent=consentEvidence(s.contact),boundary=l.incomingBoundary(entry);
 if(consent.status!=='allowed')blocked(consent.reason);
 if(!Array.isArray(s.assessments)||new Set(s.assessments.map(a=>a.messageId)).size!==s.assessments.length)blocked('message_assessments_required');
 for(const m of messages.filter(m=>m.direction==='inbound'&&m.at>utc(entry.notBefore))){
  const a=s.assessments.find(a=>a.messageId===m.id);
  if(!a||!['normal','opt_out','system','handoff','unsafe'].includes(a.kind))blocked('inbound_assessment_required');
  if(explicitOptOut(m.body)||a.kind==='opt_out')blocked('opt_out_or_dnd');
  if(m.at>boundary&&['handoff','unsafe'].includes(a.kind))blocked('new_handoff_requires_review');
 }
 const candidate=messages.find(m=>m.id===messageId),stored=rows.find(r=>r.messageId===messageId),latest=messages.find(m=>m.direction==='inbound');
 if(!candidate||candidate.direction!=='inbound'||candidate.conversationProviderId!==PROVIDER||candidate.messageType!=='TYPE_CUSTOM_PROVIDER_SMS'||candidate.at<=boundary||latest?.id!==messageId||s.assessments.find(a=>a.messageId===messageId)?.kind!=='normal')blocked('latest_unanswered_message_required');
 if(!stored||!['pending','prepared'].includes(stored.phase)||attempted(stored)||stored.fingerprint!==l.messageFingerprint(candidate)||stored.dateAdded!==candidate.dateAdded||!stored.receiptIds?.includes(eventId)||stored.eventId&&stored.eventId!==eventId)blocked('original_unsent_message_required');
 const pending=rows.filter(r=>['pending','prepared'].includes(r.phase)&&utc(r.dateAdded)>boundary);
 if(pending.length!==1||pending[0].messageId!==messageId||stored.batchMessageIds&&(stored.batchMessageIds.length!==1||stored.batchMessageIds[0]!==messageId)||stored.anchorMessageId&&stored.anchorMessageId!==messageId)blocked('unrelated_pending_batch');
 // A new unobserved inbound must not get silently added to the old event's batch.
 if(messages.some(m=>m.direction==='inbound'&&m.at>boundary&&m.id!==messageId&&!rows.some(r=>r.messageId===m.id&&['sent','delivered','ignored_system','suppressed'].includes(r.phase))))blocked('unrelated_pending_batch');
 const permission=l.permission({event,entry,state,snapshot:s,messages:messages.filter(m=>m.at>boundary)});
 if(permission)blocked(permission);
 const previous=l.currentMessages;l.currentMessages=messages;let known;
 try{known=l.knownOutboundIds(entry,rows);}finally{l.currentMessages=previous;}
 const external=messages.filter(m=>m.direction==='outbound'&&TAKEOVER_TYPES.has(m.messageType)&&m.at>=Math.min(utc(entry.notBefore),now-DAY)&&!known.has(m.id));
 if(external.some(m=>m.at+DAY>now))blocked('manual_takeover_24h');
 if(external.some(m=>!l.isVerifiedManual(m)))blocked('unverified_external_sender');
 return {claim,job,snapshot:s,candidate};
}
export function reviewUnsentEvent(b,operator,input={}){
 if(typeof operator!=='string'||!operator||operator!==b.operatorOwner||operator===WORKER)blocked('pinned_operator_required');
 const {action,eventId,contactId,messageId,expectedStateHash,reason,note}=input;
 if(!['inspect','retry'].includes(action)||![eventId,contactId,messageId].every(validId))blocked('unsent_retry_identity_required');
 if(action==='inspect'){
  const x=capture(b,eventId,contactId);let why=null;
  try{verify(b,input,x);}catch(e){why=e.reason??'unsent_retry_inspection_failed';}
  return {event_id:eventId,contact_id:contactId,message_id:messageId,eligible:why===null,reason:why,expected_state_hash:hash(canonical(x)),mutated:false};
 }
 if(typeof expectedStateHash!=='string'||!/^[a-f0-9]{64}$/.test(expectedStateHash)||typeof reason!=='string'||!/^[a-z][a-z0-9_]{0,100}$/.test(reason)||typeof note!=='string'||!note.trim()||note.length>2000)blocked('owner_review_details_required');
 return b.prospectV1.tx(()=>{
  const before=capture(b,eventId,contactId);if(hash(canonical(before))!==expectedStateHash)blocked('state_changed_since_inspection');
  const verified=verify(b,input,before),at=b.now(),id='retry_'+hash(canonical({eventId,expectedStateHash,at}));
  const receipt={version:1,id,event_id:eventId,contact_id:contactId,message_id:messageId,location_id:LOCATION,operator,reason,note:note.trim(),created_at:new Date(at).toISOString(),before_hash:expectedStateHash,before,snapshot_hash:hash(canonical(verified.snapshot)),target_fingerprint:b.prospectV1.messageFingerprint(verified.candidate),no_send:true,ledger_rows_changed:0};
  b.db.prepare('INSERT INTO operator_unsent_retries(event_id,id,created,payload) VALUES(?,?,?,?)').run(eventId,id,at,b.vault.seal(receipt,id));
  // Revoke both old fences before reopening this proven-unsent event. All retry
  // counters remain monotonic; normal claim() issues the next generation/token.
  b.db.prepare('UPDATE processing SET completed=NULL,outcome=NULL,claim_token_hash=?,lease_until=?,recovery_deadline=?,deferred_until=NULL,retry_check_at=NULL WHERE event_id=?').run(hash(crypto.randomBytes(32).toString('base64url')),at,at+120000,eventId);
  b.db.prepare("UPDATE server_jobs SET state='pending',due=?,updated=?,lease_token_hash=NULL,lease_until=NULL,reason='owner_reviewed_unsent_retry' WHERE event_id=?").run(at,at,eventId);
  b.traceProcessing(WORKER,eventId,verified.claim.generation,'deferred','owner_reviewed_unsent_retry');b.audit('owner_unsent_retry_queued',id);
  return {queued:true,retry_receipt:id,event_id:eventId,contact_id:contactId,message_id:messageId,no_send:true,ledger_rows_changed:0,retry_deadline:new Date(at+120000).toISOString()};
 });
}

