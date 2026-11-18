// Authenticated owner review only. No network calls, sends, replay, or broad reset.
import {canonical,hash} from './security.mjs';
import {LOCATION,validId,utc,blocked,consentEvidence,explicitOptOut} from './sales-policy.mjs';
import {SalesReplyLedger} from './sales-reply.mjs';
const DAY=86400000;
const TAKEOVER_TYPES=new Set(['TYPE_SMS','TYPE_SMS_REVIEW_REQUEST','TYPE_SMS_NO_SHOW_REQUEST','TYPE_CAMPAIGN_SMS','TYPE_CAMPAIGN_MANUAL_SMS','TYPE_EMAIL','TYPE_CUSTOM_SMS','TYPE_CUSTOM_EMAIL','TYPE_CUSTOM_PROVIDER_SMS','TYPE_CUSTOM_PROVIDER_EMAIL','TYPE_WHATSAPP','TYPE_FACEBOOK','TYPE_INSTAGRAM','TYPE_GMB','TYPE_WEBCHAT','TYPE_LIVE_CHAT']);
const fingerprint=m=>hash(canonical({id:m.id,locationId:m.locationId,contactId:m.contactId,conversationId:m.conversationId,dateAdded:m.dateAdded,body:m.body??null,attachments:m.attachments??null,provider:m.conversationProviderId??null,messageType:m.messageType,direction:m.direction,source:m.source??null,userId:m.userId??null}));
const read=(b,t,k)=>{const r=b.db.prepare(`SELECT payload FROM ${t} WHERE id=?`).get(k);return r?b.vault.open(r.payload,k):null;};
function authorize(b,operator){if(typeof operator!=='string'||!operator||operator!==b.operatorOwner||operator==='attn_server_worker_v1')blocked('pinned_operator_required');}
function receiptFor(b,entry){
 const id=entry?.ownerHoldResolutionRef;if(!validId(id))return null;
 const r=read(b,'prospect_owner_resolutions',id);
 if(!r||r.version!==1||r.resolution!==id||r.contact_id!==entry.contactId||r.conversation_id!==entry.conversationId||r.location_id!==LOCATION||!Array.isArray(r.reviewed_external_messages)||r.resume_after!==entry.resumeAfter)blocked('owner_resolution_receipt_mismatch');
 return r;
}
// The caller must still enforce its normal latest-manual pause. Only the exact
// owner-reviewed historical evidence is recognized; changed source/content is new.
export function reviewedExternalIds(b,entry,messages){
 const receipt=receiptFor(b,entry);if(!receipt)return [];
 return messages.filter(m=>m.contactId===entry.contactId&&m.conversationId===entry.conversationId&&m.locationId===LOCATION&&m.direction==='outbound'&&utc(m.dateAdded)+DAY<=utc(receipt.resume_after)&&receipt.reviewed_external_messages.some(r=>r.message_id===m.id&&r.fingerprint===fingerprint(m))).map(m=>m.id);
}
export function releasedAlertHash(b,entry){return receiptFor(b,entry)?.released_alert_hash??null;}
function capture(b,contactId){
 const l=b.prospectV1,key=l.key(contactId),entry=l.read('prospect_entries',key),state=l.read('sales_contacts',key),alert=l.read('prospect_alerts',key);
 const events=b.db.prepare('SELECT id,payload FROM events ORDER BY created,id').all().map(r=>b.vault.open(r.payload,r.id)).filter(e=>e.data.contact_id===contactId);
 const messages=l.rows(contactId).sort((a,z)=>a.messageId.localeCompare(z.messageId));
 const claims=events.map(e=>b.db.prepare('SELECT * FROM processing WHERE event_id=?').get(e.eventId)??null);
 const jobs=events.map(e=>b.db.prepare('SELECT * FROM server_jobs WHERE event_id=?').get(e.eventId)??null);
 return {entry,state,alert,messages,events,claims,jobs,previous:entry?receiptFor(b,entry):null};
}
function verify(b,contactId,x,raw){
 const l=b.prospectV1,now=b.now(),{entry,state}=x;
 if(!entry||entry.contactId!==contactId||!state||state.contactId!==contactId||state.enrollmentHash!==hash(canonical(entry)))blocked('verified_contact_state_required');
 if(!state.handoff&&!state.externalLock)blocked('no_reviewable_hold');
 if(state.suppressed||state.suppressionEvidence)blocked('opt_out_or_dnd');
 if(!Number.isFinite(state.manualUntil)||state.manualUntil<0)blocked('manual_state_unknown');
 if(state.manualUntil>now)blocked('manual_takeover_24h');
 if(l.exclusions.some(e=>e.contactId===contactId))blocked('owner_excluded_client_or_conflict');
 if(x.messages.some(r=>['attempt_unknown','queued','failed'].includes(r.phase)||(r.attemptId||r.attemptAt!==undefined)&&!['sent','delivered'].includes(r.phase)))blocked('consumed_send_requires_reconciliation');
 if(x.claims.some(c=>c&&c.completed===null)||x.jobs.some(j=>j?.state==='running'))blocked('processing_still_open');
 if(x.claims.some(c=>c?.send_reserved&&!x.messages.some(r=>r.eventId===c.event_id&&['sent','delivered'].includes(r.phase))))blocked('consumed_send_requires_reconciliation');
 const event=x.events.at(-1);if(!event||event.data.location_id!==LOCATION||event.data.source!=='loop_workflow')blocked('verified_contact_event_required');
 const {scope,s}=l.adapt(event,raw);
 if(scope.conversationId!==entry.conversationId)blocked('pinned_conversation_changed');
 const {messages}=l.snapshot(event,entry,s),consent=consentEvidence(s.contact);
 if(consent.status!=='allowed')blocked(consent.reason);
 if(!Array.isArray(s.assessments)||new Set(s.assessments.map(a=>a.messageId)).size!==s.assessments.length)blocked('message_assessments_required');
 for(const m of messages.filter(m=>m.direction==='inbound'&&m.at>utc(entry.notBefore))){
  const assessment=s.assessments.find(a=>a.messageId===m.id);
  if(!assessment||!['normal','opt_out','system','handoff','unsafe'].includes(assessment.kind))blocked('inbound_assessment_required');
  if(explicitOptOut(m.body)||assessment.kind==='opt_out')blocked('opt_out_or_dnd');
  if(['handoff','unsafe'].includes(assessment.kind)&&m.at>(state.handoff?.at??0))blocked('new_handoff_requires_review');
 }
 // Evaluate the existing policy with only the reviewed human hold removed in
 // memory. Consent, client/payment, provider, scope and appointment gates stay.
 const reason=SalesReplyLedger.prototype.permission.call(l,{event,entry,state:{...state,handoff:null,externalLock:null,consentHold:null},snapshot:s,messages});
 if(reason)blocked(reason);
 l.currentMessages=messages;
 let known;try{known=l.knownOutboundIds(entry,x.messages);}finally{l.currentMessages=null;}
 for(const id of reviewedExternalIds(b,entry,messages))known.add(id);
 const unknown=messages.filter(m=>m.direction==='outbound'&&TAKEOVER_TYPES.has(m.messageType)&&m.at>=Math.min(utc(entry.notBefore),now-DAY)&&!known.has(m.id));
 if(unknown.some(m=>m.at+DAY>now))blocked('manual_takeover_24h');
 if(state.externalLock&&!unknown.some(m=>m.id===state.externalLock.messageId))blocked('external_lock_evidence_required');
 if(state.handoff?.at>now||entry.resumeAfter&&utc(entry.resumeAfter)>=now||utc(entry.notBefore)>=now)blocked('resolution_clock_invalid');
 const reviewed=unknown.filter(m=>!l.isVerifiedManual(m)).map(m=>({message_id:m.id,date_added:m.dateAdded,fingerprint:fingerprint(m)}));
 return {event,snapshot:s,messages,reviewed};
}
export function inspectOwnerHold(b,operator,{contactId,snapshot}={}){
 authorize(b,operator);if(!validId(contactId))blocked('contact_id_required');
 const x=capture(b,contactId);let reason=null,verified=null;
 try{verified=verify(b,contactId,x,snapshot);}catch(e){reason=e.reason??'hold_inspection_failed';}
 return {contact_id:contactId,eligible:reason===null,reason,expected_state_hash:hash(canonical(x)),handoff:x.state?.handoff??null,external_lock:x.state?.externalLock??null,manual_until:x.state?.manualUntil??null,suppressed:x.state?.suppressed??false,message_phases:x.messages.map(r=>({message_id:r.messageId,phase:r.phase})),reviewed_external_messages:verified?.reviewed??[],resumes_on_new_inbound_only:true,mutated:false};
}
export function resolveOwnerHold(b,operator,{contactId,snapshot,expectedStateHash,reason,note}={}){
 authorize(b,operator);
 if(!validId(contactId)||typeof expectedStateHash!=='string'||!/^[a-f0-9]{64}$/.test(expectedStateHash)||typeof reason!=='string'||!/^[a-z][a-z0-9_]{0,100}$/.test(reason)||typeof note!=='string'||!note.trim()||note.length>2000)blocked('owner_review_details_required');
 return b.prospectV1.tx(()=>{
  const before=capture(b,contactId);if(hash(canonical(before))!==expectedStateHash)blocked('state_changed_since_inspection');
  const checked=verify(b,contactId,before,snapshot),at=b.now(),resumeAfter=new Date(at).toISOString(),id='hold_'+hash(canonical({contactId,expectedStateHash,at}));
  const entry={...before.entry,resumeAfter,ownerHoldResolutionRef:id};
  const state={...before.state,handoff:null,externalLock:null,consentHold:null,enrollmentHash:hash(canonical(entry))};
  const external=new Map((before.previous?.reviewed_external_messages??[]).map(r=>[r.message_id,r]));for(const r of checked.reviewed)external.set(r.message_id,r);
  const receipt={version:1,resolution:id,location_id:LOCATION,contact_id:contactId,conversation_id:entry.conversationId,operator,reason,note:note.trim(),resolved_at:resumeAfter,resume_after:resumeAfter,before_hash:expectedStateHash,before,snapshot_hash:hash(canonical(checked.snapshot)),released_alert_hash:before.alert?hash(canonical(before.alert)):null,reviewed_external_messages:[...external.values()],previous_resolution_ref:before.entry.ownerHoldResolutionRef??null,no_send:true,no_replay:true};
  // Immutable insert only. Existing alert, claims, receipts and message payloads
  // are left untouched, including all consumed attempts and the 7969 resolution.
  b.db.prepare('INSERT INTO prospect_owner_resolutions(id,payload) VALUES(?,?)').run(id,b.vault.seal(receipt,id));
  b.prospectV1.write('prospect_entries',b.prospectV1.key(contactId),entry);b.prospectV1.write('sales_contacts',b.prospectV1.key(contactId),state);
  b.audit('owner_hold_resolved',id);
  return {resolved:true,resolution:id,contact_id:contactId,resume_after:resumeAfter,historical_rows_changed:0,no_send:true,no_replay:true};
 });
}

