// Review-only deterministic safety ledger. No GHL/OpenAI credentials or network I/O.
// Raw records are evidence supplied by the authenticated cloud agent, not signed GHL proofs.
import {hash,canonical} from './security.mjs';
import {LOCATION,PROVIDER,CALENDAR,BOOKING_LINK,validId,blocked,utc,inHours,explicitOptOut,consentEvidence,inspectDraft,priceAnswer,validatePriceDraft,NEEDS_COLE,HANDED_OFF} from './sales-policy.mjs';
import {transactionPaymentEvidence} from './owner-ai-evidence.mjs';
const object=x=>x&&typeof x==='object'&&!Array.isArray(x);
const clone=x=>JSON.parse(JSON.stringify(x));
const CONSUMED=['attempt_unknown','queued','sent','delivered','failed'];
// GHL unfiltered histories also contain CRM activities and calls. Those are not
// human messaging replies. Unknown provenance of actual communication stays held.
const TAKEOVER_MESSAGE_TYPES=new Set(['TYPE_SMS','TYPE_SMS_REVIEW_REQUEST','TYPE_SMS_NO_SHOW_REQUEST','TYPE_CAMPAIGN_SMS','TYPE_CAMPAIGN_MANUAL_SMS','TYPE_EMAIL','TYPE_CUSTOM_SMS','TYPE_CUSTOM_EMAIL','TYPE_CUSTOM_PROVIDER_SMS','TYPE_CUSTOM_PROVIDER_EMAIL','TYPE_WHATSAPP','TYPE_FACEBOOK','TYPE_INSTAGRAM','TYPE_GMB','TYPE_WEBCHAT','TYPE_LIVE_CHAT']);
const RESULT_STATUSES={queued:['queued','pending'],sent:['sent'],delivered:['delivered','read'],failed:['failed']};
export class SalesReplyLedger {
  constructor(bridge,{enabled=false,enrollments=[],manualIdentity=null,handoffRoute=null}={}) {
    this.b=bridge; this.enabled=enabled; this.enrollments=clone(enrollments);this.manualIdentity=manualIdentity;this.handoffRoute=handoffRoute;
    if(!Array.isArray(enrollments)) blocked('invalid_enrollments');
    const ids=new Set();
    for(const e of enrollments) {
      if(!validId(e.contactId)||!validId(e.conversationId)||ids.has(e.contactId)||!validId(e.baseline?.id)||!validId(e.approvalRef)||!['allowlist','tag'].includes(e.mode)||e.mode==='tag'&&typeof e.tag!=='string') blocked('invalid_enrollment');
      ids.add(e.contactId);utc(e.notBefore);if(utc(e.baseline.dateAdded)>utc(e.notBefore)) blocked('baseline_after_cutoff');
      if(!e.timezone||!['owner_verified','contact_confirmed'].includes(e.timezone.source)||!validId(e.timezone.evidenceRef)) blocked('reliable_timezone_required');
      inHours(this.b.now(),e.timezone.value);
    }
    bridge.db.exec(`CREATE TABLE IF NOT EXISTS sales_contacts(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS sales_messages(id TEXT PRIMARY KEY,payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS sales_receipts(id TEXT PRIMARY KEY,payload TEXT NOT NULL);`);
  }
  status(){return {enabled:this.enabled,enrollment_count:this.enrollments.length,booking_writes_enabled:false,followup_timers_enabled:false,internal_handoff_configured:!!this.handoffRoute};}
  key(contact,id=''){return hash(canonical({location:LOCATION,contact,id}));}
  read(table,key){const r=this.b.db.prepare(`SELECT payload FROM ${table} WHERE id=?`).get(key);return r?this.b.vault.open(r.payload,key):null;}
  write(table,key,x){this.b.db.prepare(`INSERT INTO ${table}(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`).run(key,this.b.vault.seal(x,key));}
  tx(fn){this.b.db.exec('BEGIN IMMEDIATE');try{const r=fn();this.b.db.exec('COMMIT');return r;}catch(e){this.b.db.exec('ROLLBACK');throw e;}}
  rows(contact){return this.b.db.prepare('SELECT id,payload FROM sales_messages').all().map(r=>this.b.vault.open(r.payload,r.id)).filter(r=>r.contactId===contact);}
  gate(owner,eventId,open=true){
    this.b.assertClaim?.(owner,eventId,{open,allowExpired:!open});
    if(!this.enabled||!this.b.enabled) blocked('sales_replies_disabled');
    const event=this.b.eventForOwner(owner,eventId);
    if(event.data.location_id!==LOCATION||event.data.source!=='loop_workflow') blocked('sales_event_scope');
    const entry=this.enrollments.find(e=>e.contactId===event.data.contact_id);
    if(!entry) blocked('contact_not_enrolled');
    if(utc(event.timestamp)<=utc(entry.notBefore)) blocked('event_before_enrollment');
    const claim=this.b.db.prepare('SELECT * FROM processing WHERE event_id=? AND owner=?').get(eventId,owner);
    if(!claim||(open&&claim.completed!==null)) blocked('open_event_claim_required');
    return {event,entry};
  }
  contactState(entry){const key=this.key(entry.contactId);const prior=this.read('sales_contacts',key);
    if(prior&&prior.enrollmentHash!==hash(canonical(entry))) blocked('enrollment_changed_requires_review');
    return prior??{contactId:entry.contactId,enrollmentHash:hash(canonical(entry)),manualUntil:0,suppressed:false,handoff:null,externalLock:null};
  }
  snapshot(event,entry,s,minRead=0){
    if(!object(s)||!object(s.contact)||!Array.isArray(s.pages)||s.pages.length<1||s.pages.length>this.maxHistoryPages()) blocked('snapshot_required');
    const start=utc(s.read_started_at),end=utc(s.read_completed_at),now=this.b.now();
    if(start<Math.max(minRead,utc(event.timestamp))||end<start||end>now||now-start>30000) blocked('fresh_snapshot_required');
    if(s.contact.id!==entry.contactId||s.contact.locationId!==LOCATION) blocked('contact_scope');
    const ids=new Set(),messages=[],checkpoint=this.historyCheckpoint(entry);let boundary=false;
    for(let i=0;i<s.pages.length;i++){
      const p=s.pages[i];
      if(!object(p)||!Array.isArray(p.messages)||p.messages.length>50||typeof p.nextPage!=='boolean') blocked('invalid_page');
      if((p.nextPage||p.lastMessageId!=null)&&!validId(p.lastMessageId)) blocked('invalid_cursor');
      if(i===0&&p.request_last_message_id!==null||i>0&&(!s.pages[i-1].nextPage||!validId(p.request_last_message_id)||p.request_last_message_id!==s.pages[i-1].lastMessageId)) blocked('pagination_chain');
      for(const m of p.messages){
        if(!object(m)||!validId(m.id)||ids.has(m.id)) blocked('invalid_message_id');ids.add(m.id);
        if(m.locationId!==LOCATION||m.contactId!==entry.contactId||m.conversationId!==entry.conversationId) blocked('message_scope');
        const at=utc(m.dateAdded);if(at>end) blocked('future_message');
        if(m.id===entry.baseline.id){if(m.dateAdded!==entry.baseline.dateAdded)blocked('baseline_mismatch');boundary=true;}
        if(checkpoint&&m.id===checkpoint.messageId){if(this.messageFingerprint(m)!==checkpoint.fingerprint)blocked('history_checkpoint_mismatch');boundary=true;}
        messages.push({...m,at});
      }
    }
    if(!boundary&&s.pages.at(-1).nextPage)blocked('coverage_gap');
    return {messages,start,end};
  }
  incomingBoundary(entry){return utc(entry.notBefore);}
  maxHistoryPages(){return 2;}
  historyCheckpoint(entry){return null;}
  activeInboundReplies(){return false;}
  batchPending(){return false;}
  flexiblePricing(){return false;}
  messageFingerprint(m){return hash(canonical({id:m.id,date:m.dateAdded,body:m.body??null,attachments:m.attachments??null,provider:m.conversationProviderId??null,type:m.messageType,direction:m.direction}));}
  timezoneMatches(entry,s){return s.contact.timezone===undefined||s.contact.timezone===entry.timezone.value;}
  observe(owner,eventId,s,minRead=0){
    const {event,entry}=this.gate(owner,eventId),{messages}=this.snapshot(event,entry,s,minRead);
    // Always persist safety signals before evaluating positive permission gates.
    const state=this.contactState(entry),saved=this.rows(entry.contactId),knownOut=this.knownOutboundIds(entry,saved);
    const consent=consentEvidence(s.contact);
    if(consent.status==='blocked'){state.suppressed=true;state.suppressionEvidence??={reason:consent.reason,eventId,at:this.b.now()};}
    // Unknown reads are a temporary permission hold, never proof of an opt-out.
    // Preserve older durable suppression: its original evidence may be unavailable.
    state.consentHold=consent.status==='unknown'?{reason:consent.reason,eventId,at:this.b.now()}:null;
    // A resume boundary limits replies, never safety: delayed opt-outs or unsafe
    // content from the original eligible history must still persist a hold.
    const safetyHistory=messages.filter(m=>m.at>utc(entry.notBefore));
    const candidateBoundary=this.incomingBoundary(entry),newer=messages.filter(m=>m.at>candidateBoundary);
    for(const m of messages.filter(m=>m.direction==='outbound'&&TAKEOVER_MESSAGE_TYPES.has(m.messageType)&&m.at>=Math.min(utc(entry.notBefore),this.b.now()-86400000))){
      if(knownOut.has(m.id))continue;
      state.manualUntil=Math.max(state.manualUntil,m.at+86400000);
      if(!this.isVerifiedManual(m)) state.externalLock={messageId:m.id,reason:'unverified_external_sender'};
    }
    if(!Array.isArray(s.assessments))blocked('message_assessments_required');
    if(new Set(s.assessments.map(a=>a.messageId)).size!==s.assessments.length)blocked('duplicate_assessment');
    for(const m of safetyHistory){
      if(!['inbound','outbound'].includes(m.direction))blocked('unknown_direction');
      if(m.direction!=='inbound')continue;
      const a=s.assessments.find(a=>a.messageId===m.id);
      if(!a||!['normal','opt_out','system','handoff','unsafe'].includes(a.kind))blocked('inbound_assessment_required');
      const key=this.key(entry.contactId,m.id),prior=this.read('sales_messages',key),digest=hash(canonical(m));
      // Delivery status may change; freeze only identity, source time, text and attachments.
      const fingerprint=this.messageFingerprint(m);
      if(prior&&prior.fingerprint!==fingerprint)blocked('message_evidence_changed');
      const optOut=explicitOptOut(m.body)||a.kind==='opt_out';
      if(optOut){state.suppressed=true;state.suppressionEvidence??={reason:a.kind==='opt_out'?'assessed_opt_out':'explicit_opt_out',eventId,messageId:m.id,at:this.b.now()};}
      if(['handoff','unsafe'].includes(a.kind)&&!state.handoff)state.handoff={eventId,messageId:m.id,status:'unavailable',at:this.b.now()};
      // Do not create, rephase or append receipts to historical reply rows. Their
      // safety evidence is covered by this observation receipt and contact state.
      if(m.at<=candidateBoundary)continue;
      const row=prior??{contactId:entry.contactId,messageId:m.id,dateAdded:m.dateAdded,fingerprint,phase:'pending'};
      if(optOut)row.phase=CONSUMED.includes(row.phase)?row.phase:'suppressed';
      if(a.kind==='system'&&!CONSUMED.includes(row.phase))row.phase='ignored_system';
      row.receiptIds=[...new Set([...(row.receiptIds??[]),eventId])];row.evidenceHash=digest;
      this.write('sales_messages',key,row);
    }
    const receipt={eventId,source:event.data.source,source_message_id:event.data.source_message_id,received_at:event.timestamp,contactId:entry.contactId,snapshotHash:hash(canonical(s)),observedAt:this.b.now()};
    const receiptKey=this.key(entry.contactId,eventId+':'+receipt.snapshotHash);this.write('sales_receipts',receiptKey,receipt);
    if(this.batchPending()&&messages.length){const newest=[...messages].sort((a,b)=>b.at-a.at||b.id.localeCompare(a.id))[0];state.historyCheckpoint={messageId:newest.id,dateAdded:newest.dateAdded,fingerprint:this.messageFingerprint(newest),observedAt:this.b.now()};}
    this.write('sales_contacts',this.key(entry.contactId),state);
    return {entry,event,state,messages:newer,snapshot:s};
  }
  permission(x){
    const {entry,state,snapshot:s,messages}=x,now=this.b.now();
    if(state.suppressed)return 'opt_out_or_dnd';
    if(state.handoff)return 'unresolved_handoff';
    if(state.externalLock)return 'unverified_external_sender';
    if(state.manualUntil>now)return 'manual_takeover_24h';
    if(state.consentHold)return state.consentHold.reason;
    if(entry.mode==='tag'&&(!Array.isArray(s.contact.tags)||!s.contact.tags.includes(entry.tag)))return 'enrollment_tag_missing';
    if(!Array.isArray(s.contact.tags)||s.contact.tags.some(t=>typeof t!=='string'))return 'tags_unknown';
    if(!validId(entry.takeoverClearanceRef))return 'initial_takeover_clearance_required';
    if(s.contact.tags.some(t=>['current customer','wavv-not-interested','wavv-bad-number','wavv-call-blocked'].includes(t.toLowerCase())))return 'client_or_suppression_tag';
    // These are explicit adapter evidence, not facts inferred from an Open opportunity.
    const c=s.crm;
    if(!object(c)||c.contactId!==entry.contactId||c.locationId!==LOCATION||c.complete!==true||!validId(c.evidenceRef))return 'crm_evidence_unknown';
    if(c.client!==false||c.paid===true||!this.activeInboundReplies()&&c.paid!==false||c.sold!==false||c.conflict!==false)return 'client_paid_sold_or_conflict';
    if(!Array.isArray(c.opportunities)||!Array.isArray(c.transactions)||!Array.isArray(c.appointments))return 'crm_records_required';
    if(s.contact.type==='customer')return 'client_paid_sold_or_conflict';
    if(c.opportunities.some(o=>o.pipelineStageId==='9aef9c63-f86a-41c1-b241-ff013cf4a1ff'))return 'client_paid_sold_or_conflict';
    if(c.opportunities.some(o=>o.contactId!==entry.contactId||o.locationId!==LOCATION||!validId(o.id)||!['open','lost','abandoned'].includes(o.status)))return 'opportunity_conflict';
    if(this.activeInboundReplies()){
      // Recheck the actual records, not only a supplied summary boolean. A real
      // payment stays excluded even if another record is failed or incomplete.
      const payment=transactionPaymentEvidence(c.transactions,{locationId:LOCATION,contactId:entry.contactId});
      if(payment.paid===true)return 'client_paid_sold_or_conflict';
      if(c.paid!==false||payment.paid!==false)return 'payment_record_requires_review';
    }else if(c.transactions.length)return 'payment_record_requires_review';
    if(c.appointments.some(a=>!object(a)||a.contactId!==entry.contactId||a.locationId!==LOCATION||!validId(a.calendarId)||!this.activeInboundReplies()&&a.calendarId!==CALENDAR||!validId(a.id)||!['new','confirmed','active','cancelled','showed','noshow','invalid','completed'].includes(a.appointmentStatus)))return 'appointment_conflict';
    if(!this.activeInboundReplies()&&c.appointments.filter(a=>['new','confirmed','active'].includes(a.appointmentStatus)).length>1)return 'multiple_bookings';
    // Active answers and scheduled outbound have different permission gates.
    // Booking timezone confirmation remains a separate scheduling guard.
    if(!this.activeInboundReplies()){
      if(!this.timezoneMatches(entry,s))return 'timezone_conflict';
      if(!inHours(now,entry.timezone.value)||!inHours(now+15000,entry.timezone.value))return 'outside_local_hours';
    }
    const w=s.workflowSuppression;
    if(!object(w)||w.contactId!==entry.contactId||w.locationId!==LOCATION||w.verified!==true||!validId(w.evidenceRef)||w.profileRevision!==entry.workflowProfileRevision||!validId(entry.workflowProfileRevision))return 'workflow_suppression_unverified';
    if(s.channelPermission?.providerId!==PROVIDER||s.channelPermission?.allowed!==true||!validId(s.channelPermission?.evidenceRef))return 'channel_permission_unknown';
    // Content/provider support applies to unanswered candidates, not handled
    // history. observe() has already rechecked every identity and safety signal;
    // unresolved attempts still block separately in prepare(), never reset here.
    const handled=this.activeInboundReplies()?new Set(this.rows(entry.contactId).filter(r=>[...CONSUMED,'ignored_system','suppressed'].includes(r.phase)).map(r=>r.messageId)):new Set();
    for(const m of messages.filter(m=>m.direction==='inbound'&&!handled.has(m.id)&&s.assessments.find(a=>a.messageId===m.id)?.kind!=='system')){
      if(m.conversationProviderId!==PROVIDER||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS')return 'provider_mismatch';
      if(typeof m.body!=='string'||m.attachments!==undefined&&(!Array.isArray(m.attachments)||m.attachments.length))return 'unsupported_message_content';
    }
    return null;
  }
  persistHold(x,reason){
    if(['client_or_suppression_tag','client_paid_sold_or_conflict','opportunity_conflict','payment_record_requires_review','appointment_conflict','multiple_bookings'].includes(reason)){
      x.state.handoff??={eventId:x.event.eventId??null,status:'unavailable',reason,at:this.b.now()};
      this.write('sales_contacts',this.key(x.entry.contactId),x.state);
    }
  }
  appointmentRestrictions(x){
    if(!this.activeInboundReplies())return [];
    const active=x.snapshot.crm.appointments.filter(a=>['new','confirmed','active'].includes(a.appointmentStatus));
    return [...(active.some(a=>a.calendarId!==CALENDAR)?['active_other_calendar']:[]),...(active.length>1?['multiple_active_bookings']:[])];
  }
  unresolvedPhases(){return ['attempt_unknown','queued','sent','failed'];}
  isVerifiedManual(m){return !!(this.manualIdentity&&m.source===this.manualIdentity.source&&m.userId===this.manualIdentity.userId);}
  knownOutboundIds(entry,saved){return new Set(saved.map(r=>r.outboundId).filter(Boolean));}
  reserveCampaign(entry,row,attemptId,at){}
  recordCampaign(row,outcome,outboundId){}
  prepare(owner,eventId,evidence){return this.tx(()=>{
    this.b.assertClaim?.(owner,eventId,{open:true});
    const x=this.observe(owner,eventId,evidence),reason=this.permission(x);
    if(reason){this.persistHold(x,reason);return {prepared:false,reason};}
    const rows=this.rows(x.entry.contactId);
    if(!this.batchPending()&&rows.some(r=>r.eventId===eventId&&CONSUMED.includes(r.phase)))return {prepared:false,reason:'event_attempt_consumed'};
    if(rows.some(r=>this.unresolvedPhases().includes(r.phase)))return {prepared:false,reason:'prior_send_unresolved_or_failed'};
    const pending=rows.filter(r=>['pending','prepared'].includes(r.phase)&&utc(r.dateAdded)>this.incomingBoundary(x.entry)).sort((a,b)=>utc(a.dateAdded)-utc(b.dateAdded)||a.messageId.localeCompare(b.messageId));
    if(!pending.length)return {prepared:false,reason:'no_pending_inbound'};
    if(!this.batchPending()&&pending.length>1&&pending[0].dateAdded===pending[1].dateAdded)return {prepared:false,reason:'ambiguous_chronology'};
    const batch=this.batchPending()?pending:[pending[0]],row=batch.at(-1);
    if(batch.some(r=>!x.messages.some(m=>m.id===r.messageId)))return {prepared:false,reason:'older_pending_not_visible',pending_message_ids:batch.map(r=>r.messageId)};
    for(const candidate of batch)if(candidate.phase==='prepared'&&candidate.eventId!==eventId){
      // BEGIN IMMEDIATE serializes the transfer with begin_attempt. Only a closed,
      // same-owner claim can relinquish a preparation that never consumed a permit.
      const priorClaim=this.b.db.prepare('SELECT completed FROM processing WHERE event_id=? AND owner=?').get(candidate.eventId,owner);
      if(!priorClaim||priorClaim.completed===null)return {prepared:false,reason:'another_event_owns_candidate'};
      if(candidate.attemptId!==undefined||candidate.attemptAt!==undefined)return {prepared:false,reason:'candidate_attempt_consumed'};
    }
    const ids=batch.map(r=>r.messageId),threadHash=hash(canonical(x.messages)),at=this.b.now();
    for(const candidate of batch)this.write('sales_messages',this.key(x.entry.contactId,candidate.messageId),{...candidate,phase:'prepared',eventId,preparedAt:at,threadHash,batchMessageIds:ids,anchorMessageId:row.messageId});
    return {prepared:true,message_id:row.messageId,message_ids:ids,thread_version:threadHash,requires_fresh_preflight:true,context_message_ids:x.messages.map(m=>m.id),delivery_kind:this.activeInboundReplies()?'active_inbound':'configured_outbound',timezone_evidence:x.snapshot.timezoneEvidence??x.entry.timezone,booking_restrictions:this.appointmentRestrictions(x)};
  });}
  begin(owner,eventId,{snapshot,draft,message_id,thread_version}={}){return this.tx(()=>{
    this.b.assertClaim?.(owner,eventId,{open:true});
    const {entry}=this.gate(owner,eventId);if(!validId(message_id))blocked('message_id_required');
    const key=this.key(entry.contactId,message_id),row=this.read('sales_messages',key);
    if(!row||row.phase!=='prepared'||row.eventId!==eventId||utc(row.dateAdded)<=this.incomingBoundary(entry))return {permitted:false,reason:'candidate_not_prepared_or_attempt_consumed'};
    const x=this.observe(owner,eventId,snapshot,row.preparedAt),reason=this.permission(x);
    if(reason){this.persistHold(x,reason);return {permitted:false,reason};}
    const ids=row.batchMessageIds??[message_id],batch=ids.map(id=>this.read('sales_messages',this.key(entry.contactId,id)));
    if(ids.some(id=>snapshot.assessments.find(a=>a.messageId===id)?.kind!=='normal'))return {permitted:false,reason:'candidate_classification_changed',retryable:true};
    if(row.anchorMessageId&&row.anchorMessageId!==message_id||batch.some(r=>!r||r.phase!=='prepared'||r.eventId!==eventId||r.threadHash!==row.threadHash))return {permitted:false,reason:'candidate_not_prepared_or_attempt_consumed'};
    if(thread_version!==row.threadHash||hash(canonical(x.messages))!==row.threadHash)return {permitted:false,reason:'thread_changed',retryable:true};
    inspectDraft(draft?.text);
    // Semantic checks are model review, not a software proof of policy compliance.
    if(draft.policyReviewed!==true||draft.untrustedContentIgnored!==true||draft.answersMessageId!==message_id||!['answer','pricing','booking_link','checkout_link','existing_meeting','acknowledge'].includes(draft.intent))blocked('draft_review_required');
    if(ids.length>1&&canonical(draft.answersMessageIds)!==canonical(ids)||draft.answersMessageIds!==undefined&&canonical(draft.answersMessageIds)!==canonical(ids))blocked('draft_batch_review_required');
    if(draft.text.includes(HANDED_OFF))blocked('handoff_ack_requires_verified_notification');
    const restrictions=this.appointmentRestrictions(x),appointmentAction=['booking_link','existing_meeting'].includes(draft.intent)||draft.text.includes(BOOKING_LINK);
    if(restrictions.length&&appointmentAction)return {permitted:false,reason:'booking_action_requires_review',retryable:true,booking_restrictions:restrictions};
    const booked=snapshot.crm.appointments.some(a=>['new','confirmed','active'].includes(a.appointmentStatus));
    if(booked&&(draft.intent==='booking_link'||draft.text.includes(BOOKING_LINK)))blocked('already_booked');
    if(draft.intent==='pricing'){if(this.flexiblePricing())validatePriceDraft(draft);else if(draft.text!==priceAnswer(draft.priceKey))blocked('approved_price_text_required');}
    if(draft.intent!=='pricing'&&/\$|\bUSD\b/i.test(draft.text))blocked('pricing_intent_required');
    const attemptId='attn_sales_'+hash(key),at=this.b.now();
    this.b.reserveProcessingSend?.(owner,eventId);
    this.reserveCampaign(entry,row,attemptId,at);
    for(const candidate of batch)this.write('sales_messages',this.key(entry.contactId,candidate.messageId),{...candidate,phase:'attempt_unknown',attemptId,attemptAt:at,bodyHash:hash(draft.text)});
    this.b.audit('sales_attempt_reserved',message_id);
    return {permitted:true,attempt_id:attemptId,message_id,message_ids:ids,expires_at:new Date(at+15000).toISOString(),operationId:'send-a-new-message',locationId:LOCATION,idempotencyKey:attemptId,params:{body:{type:'Custom',contactId:entry.contactId,conversationProviderId:PROVIDER,message:draft.text,replyMessageId:message_id}}};
  });}
  record(owner,eventId,r){return this.tx(()=>{
    const {entry}=this.gate(owner,eventId,false);const row=this.rows(entry.contactId).find(x=>x.eventId===eventId&&x.attemptId===r?.attempt_id);
    if(!row)blocked('attempt_mismatch');
    const canAdvance=row.phase==='attempt_unknown'||row.phase==='queued'&&['sent','delivered','failed'].includes(r.outcome)||row.phase==='sent'&&['delivered','failed'].includes(r.outcome);
    if(!canAdvance)return {recorded:true,already_recorded:true,phase:row.phase,retry_allowed:false};
    const batch=this.rows(entry.contactId).filter(x=>x.eventId===eventId&&x.attemptId===row.attemptId);
    if(batch.some(x=>x.phase!==row.phase||x.bodyHash!==row.bodyHash||x.outboundId!==row.outboundId||x.sendResponseRecorded!==row.sendResponseRecorded||x.sendResponseConversationId!==row.sendResponseConversationId))blocked('batch_result_state_mismatch');
    if(row.sendResponseRecorded===true&&(!validId(row.outboundId)||row.sendResponseConversationId!==entry.conversationId))blocked('send_response_or_outbound_mismatch');
    if(r.outcome==='unknown'){
      if(r.send_response===undefined)return {recorded:true,phase:row.phase,retry_allowed:false};
      const response=r.send_response;
      if(!object(response)||!validId(response.messageId)||response.conversationId!==entry.conversationId||batch.some(x=>x.messageId===response.messageId)||row.outboundId&&row.outboundId!==response.messageId||row.sendResponseConversationId&&row.sendResponseConversationId!==response.conversationId)blocked('send_response_or_outbound_mismatch');
      // A successful POST response establishes only an exact provider ID. It is
      // not delivery evidence and cannot release or retry the consumed attempt.
      // Save before the separate GET so a transient visibility delay can later
      // be reconciled by this ID, with the original body/scope/attempt checks.
      if(row.sendResponseRecorded!==true){
        for(const candidate of batch)this.write('sales_messages',this.key(entry.contactId,candidate.messageId),{...candidate,outboundId:response.messageId,sendResponseRecorded:true,sendResponseConversationId:entry.conversationId});
        this.b.audit('sales_send_response_recorded',response.messageId);
      }
      return {recorded:true,phase:row.phase,send_response_recorded:true,message_ids:batch.map(x=>x.messageId),retry_allowed:false};
    }
    const m=r.outbound,states=RESULT_STATUSES[r.outcome];
    if(!states||!object(m)||!validId(m.id)||m.id===row.messageId||m.locationId!==LOCATION||m.contactId!==entry.contactId||m.conversationId!==entry.conversationId||m.conversationProviderId!==PROVIDER||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS'||m.direction!=='outbound'||hash(m.body??'')!==row.bodyHash||utc(m.dateAdded)<row.attemptAt||utc(m.dateAdded)>this.b.now()||!states.includes(m.status))blocked('outbound_evidence_required');
    if(!validId(r.send_response?.messageId)||r.send_response.messageId!==m.id||r.send_response.conversationId!==entry.conversationId||row.outboundId&&row.outboundId!==m.id)blocked('send_response_or_outbound_mismatch');
    if(row.sendResponseRecorded===true&&row.sendResponseConversationId!==entry.conversationId)blocked('send_response_or_outbound_mismatch');
    for(const candidate of batch)this.write('sales_messages',this.key(entry.contactId,candidate.messageId),{...candidate,phase:r.outcome,outboundId:m.id});
    this.recordCampaign(row,r.outcome,m.id);
    this.b.audit('sales_result_recorded',m.id);return {recorded:true,phase:r.outcome,message_id:row.anchorMessageId??row.messageId,message_ids:batch.map(x=>x.messageId),retry_allowed:false};
  });}
  handoff(owner,eventId,evidence={}){return this.tx(()=>{
    const {entry}=this.gate(owner,eventId,false),state=this.contactState(entry);
    // Route must be pinned by owner-reviewed code/config; a note/task/assignment cannot count.
    const good=this.handoffRoute&&evidence.route===this.handoffRoute.id&&evidence.recipientId===this.handoffRoute.recipientId&&evidence.operationId===this.handoffRoute.operationId&&evidence.success===true&&validId(evidence.notificationId);
    state.handoff={eventId,status:good?'notified':evidence.success===false?'failed':'unavailable',at:this.b.now(),notificationId:good?evidence.notificationId:null};
    this.write('sales_contacts',this.key(entry.contactId),state);
    return {locked:true,status:state.handoff.status,suggested_text:good?HANDED_OFF:NEEDS_COLE,send_authorized:false};
  });}
}


