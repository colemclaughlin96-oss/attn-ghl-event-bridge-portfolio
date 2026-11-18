// Incoming-only ATTN prospect runtime. No backend GHL credentials or network writes.
import {SalesReplyLedger} from './sales-reply.mjs';
import {LOCATION,PROVIDER,BOOKING_LINK,CHECKOUT_LINK,validId,utc,blocked,inHours,NEEDS_COLE} from './sales-policy.mjs';
import {scopedEvidence,exactPriorAutomation,exactConfirmedManual,CONFIRMED_MANUAL,MANUAL_RELEASE} from './owner-ai-evidence.mjs';
import {OWNER_SCOPE} from './owner-reply.mjs';
import {canonical,hash} from './security.mjs';
import {sendingTimezone,sameTimezone} from './prospect-timezone.mjs';
import {reviewedExternalIds,releasedAlertHash} from './owner-hold-resolution.mjs';
const TABLES={sales_contacts:'prospect_contacts',sales_messages:'prospect_messages',sales_receipts:'prospect_receipts'};
export const RELEASE='prospect-v1';
export const EXCLUSIONS=Object.freeze([
 {contactId:'portfolio_fixture_04',reason:'sold_owner_confirmed',evidenceRef:'cole_example_client_a_sold_20261012_confirmed_20261020'},
 {contactId:'portfolio_fixture_05',reason:'sales_status_conflict',evidenceRef:'cole_example_client_b_unknown_outcome_20261012_confirmed_20261020'}
]);
const obj=x=>x&&typeof x==='object'&&!Array.isArray(x);
export function prospectOptions(env){
 const ingress=env.PROSPECT_V1_INGRESS_ENABLED==='true',enabled=env.PROSPECT_V1_REPLIES_ENABLED==='true';
 if(!ingress&&!enabled)return {ingress:false,enabled:false};
 if(!ingress||env.LISTENER_ENABLED!=='true'||env.OWNER_REPLY_TEST_ENABLED==='true'||env.OWNER_AI_PILOT_ENABLED==='true')throw new Error('prospect_requires_listener_and_closed_owner_experiments');
 utc(env.PROSPECT_V1_NOT_BEFORE);
 return {ingress,enabled,notBefore:env.PROSPECT_V1_NOT_BEFORE};
}
export class ProspectV1 extends SalesReplyLedger{
 constructor(b,{enabled=false,ingress=false,notBefore=null,exclusions=EXCLUSIONS}={}){
  super(b,{enabled:false});this.enabled=enabled;this.ingress=ingress;
  for(const t of [...Object.values(TABLES),'prospect_entries','prospect_release','prospect_alerts','prospect_owner_resolutions'])b.db.exec(`CREATE TABLE IF NOT EXISTS ${t}(id TEXT PRIMARY KEY,payload TEXT NOT NULL);`);
  // Index only opaque hashes. Existing encrypted evidence stays byte-for-byte intact;
  // a one-time migration avoids decrypting every other contact on each reply.
  b.db.exec('CREATE TABLE IF NOT EXISTS prospect_message_index(message_key TEXT PRIMARY KEY,contact_key TEXT NOT NULL); CREATE INDEX IF NOT EXISTS prospect_messages_by_contact ON prospect_message_index(contact_key);');
  const missing=b.db.prepare('SELECT m.id,m.payload FROM prospect_messages m LEFT JOIN prospect_message_index i ON i.message_key=m.id WHERE i.message_key IS NULL').all();
  for(const r of missing){const message=b.vault.open(r.payload,r.id);if(!validId(message.contactId))blocked('invalid_stored_contact');b.db.prepare('INSERT OR IGNORE INTO prospect_message_index(message_key,contact_key) VALUES(?,?)').run(r.id,this.key(message.contactId));}
  const old=this.read('prospect_release',RELEASE);
  if(ingress||enabled){utc(notBefore);if(!Array.isArray(exclusions)||exclusions.length<2||new Set(exclusions.map(e=>e.contactId)).size!==exclusions.length||exclusions.some(e=>!validId(e.contactId)||!['sold_owner_confirmed','sales_status_conflict'].includes(e.reason)||!validId(e.evidenceRef)))blocked('verified_individual_exclusions_required');if(!b.enabled||enabled&&!ingress||b.ownerReply.enabled||b.ownerAiPilot.enabled)blocked('prospect_runtime_scope');if(old&&old.notBefore!==notBefore)blocked('prospect_cutoff_immutable');if(old&&canonical(old.exclusions)!==canonical(exclusions))blocked('prospect_exclusions_changed_requires_review');if(!old)this.write('prospect_release',RELEASE,{notBefore,exclusions});}
  this.notBefore=old?.notBefore??notBefore;this.exclusions=old?.exclusions??exclusions;
 }
 read(t,k){return super.read(TABLES[t]??t,k);} write(t,k,x){const table=TABLES[t]??t,result=super.write(table,k,x);if(table==='prospect_messages')this.b.db.prepare('INSERT INTO prospect_message_index(message_key,contact_key) VALUES(?,?) ON CONFLICT(message_key) DO UPDATE SET contact_key=excluded.contact_key').run(k,this.key(x.contactId));return result;}
 rows(c){return this.b.db.prepare('SELECT m.id,m.payload FROM prospect_messages m JOIN prospect_message_index i ON i.message_key=m.id WHERE i.contact_key=?').all(this.key(c)).map(r=>this.b.vault.open(r.payload,r.id));}
 status(){return {release:RELEASE,enabled:this.enabled,ingress_enabled:this.ingress,not_before:this.notBefore,scope:'genuine_incoming_attn_prospects',booking_writes_enabled:false,checkout_links_enabled:true,excluded_contacts:this.exclusions,new_followups_enabled:false,customer_replies_enabled:false};}
 eventGate(owner,eventId,open=true){
  if(!this.ingress||!this.b.enabled||!this.notBefore)blocked('prospect_v1_disabled');
  this.b.assertClaim?.(owner,eventId,{open,allowExpired:!open});
  const event=this.b.eventForOwner(owner,eventId),claim=this.b.db.prepare('SELECT * FROM processing WHERE event_id=? AND owner=?').get(eventId,owner);
  if(!claim||(open&&claim.completed!==null))blocked('open_event_claim_required');
  const entry=this.read('prospect_entries',this.key(event.data.contact_id));if(entry?.resumeAfter&&utc(event.timestamp)<=utc(entry.resumeAfter))blocked('event_before_owner_resume');
  if(event.data.location_id!==LOCATION||!validId(event.data.contact_id)||event.data.source!=='loop_workflow'||utc(event.timestamp)<=utc(this.notBefore)||utc(event.timestamp)>this.b.now())blocked('prospect_event_scope');
  return event;
 }
 gate(owner,eventId,open=true){const event=this.eventGate(owner,eventId,open),entry=this.read('prospect_entries',this.key(event.data.contact_id));if(!entry)blocked('verified_contact_thread_required');return {event,entry};}
 historyRequirements(owner,eventId){
  const event=this.eventGate(owner,eventId),entry=this.read('prospect_entries',this.key(event.data.contact_id));
  const checkpoint=entry?this.historyCheckpoint(entry):null;
  const initial=Math.max(utc(this.notBefore),utc(event.timestamp)-120000,this.serverBoundary());
  return {contact_id:event.data.contact_id,conversation_id:entry?.conversationId??null,baseline_id:entry?.baseline.id??null,checkpoint_id:checkpoint?.messageId??null,initial_cutoff:entry?null:new Date(initial).toISOString(),incoming_not_before:new Date(entry?this.incomingBoundary(entry):initial).toISOString(),handled_message_ids:entry?this.rows(entry.contactId).filter(r=>['attempt_unknown','queued','sent','delivered','failed','ignored_system','suppressed'].includes(r.phase)).map(r=>r.messageId):[],takeover_before:new Date(this.b.now()-86400000).toISOString(),max_pages:this.maxHistoryPages()};
 }
 adapt(event,raw,{full=true,candidateId=null}={}){
  if(!obj(raw)||!Array.isArray(raw.reads))blocked('official_reads_required');
  const matches=raw.reads.filter(r=>r.operationId==='search-conversation'),r=matches[0],d=r?.result?.data,c=event.data.contact_id;
  if(matches.length!==1||r.locationId!==LOCATION||canonical(r.params)!==canonical({query:{contactId:c,limit:2}})||r.result.success!==true||r.result.status!==200||r.result.operationId!=='search-conversation'||d?.total!==1||!Array.isArray(d.conversations)||d.conversations.length!==1)blocked('unique_contact_conversation_required');
  const v=d.conversations[0];if(v.locationId!==LOCATION||v.contactId!==c||!validId(v.id))blocked('conversation_scope');
  const scope={locationId:LOCATION,contactId:c,conversationId:v.id,conversationProviderId:PROVIDER};
  const s=scopedEvidence({...raw,reads:raw.reads.filter(x=>x.operationId!=='search-conversation')},{scope,full,candidateId,maxPages:this.maxHistoryPages(),workflowRef:{id:'cole_existing_workflows_preserved_20261020'}});
  const ms=s.pages.flatMap(p=>p.messages),tz=sendingTimezone(s.contact,ms,raw.reads.find(r=>r.operationId==='get-contact'),raw.reads.filter(r=>r.operationId==='get-messages'),raw.read_completed_at);s.timezoneEvidence=tz;
  // This is permission for reply coexistence, NOT a claim that workflows were stopped.
  s.workflowSuppression={locationId:LOCATION,contactId:c,verified:true,evidenceRef:'cole_existing_workflows_preserved_20261020',profileRevision:'prospect_v1_coexistence'};
  return {scope,s,tz};
 }
 ensureEntry(event,{scope,s,tz}){
  const key=this.key(scope.contactId),old=this.read('prospect_entries',key);
  if(old){
   if(old.conversationId!==scope.conversationId)blocked('pinned_conversation_changed');
   // Validate raw scope, ordering, complete coverage and freshness BEFORE changing evidence.
   this.snapshot(event,old,s);
   if(sameTimezone(old.timezone,tz))return old;
   const state=this.contactState(old),next={...old,timezone:tz,timezoneHistory:[...(old.timezoneHistory??[]),{at:this.b.now(),previous:old.timezone??null,next:tz,eventId:event.eventId}]};
   // Only evidence changes; preserve ALL suppression/manual/external/handoff state.
   state.enrollmentHash=hash(canonical(next));this.write('sales_contacts',key,state);this.write('prospect_entries',key,next);this.b.audit('prospect_timezone_reconciled',event.eventId);return next;
  }
  // A real event can establish state; at most 120 seconds of fresh pre-receipt records,
  // never earlier than the rollout cutoff. No unread-list enrollment.
  const cutoff=Math.max(utc(this.notBefore),utc(event.timestamp)-120000,this.serverBoundary()),messages=s.pages.flatMap(p=>p.messages);
  const baseline=messages.filter(m=>utc(m.dateAdded)<=cutoff).sort((a,b)=>utc(b.dateAdded)-utc(a.dateAdded))[0];
  if(!baseline&&s.pages.at(-1).nextPage)blocked('new_contact_boundary_missing');
  const entry={contactId:scope.contactId,conversationId:scope.conversationId,mode:'allowlist',approvalRef:'cole_incoming_prospect_release_20261020',baseline:baseline?{id:baseline.id,dateAdded:baseline.dateAdded}:{id:'empty_history_boundary',dateAdded:new Date(cutoff).toISOString()},notBefore:new Date(cutoff).toISOString(),timezone:tz,takeoverClearanceRef:'raw_24h_history_required',workflowProfileRevision:'prospect_v1_coexistence'};
  this.snapshot(event,entry,s);this.write('prospect_entries',key,entry);return entry;
 }
 serverBoundary(){return this.b.processingMode==='server'?utc(this.b.serverWorkerNotBefore):0;}
 incomingBoundary(entry){return Math.max(utc(entry.notBefore),entry.resumeAfter?utc(entry.resumeAfter):0,this.serverBoundary());}
 maxHistoryPages(){return 10;}
 historyCheckpoint(entry){return this.contactState(entry).historyCheckpoint??null;}
 activeInboundReplies(){return true;}
 batchPending(){return true;}
 flexiblePricing(){return true;}
 timezoneMatches(entry,s){return sameTimezone(entry.timezone,s.timezoneEvidence);}
 snapshot(event,entry,s,minRead=0){const x=super.snapshot(event,entry,s,minRead);if(x.messages.some((m,i)=>i&&m.at>x.messages[i-1].at))blocked('history_order_required');if(s.pages.at(-1).nextPage&&!x.messages.some(m=>m.at<=x.start-86400000))blocked('takeover_24h_history_incomplete');for(const m of x.messages){if(m.id===CONFIRMED_MANUAL.id&&!exactConfirmedManual(m))blocked('confirmed_manual_evidence_mismatch');}return x;}
 knownOutboundIds(entry,saved){const ids=super.knownOutboundIds(entry,saved);for(const m of this.currentMessages??[])if(exactPriorAutomation(m)||['workflow','campaign'].includes(m.source)&&validId(m.id))ids.add(m.id);for(const id of reviewedExternalIds(this.b,entry,this.currentMessages??[]))ids.add(id);return ids;}
 isVerifiedManual(m){return exactConfirmedManual(m);}
 // 'sent' is a definitive API result, not phone delivery. A later genuine incoming can
 // advance the conversation; queued/unknown/failed remain held, never retried.
 unresolvedPhases(){return ['attempt_unknown','queued','failed'];}
 permission(x){
  if(!this.enabled)return 'prospect_replies_disabled';
  if(this.exclusions.some(e=>e.contactId===x.entry.contactId))return 'owner_excluded_client_or_conflict';
  if(x.entry.contactId===OWNER_SCOPE.contactId&&this.b.now()<utc(MANUAL_RELEASE))return 'manual_takeover_24h';
  return super.permission(x);
 }
 prepare(owner,eventId,raw){
  const event=this.eventGate(owner,eventId),adapted=this.adapt(event,raw);
  this.tx(()=>this.ensureEntry(event,adapted));this.currentMessages=adapted.s.pages.flatMap(p=>p.messages);
  try{return super.prepare(owner,eventId,adapted.s);}finally{this.currentMessages=null;}
 }
 begin(owner,eventId,e){const event=this.eventGate(owner,eventId),{s}=this.adapt(event,e?.snapshot,{candidateId:e?.message_id});
  // Checkout only for explicit relevant buying intent; no fabricated payment/meeting URLs.
  const urls=e?.draft?.text?.match(/https?:\/\/[^\s<>]+/g)??[];
  if(urls.some(u=>![BOOKING_LINK,CHECKOUT_LINK].includes(u.replace(/[.,!?]$/,''))))blocked('unapproved_link');
  // Repeated wording can answer a distinct new question. The base ledger still
  // deduplicates by inbound message and consumed attempt, not by reply text.
  if(urls.some(u=>u.replace(/[.,!?]$/,'')===CHECKOUT_LINK)){
   // Use only this event's durable, unresolved preparation. Earlier answered
   // buying intent and caller-supplied message IDs cannot authorize checkout.
   const anchor=this.read('sales_messages',this.key(event.data.contact_id,e.message_id));
   const ids=anchor?.phase==='prepared'&&anchor.eventId===eventId&&(!anchor.anchorMessageId||anchor.anchorMessageId===e.message_id)?anchor.batchMessageIds??[e.message_id]:[];
   const pending=new Set(ids.filter(id=>{const row=this.read('sales_messages',this.key(event.data.contact_id,id));return row?.phase==='prepared'&&row.eventId===eventId&&row.threadHash===anchor.threadHash;}));
   const context=s.pages.flatMap(p=>p.messages).filter(m=>pending.has(m.id)&&m.direction==='inbound'&&m.conversationProviderId===PROVIDER&&m.messageType==='TYPE_CUSTOM_PROVIDER_SMS'&&s.assessments.find(a=>a.messageId===m.id)?.kind==='normal');
   if(e.draft.intent!=='checkout_link'||e.draft.buyingIntentVerified!==true||!context.some(m=>/\b(ready to (buy|pay|sign up|start)|send (me )?(the )?(checkout|payment|sales) link|where (can|do) I (pay|buy|sign up)|how (can|do) I (pay|buy|sign up))\b/i.test(m.body??'')))blocked('relevant_buying_intent_required');
  }
  this.currentMessages=s.pages.flatMap(p=>p.messages);try{return super.begin(owner,eventId,{...e,snapshot:s});}finally{this.currentMessages=null;}
 }
 handoff(owner,eventId,e={}){return this.tx(()=>{
  const event=this.eventGate(owner,eventId),contact=event.data.contact_id,key=this.key(contact),existing=this.read('prospect_alerts',key);
  const resolution=this.read('prospect_owner_resolutions',key);
  const resolvedEntry=this.read('prospect_entries',key);
  if(existing&&resolution?.releasedAlertHash!==hash(canonical(existing))&&releasedAlertHash(this.b,resolvedEntry)!==hash(canonical(existing)))return {locked:true,should_alert:false,send_authorized:false,reason:'contact_handoff_already_recorded'};
  if(typeof e.reason!=='string'||!e.reason.trim()||e.reason.length>300||typeof e.decision_needed!=='string'||!e.decision_needed.trim()||e.decision_needed.length>1000)blocked('handoff_reason_required');
  let question=null,conversation=null,booking='unknown',verified=[];
  if(e.snapshot){const {scope,s}=this.adapt(event,e.snapshot,{full:false});const entry=this.ensureEntry(event,{scope,s,tz:s.timezoneEvidence});const {messages}=this.snapshot(event,entry,s);const m=messages.find(m=>m.id===e.message_id);if(!m||m.direction!=='inbound'||m.at<=this.incomingBoundary(entry)||m.conversationProviderId!==PROVIDER)blocked('handoff_message_scope');question=m.body;conversation=scope.conversationId;booking=s.bookingEvidence;verified=['Contact, conversation and question verified through official MCP.'];}
  const entry=this.read('prospect_entries',key),state=entry?this.contactState(entry):{contactId:contact,manualUntil:0,suppressed:false};state.handoff={eventId,reason:e.reason,at:this.b.now(),status:'chat_alert_prepared'};
  this.write('sales_contacts',key,state);
  const alert={event_id:eventId,contact_id:contact,conversation_id:conversation,record_link:`https://app.gohighlevel.com/v2/location/${LOCATION}/contacts/detail/${contact}`,exact_question:question,context:typeof e.context==='string'?e.context.slice(0,1500):null,booking_status:booking,verified_facts:verified,reason:e.reason,decision_needed:e.decision_needed,receipt_status:'prepared_not_push_read_or_jarvis_receipt'};
  this.write('prospect_alerts',key,alert);this.b.audit('prospect_handoff_prepared',eventId);
  return {locked:true,should_alert:true,alert,send_authorized:false,needs_cole_wording:NEEDS_COLE};
 });}
 contactState(entry){const prior=this.read('sales_contacts',this.key(entry.contactId));if(prior&&!prior.enrollmentHash){prior.enrollmentHash=hash(canonical(entry));return prior;}return super.contactState(entry);}
 recordHandoff(owner,eventId,e={}){return this.tx(()=>{
  const event=this.eventGate(owner,eventId,false),key=this.key(event.data.contact_id),alert=this.read('prospect_alerts',key);
  if(!alert||alert.event_id!==eventId)blocked('handoff_alert_required');
  if(!['notified','queued','failed','unavailable'].includes(e.status)||e.route!=='existing_cloud_chat')blocked('handoff_receipt_required');
  if(e.status==='notified'&&(!validId(e.notificationId)||e.success!==true))blocked('handoff_receipt_required');
  const entry=this.read('prospect_entries',key),state=entry?this.contactState(entry):this.read('sales_contacts',key);
  if(!state?.handoff||state.handoff.eventId!==eventId)blocked('handoff_state_mismatch');
  // A successful configured action is not evidence of push delivery or Cole reading it.
  const receipt={status:e.status,route:e.route,notificationId:e.status==='notified'?e.notificationId:null,at:this.b.now(),reason:e.status==='notified'?null:String(e.reason??'internal_notification_failed').slice(0,300)};
  state.handoff={...state.handoff,...receipt};this.write('sales_contacts',key,state);this.write('prospect_alerts',key,{...alert,notification_receipt:receipt});this.b.audit('prospect_handoff_result',eventId);
  return {recorded:true,status:e.status,notified:e.status==='notified',push_delivered:false,owner_read:false,send_authorized:false};
 });}
}


