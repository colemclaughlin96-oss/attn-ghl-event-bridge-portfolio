// Distinct, owner-only, ONE ordinary send campaign. No reset API, no credentials/network calls.
import {SalesReplyLedger} from './sales-reply.mjs';
import {OWNER_SCOPE as S} from './owner-reply.mjs';
import {blocked,utc,validId,NEEDS_COLE} from './sales-policy.mjs';
import {canonical,hash} from './security.mjs';
import {ownerEvidence,exactPriorAutomation,AUTOMATED_OWNER_ACK,CONFIRMED_MANUAL,MANUAL_RELEASE,exactConfirmedManual} from './owner-ai-evidence.mjs';
export const PILOT_ID='owner-ai-pilot-v1';
const TABLES={sales_contacts:'owner_ai_contacts',sales_messages:'owner_ai_messages',sales_receipts:'owner_ai_receipts'};
const object=x=>x&&typeof x==='object'&&!Array.isArray(x);
export function validatePilotConfig(c){
  if(!object(c)||c.campaign!==PILOT_ID||c.locationId!==S.locationId||c.contactId!==S.contactId||c.conversationId!==S.conversationId||c.providerId!==S.conversationProviderId||c.maximum_attempts!==1)blocked('owner_ai_fixed_scope_required');
  const start=utc(c.not_before),end=utc(c.expires_at);
  if(start<=utc(AUTOMATED_OWNER_ACK.dateAdded)||end<=start||end-start>20*60000)blocked('owner_ai_short_window_required');
  if(start<utc(MANUAL_RELEASE))blocked('confirmed_manual_takeover_24h');
  if(!validId(c.baseline?.id)||utc(c.baseline.dateAdded)>start)blocked('owner_ai_baseline_required');
  if(c.timezone?.value!=='America/Los_Angeles'||c.timezone.source!=='owner_provided'||c.timezone.confirmed_on!=='2026-10-20'||c.timezone.evidenceRef!=='cole_timezone_confirmation_20261020')blocked('owner_timezone_evidence_required');
  const w=c.workflow_coexistence;
  if(!object(w)||w.locationId!==S.locationId||w.contactId!==S.contactId||w.status!=='owner_active_enrollments_empty'||w.source!=='local_coordinator_rendered_ui'||w.external_queues_verified!==false||!validId(w.id)||!Array.isArray(w.checked_workflow_ids)||!w.checked_workflow_ids.length||w.checked_workflow_ids.some(id=>!validId(id))||w.owner_specific_verified!==true||utc(w.verified_at)>start||start-utc(w.verified_at)>3600000||utc(w.expires_at)<end)blocked('owner_workflow_evidence_required');
  return c;
}
export class OwnerAiPilot extends SalesReplyLedger {
  constructor(bridge,{enabled=false,config=null}={}){
    if(enabled)validatePilotConfig(config);
    super(bridge,{enabled:false});
    for(const t of ['owner_ai_contacts','owner_ai_messages','owner_ai_receipts','owner_ai_alerts','owner_ai_campaign'])bridge.db.exec(`CREATE TABLE IF NOT EXISTS ${t}(id TEXT PRIMARY KEY,payload TEXT NOT NULL);`);
    const old=this.campaign();
    if(config)validatePilotConfig(config);
    if(old&&config&&old.configHash!==hash(canonical(config)))blocked('owner_ai_campaign_config_immutable');
    this.config=config??old?.config??null;this.enabled=enabled;
    if(enabled&&(!bridge.enabled||bridge.contact!==S.contactId||bridge.ownerReply.enabled))blocked('owner_ai_listener_or_legacy_gate');
    if(enabled&&!old)this.tx(()=>{const concurrent=this.campaign();if(concurrent&&concurrent.configHash!==hash(canonical(config)))blocked('owner_ai_campaign_config_immutable');if(!concurrent)this.write('owner_ai_campaign',PILOT_ID,{config:config,configHash:hash(canonical(config)),attempts:0,phase:'armed',createdAt:bridge.now()});});
    this.enrollments=this.config?[{contactId:S.contactId,conversationId:S.conversationId,mode:'allowlist',approvalRef:'cole_owner_ai_pilot_review',baseline:this.config.baseline,notBefore:this.config.not_before,timezone:{value:'America/Los_Angeles',source:'owner_verified',evidenceRef:'cole_timezone_confirmation_20261020'},takeoverClearanceRef:'fresh_24h_raw_mcp_history',workflowProfileRevision:this.config.workflow_coexistence.id}]:[];
  }
  read(table,id){return super.read(TABLES[table]??table,id);}
  write(table,id,x){return super.write(TABLES[table]??table,id,x);}
  rows(contact){return this.b.db.prepare('SELECT id,payload FROM owner_ai_messages').all().map(r=>this.b.vault.open(r.payload,r.id)).filter(r=>r.contactId===contact);}
  campaign(){return this.read('owner_ai_campaign',PILOT_ID);}
  status(){const s=this.campaign();return {campaign:PILOT_ID,enabled:this.enabled,phase:s?.phase??'unarmed',attempts_consumed:s?.attempts??0,maximum_attempts:1,not_before:this.config?.not_before??null,expires_at:this.config?.expires_at??null,customer_replies_enabled:false,post_cap_handoff_enabled:this.enabled,booking_writes_enabled:false};}
  gate(owner,eventId,open=true){
    if((open&&!this.enabled)||!this.config||!this.b.enabled)blocked('owner_ai_pilot_disabled');
    const e=this.b.eventForOwner(owner,eventId),c=this.b.db.prepare('SELECT * FROM processing WHERE event_id=? AND owner=?').get(eventId,owner);
    if(e.data.location_id!==S.locationId||e.data.contact_id!==S.contactId||e.data.source!=='loop_workflow'||this.b.contact!==S.contactId)blocked('owner_ai_event_scope');
    if(!c||(open&&c.completed!==null))blocked('open_event_claim_required');
    if(utc(e.timestamp)<=utc(this.config.not_before)||utc(e.timestamp)>this.b.now())blocked('owner_ai_event_outside_start');
    return {event:e,entry:this.enrollments[0]};
  }
  snapshot(event,entry,s,minRead=0){
    const x=super.snapshot(event,entry,s,minRead),cut=x.start-86400000;
    // Every snapshot proves actual 24h coverage, not a model-supplied clearance flag.
    if(x.messages.some((m,i)=>i>0&&m.at>x.messages[i-1].at))blocked('owner_history_order_required');
    if(s.pages.at(-1).nextPage&&!x.messages.some(m=>m.at<=cut))blocked('owner_24h_history_incomplete');
    for(const m of x.messages)if(m.id===AUTOMATED_OWNER_ACK.id&&!exactPriorAutomation(m))blocked('prior_automation_evidence_mismatch');
    for(const m of x.messages)if(m.id===CONFIRMED_MANUAL.id&&!exactConfirmedManual(m))blocked('confirmed_manual_evidence_mismatch');
    return x;
  }
  isVerifiedManual(m){return exactConfirmedManual(m);}
  knownOutboundIds(entry,saved){return new Set([...super.knownOutboundIds(entry,saved),AUTOMATED_OWNER_ACK.id]);}
  permission(x){
    // Safety observation/holds precede cap checks, so a later genuine question can hand off.
    if(x.state.handoff||x.state.suppressed||x.state.externalLock||x.state.manualUntil>this.b.now())return super.permission(x);
    if(this.b.now()<utc(MANUAL_RELEASE))return 'confirmed_manual_takeover_24h';
    if((this.campaign()?.attempts??0)>=1)return 'owner_ai_total_attempt_consumed';
    if(this.b.now()<utc(this.config.not_before)||this.b.now()+15000>utc(this.config.expires_at)||utc(x.event.timestamp)>utc(this.config.expires_at))return 'owner_ai_send_window_closed';
    if(this.b.now()>utc(this.config.workflow_coexistence.expires_at))return 'owner_workflow_evidence_expired';
    return super.permission(x);
  }
  reserveCampaign(entry,row,attemptId,at){
    const s=this.campaign();if(!s||s.attempts!==0)blocked('owner_ai_total_attempt_consumed');
    this.write('owner_ai_campaign',PILOT_ID,{...s,attempts:1,phase:'attempt_unknown',attemptId,eventId:row.eventId,messageId:row.messageId,attemptAt:at});
  }
  recordCampaign(row,outcome,outboundId){const s=this.campaign();if(!s||s.attempts!==1||s.attemptId!==row.attemptId)blocked('owner_ai_campaign_attempt_mismatch');this.write('owner_ai_campaign',PILOT_ID,{...s,phase:outcome,outboundId});}
  prepare(owner,eventId,raw){this.gate(owner,eventId);return super.prepare(owner,eventId,ownerEvidence(raw,{workflowRef:this.config.workflow_coexistence}));}
  begin(owner,eventId,e){this.gate(owner,eventId);return super.begin(owner,eventId,{...e,snapshot:ownerEvidence(e?.snapshot,{candidateId:e?.message_id,workflowRef:this.config.workflow_coexistence})});}
  handoff(owner,eventId,e={}){return this.tx(()=>{
    const {event,entry}=this.gate(owner,eventId);
    if(!validId(e.message_id)||!['owner_requested','unknown_answer','scheduling','complaint','conflict','unsafe'].includes(e.reason))blocked('owner_ai_handoff_evidence_required');
    const snapshot=ownerEvidence(e.snapshot,{full:false});
    const {messages}=this.snapshot(event,entry,snapshot);
    const m=messages.find(x=>x.id===e.message_id);
    if(!m||m.direction!=='inbound'||m.conversationProviderId!==S.conversationProviderId||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS'||m.at<=utc(this.config.not_before)||typeof m.body!=='string')blocked('owner_ai_handoff_message_scope');
    const state=this.contactState(entry);state.handoff??={eventId,messageId:m.id,status:'chat_alert_pending',at:this.b.now()};
    this.write('sales_contacts',this.key(entry.contactId),state);
    const key=this.key(entry.contactId,'alert_'+m.id),prior=this.read('owner_ai_alerts',key);
    if(prior)return {locked:true,should_alert:false,reason:'alert_already_prepared',send_authorized:false};
    if(typeof e.context!=='string'||e.context.length>1500||typeof e.decision_needed!=='string'||!e.decision_needed.trim()||e.decision_needed.length>1000)blocked('owner_ai_alert_context_required');
    const alert={event_id:eventId,message_id:m.id,location_id:S.locationId,contact_id:S.contactId,conversation_id:S.conversationId,record_link:'https://app.gohighlevel.com/v2/location/portfolio_fixture_01/contacts/detail/portfolio_fixture_12',exact_question:m.body,context:e.context,booking_status:snapshot.bookingEvidence,verified_facts:['Exact owner/thread/provider inbound verified from official MCP evidence.','Durable unresolved contact hold recorded.'],decision_needed:e.decision_needed,receipt_status:'prepared_not_delivery_or_read_proof',prepared_at:new Date(this.b.now()).toISOString()};
    this.write('sales_receipts',this.key(entry.contactId,eventId+':handoff'),{eventId,messageId:m.id,snapshotHash:hash(canonical(snapshot)),observedAt:this.b.now()});
    this.write('owner_ai_alerts',key,alert);this.b.audit('owner_ai_handoff_prepared',m.id);
    return {locked:true,should_alert:true,alert,needs_cole_wording:NEEDS_COLE,send_authorized:false,push_receipt_verified:false,owner_read_verified:false,jarvis_notification_verified:false};
  });}
}


