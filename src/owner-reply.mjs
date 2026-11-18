// Prepared owner-only, one-attempt experiment. No GHL client or network I/O here.
import {hash, canonical} from './security.mjs';

export const OWNER_SCOPE = Object.freeze({locationId:'portfolio_fixture_01',contactId:'portfolio_fixture_12',conversationId:'portfolio_fixture_13',conversationProviderId:'portfolio_fixture_02'});
export const BASELINE = Object.freeze({id:'portfolio_fixture_14',dateAdded:'2026-10-20T19:58:14.616Z'});
export const TEST_MARKER = 'ATTN CLOUD REPLY TEST';
export const TEST_REPLY = 'ATTN cloud owner test received. This is the one-time test acknowledgement.';
const CAMPAIGN='owner-reply-test-v1', ID=/^[A-Za-z0-9_-]{1,128}$/;
const validID = value => typeof value === 'string' && ID.test(value);
const fail = reason => {const e=new Error(reason);e.reason=reason;throw e;};
const time = value => typeof value==='string' && /Z$/.test(value) && Number.isFinite(Date.parse(value)) ? Date.parse(value) : fail('invalid_time');
const object = x => x && typeof x==='object' && !Array.isArray(x);

export class OwnerReplyLedger {
  constructor(bridge,{enabled=false,notBefore=''}={}) {
    this.b=bridge; this.enabled=enabled; this.notBefore=notBefore;
    if(enabled && (!notBefore || time(notBefore)<=time(BASELINE.dateAdded))) fail('activation_after_test2_required');
    bridge.db.exec(`CREATE TABLE IF NOT EXISTS owner_reply_campaign (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS owner_reply_messages (id TEXT PRIMARY KEY, payload TEXT NOT NULL);`);
  }
  read(table,id) {const row=this.b.db.prepare(`SELECT payload FROM ${table} WHERE id=?`).get(id);return row?this.b.vault.open(row.payload,id):null;}
  write(table,id,value) {this.b.db.prepare(`INSERT INTO ${table}(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`).run(id,this.b.vault.seal(value,id));}
  transaction(fn) {this.b.db.exec('BEGIN IMMEDIATE');try {const x=fn();this.b.db.exec('COMMIT');return x;}catch(e){this.b.db.exec('ROLLBACK');throw e;}}
  key(id) {return hash(canonical({...OWNER_SCOPE,id}));}
  status() {const s=this.read('owner_reply_campaign',CAMPAIGN);return {enabled:this.enabled,customer_replies_enabled:false,phase:s?.phase??'unarmed',baseline:BASELINE,not_before:s?.notBefore??(this.notBefore||null),maximum_attempts:1};}
  gate(owner,eventId) {
    if(!this.enabled || !this.b.enabled) fail('owner_reply_test_disabled');
    if(this.b.now()<=time(this.notBefore)||this.b.now()>time(this.notBefore)+30*60000) fail('owner_test_window_closed');
    if(this.b.contact!==OWNER_SCOPE.contactId) fail('owner_scope_required');
    const e=this.b.eventForOwner(owner,eventId);
    if(e.data.location_id!==OWNER_SCOPE.locationId || e.data.contact_id!==OWNER_SCOPE.contactId) fail('event_scope');
    const c=this.b.db.prepare('SELECT * FROM processing WHERE event_id=? AND owner=?').get(eventId,owner);
    if(!c || c.completed!==null) fail('open_event_claim_required');
    if(time(e.timestamp)<=time(this.notBefore)) fail('event_before_activation');
    return e;
  }
  snapshot(e,s,minRead=0) {
    if(!object(s)||!object(s.contact)||!Array.isArray(s.pages)||s.pages.length<1||s.pages.length>2) fail('snapshot_required');
    const start=time(s.read_started_at),end=time(s.read_completed_at),now=this.b.now();
    if(start<minRead || start<time(e.timestamp)||end<start||end>now||now-start>30000) fail('fresh_snapshot_required');
    if(s.contact.id!==OWNER_SCOPE.contactId||s.contact.locationId!==OWNER_SCOPE.locationId) fail('contact_scope');
    if(s.contact.dnd!==false) fail('dnd_unknown_or_active');
    if(s.contact.dndSettings!==undefined) {
      if(!object(s.contact.dndSettings)) fail('invalid_dnd_settings');
      // Any active/unknown supplied channel restriction blocks this narrow test.
      for(const v of Object.values(s.contact.dndSettings)) if(!object(v)||v.status!=='inactive') fail('channel_dnd_or_unknown');
    }
    const all=[],ids=new Set();let anchor=false;
    for(let i=0;i<s.pages.length;i++) {
      const p=s.pages[i];
      if(!object(p)||!Array.isArray(p.messages)||p.messages.length>50||typeof p.nextPage!=='boolean') fail('invalid_page');
      if((p.nextPage || p.lastMessageId!==undefined && p.lastMessageId!==null) && !validID(p.lastMessageId)) fail('invalid_pagination_cursor');
      if(i===0 && p.request_last_message_id!==null) fail('first_page_required');
      if(i>0 && (!s.pages[i-1].nextPage||!validID(s.pages[i-1].lastMessageId)||!validID(p.request_last_message_id)||p.request_last_message_id!==s.pages[i-1].lastMessageId)) fail('pagination_chain');
      for(const m of p.messages) {
        if(!object(m)||!validID(m.id)||ids.has(m.id)) fail('duplicate_or_invalid_message');ids.add(m.id);
        if(m.locationId!==OWNER_SCOPE.locationId||m.contactId!==OWNER_SCOPE.contactId||m.conversationId!==OWNER_SCOPE.conversationId) fail('message_scope');
        const at=time(m.dateAdded);if(at>end) fail('future_message');
        if(m.id===BASELINE.id) {if(at!==time(BASELINE.dateAdded)||m.direction!=='inbound'||m.conversationProviderId!==OWNER_SCOPE.conversationProviderId) fail('baseline_mismatch');anchor=true;}
        all.push({...m,at});
      }
    }
    // Do not infer complete coverage just from an approximate timestamp or supplied flag.
    if(!anchor && s.pages.at(-1).nextPage) fail('coverage_gap');
    const recent=all.filter(m=>m.at>time(this.notBefore));
    for(const m of recent) {
      if(!['inbound','outbound'].includes(m.direction)) fail('unknown_direction');
      if(m.direction==='outbound') fail('human_or_other_sender_takeover');
      if(m.conversationProviderId!==OWNER_SCOPE.conversationProviderId||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS') fail('provider_or_channel_mismatch');
      if(typeof m.body!=='string'||(m.attachments!==undefined&&(!Array.isArray(m.attachments)||m.attachments.length))) fail('text_only_required');
    }
    recent.sort((a,b)=>a.at-b.at||a.id.localeCompare(b.id));
    if(recent.length>1&&recent[0].at===recent[1].at) fail('ambiguous_chronology');
    return recent;
  }
  prepare(owner,eventId,snapshot) {
    const event=this.gate(owner,eventId),rows=this.snapshot(event,snapshot);
    return this.transaction(()=>{
      let state=this.read('owner_reply_campaign',CAMPAIGN);
      if(state && (state.owner!==owner||state.notBefore!==this.notBefore)) fail('campaign_configuration_changed');
      if(state && ['attempt_unknown','queued','sent','delivered','failed'].includes(state.phase)) return {prepared:false,reason:'one_attempt_already_consumed',phase:state.phase};
      // Persist only normalized evidence, not arbitrary message bodies/provider metadata.
      for(const m of rows) {
        const id=this.key(m.id),prior=this.read('owner_reply_messages',id);
        const evidence={id:m.id,dateAdded:m.dateAdded,provider:m.conversationProviderId,body_hash:hash(m.body),marker:m.body===TEST_MARKER};
        if(prior && canonical(prior.evidence)!==canonical(evidence)) fail('message_evidence_changed');
        this.write('owner_reply_messages',id,{...prior,evidence,state:prior?.state??'pending'});
      }
      if(!rows.length) return {prepared:false,reason:'no_new_ghl_inbound'};
      const first=rows[0];
      if(rows.some(m=>m.body!==TEST_MARKER)) {
        this.write('owner_reply_messages',this.key(first.id),{evidence:this.read('owner_reply_messages',this.key(first.id)).evidence,state:'escalated'});
        return {prepared:false,reason:'non_test_inbound_requires_review',message_id:first.id};
      }
      if(state?.messageId && state.messageId!==first.id) fail('pending_candidate_changed');
      // Across runs never forget a previously observed older pending record.
      const saved=this.b.db.prepare('SELECT id,payload FROM owner_reply_messages').all().map(r=>this.b.vault.open(r.payload,r.id));
      if(saved.some(r=>r.evidence.id!==first.id&&time(r.evidence.dateAdded)<=first.at)) fail('older_pending_message');
      state={owner,notBefore:this.notBefore,phase:'prepared',messageId:first.id,preparedAt:this.b.now(),eventId};
      this.write('owner_reply_campaign',CAMPAIGN,state);
      this.b.audit('owner_reply_prepared',first.id);
      return {prepared:true,message_id:first.id,requires_fresh_preflight:true};
    });
  }
  begin(owner,eventId,snapshot) {
    const event=this.gate(owner,eventId);
    return this.transaction(()=>{
      const s=this.read('owner_reply_campaign',CAMPAIGN);
      if(!s||s.owner!==owner||s.notBefore!==this.notBefore) fail('prepare_required');
      if(s.phase!=='prepared') return {permitted:false,reason:'one_attempt_already_consumed',phase:s.phase};
      if(s.eventId!==eventId) fail('prepared_by_other_event');
      const rows=this.snapshot(event,snapshot,s.preparedAt);
      if(!rows.length||rows[0].id!==s.messageId||rows.some(m=>m.body!==TEST_MARKER)) fail('candidate_changed');
      const ledger=this.read('owner_reply_messages',this.key(s.messageId));
      if(!ledger||ledger.evidence.body_hash!==hash(rows[0].body)||ledger.evidence.dateAdded!==rows[0].dateAdded) fail('candidate_changed');
      // Consume the ONE permit BEFORE the cloud calls GHL. Never renew/reissue it.
      const attemptId='attn_owner_'+hash(CAMPAIGN+':'+s.messageId);
      const next={...s,phase:'attempt_unknown',attemptId,attemptAt:this.b.now()};
      this.write('owner_reply_campaign',CAMPAIGN,next);
      this.write('owner_reply_messages',this.key(s.messageId),{...ledger,state:'attempt_unknown',attemptId});
      this.b.audit('owner_reply_attempt_reserved',s.messageId);
      return {permitted:true,attempt_id:attemptId,expires_at:new Date(this.b.now()+15000).toISOString(),operationId:'send-a-new-message',locationId:OWNER_SCOPE.locationId,idempotencyKey:attemptId,
        params:{body:{type:'Custom',contactId:OWNER_SCOPE.contactId,conversationProviderId:OWNER_SCOPE.conversationProviderId,message:TEST_REPLY,replyMessageId:s.messageId}}};
    });
  }
  record(owner,eventId,result) {
    // Recording may occur after the event acknowledgement; no permission to send is issued.
    this.b.eventForOwner(owner,eventId);
    return this.transaction(()=>{
      const s=this.read('owner_reply_campaign',CAMPAIGN);
      if(!s||s.owner!==owner||s.eventId!==eventId||!s.attemptId||result?.attempt_id!==s.attemptId) fail('attempt_mismatch');
      if(s.phase!=='attempt_unknown' && !((s.phase==='queued' && ['sent','delivered','failed'].includes(result.outcome)) || (s.phase==='sent' && ['delivered','failed'].includes(result.outcome)))) return {recorded:true,already_recorded:true,phase:s.phase};
      if(result.outcome==='unknown') return {recorded:true,phase:'attempt_unknown',retry_allowed:false};
      if(!['queued','sent','delivered','failed'].includes(result.outcome)) fail('invalid_result');
      const m=result.outbound;
      if(s.outboundId && m?.id!==s.outboundId) fail('outbound_id_changed');
      if(!object(m)||!validID(m.id)||m.id===s.messageId||m.locationId!==OWNER_SCOPE.locationId||m.contactId!==OWNER_SCOPE.contactId||m.conversationId!==OWNER_SCOPE.conversationId||m.conversationProviderId!==OWNER_SCOPE.conversationProviderId||m.direction!=='outbound'||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS'||m.body!==TEST_REPLY||time(m.dateAdded)<s.attemptAt||time(m.dateAdded)>this.b.now()) fail('outbound_evidence_required');
      if(!validID(result.send_response?.messageId)||result.send_response?.messageId!==m.id||result.send_response?.conversationId!==OWNER_SCOPE.conversationId) fail('send_response_mismatch');
      const states={queued:['pending','queued'],sent:['sent'],delivered:['delivered','read'],failed:['failed']};
      if(!states[result.outcome].includes(m.status)) fail('delivery_status_mismatch');
      this.write('owner_reply_campaign',CAMPAIGN,{...s,phase:result.outcome,outboundId:m.id});
      const id=this.key(s.messageId),row=this.read('owner_reply_messages',id);
      this.write('owner_reply_messages',id,{...row,state:result.outcome,outboundId:m.id});
      this.b.audit('owner_reply_result_recorded',m.id);
      return {recorded:true,phase:result.outcome,retry_allowed:false};
    });
  }
}


