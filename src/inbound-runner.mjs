// Development orchestrator. No credentials, timers, model client or production activation.
// Adapters must be provided by the authenticated host. All sends use the existing
// ledger's consumed permit; timeouts never trigger a second send.
import {LOCATION, validateGeneration, blocked, validId, validatePriceDraft} from './sales-policy.mjs';
import {setTimeout as delay} from 'node:timers/promises';
import {readFileSync} from 'node:fs';
import {hash} from './security.mjs';

// Read stable approved instructions once per worker, never CRM/contact data.
const SYSTEM_PROMPT=readFileSync(new URL('../docs/INBOUND-RELIABILITY-PROMPT.txt',import.meta.url),'utf8');
const POLICY_REVISION=hash(SYSTEM_PROMPT);

export const LIMITS=Object.freeze({budgetMs:45000,readMs:8000,modelMs:15000,sendMs:8000,visibilityAttempts:3,contextRefreshes:2,generationAttempts:2});
const failure=e=>e?.reason==='ghl_http_error'&&e.status>=500?'ghl_provider_unavailable':typeof e?.reason==='string'?e.reason:e?.name==='TimeoutError'?'tool_timeout':'adapter_failure';
const transient=new Set(['no_pending_inbound','thread_changed','fresh_snapshot_required','selected_message_mismatch']);
const humanReasons=new Set(['owner_excluded_client_or_conflict','client_or_suppression_tag','client_paid_sold_or_conflict','opportunity_conflict','payment_record_requires_review','appointment_conflict','multiple_bookings','unresolved_handoff','unverified_external_sender','unsupported_message_content']);
const safeReason=r=>String(r||'unspecified').replace(/[^a-z0-9_]/gi,'_').slice(0,100);

// A timer limits waiting; it does not imply a remote write was cancelled.
export async function bounded(fn,ms,reason='tool_timeout'){
  const controller=new AbortController();let timer;
  try{return await Promise.race([Promise.resolve().then(()=>fn(controller.signal)),new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();const e=new Error(reason);e.reason=reason;reject(e);},Math.max(1,ms));})]);}
  finally{clearTimeout(timer);}
}

// All records remain contact-scoped. Stable business facts live in the cached
// prompt; appointment/consent/payment/takeover data is fetched again for preflight.
export async function collectEvidence({execute,assess,contactId,candidateId=null,historyRequirements=null,now=Date.now,signal,parallel=true,maxPages=10}){
  if(!validId(contactId))blocked('contact_id_required');
  const started=now(),reads=[];
  const read=async(operationId,params)=>{
    const result=await execute({operationId,locationId:LOCATION,params,signal});
    if(result?.status===429){const e=new Error('provider_rate_limited');e.reason='provider_rate_limited';throw e;}
    if(result?.success!==true||result.status!==200||result.operationId!==operationId)blocked('official_read_failed');
    const r={operationId,locationId:LOCATION,params,result};reads.push(r);return result.data;
  };
  const conversation=async()=>{
    const d=await read('search-conversation',{query:{contactId,limit:2}});
    if(d.total!==1||d.conversations?.length!==1||d.conversations[0].contactId!==contactId||d.conversations[0].locationId!==LOCATION)blocked('unique_contact_conversation_required');
    return d.conversations[0].id;
  };
  const history=async()=>{
    const id=await conversation();let cursor=null;
    for(let i=0;i<maxPages;i++){
      const d=await read('get-messages',{path:{conversationId:id},query:cursor?{limit:50,lastMessageId:cursor}:{limit:50}});
      const p=d.messages;
      if(!p||!Array.isArray(p.messages))blocked('invalid_messages_response');
      if(!p.nextPage)break;
      if(historyRequirements&&historyRequirements.contact_id===contactId){
        const seen=reads.filter(r=>r.operationId==='get-messages').flatMap(r=>r.result.data.messages.messages);
        const boundary=seen.some(m=>m.id===historyRequirements.baseline_id||m.id===historyRequirements.checkpoint_id||(historyRequirements.initial_cutoff&&Date.parse(m.dateAdded)<=Date.parse(historyRequirements.initial_cutoff)));
        if(boundary&&seen.some(m=>Date.parse(m.dateAdded)<=Date.parse(historyRequirements.takeover_before)))break;
      }
      if(!validId(p.lastMessageId)||p.lastMessageId===cursor)blocked('pagination_chain');
      // Fetch enough for complete last-24h takeover evidence plus any older
      // pending/baseline context (up to a hard limit). The ledger checks coverage.
      cursor=p.lastMessageId;
    }
  };
  const jobs=[
    ()=>read('get-contact',{path:{contactId}}),
    history,
    ()=>read('get-appointments-for-contact',{path:{contactId}}),
    ()=>read('search-opportunity',{query:{contactId,status:'all',limit:100}}),
    ()=>read('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId,limit:100,offset:0}})
  ];
  if(candidateId)jobs.push(()=>read('get-message',{path:{id:candidateId}}));
  if(parallel){const rs=await Promise.allSettled(jobs.map(j=>j()));const bad=rs.find(r=>r.status==='rejected');if(bad)throw bad.reason;}
  else for(const j of jobs)await j();
  const messages=reads.filter(r=>r.operationId==='get-messages').flatMap(r=>r.result.data.messages.messages);
  const assessments=await assess({messages,contactId,signal,notBefore:historyRequirements?.incoming_not_before??historyRequirements?.initial_cutoff,handledMessageIds:historyRequirements?.handled_message_ids??[]});
  return {read_started_at:new Date(started).toISOString(),read_completed_at:new Date(now()).toISOString(),reads,assessments};
}

export class InboundRunner{
  constructor({bridge,owner,adapters,limits={},now=Date.now,sleep=ms=>delay(ms),systemPrompt=SYSTEM_PROMPT}){
    this.b=bridge;this.owner=owner;this.a=adapters;this.limits={...LIMITS,...limits};this.now=now;this.sleep=sleep;
    this.systemPrompt=systemPrompt;this.policyRevision=hash(systemPrompt);
    if(!adapters?.snapshot||!adapters.generate||!adapters.send||!adapters.verify)blocked('runner_adapters_required');
  }
  async run(eventId,{recover=false}={}){
    const start=this.now(),l=this.limits,trace=[];
    const claim=this.b.claim(this.owner,eventId,{fenced:true,recover});
    if(!claim.claimed)return {category:'suppressed',reason:claim.reason??'event_already_claimed',trace,totalMs:this.now()-start};
    const token=claim.claim_token;
    const event=this.b.eventForOwner(this.owner,eventId);
    const stage=(name,reason)=>{
      const r={stage:name,reason:safeReason(reason),elapsedMs:this.now()-start};trace.push(r);
      this.b.recordStage(this.owner,eventId,{claim_token:token,stage:name,reason:r.reason});
    };
    const remaining=()=>l.budgetMs-(this.now()-start);
    const call=(fn,cap,reason)=>{if(remaining()<=0)blocked('processing_budget_exhausted');return bounded(fn,Math.min(cap,remaining()),reason);};
    // A snapshot includes both bounded GHL reads and model classification.
    // Keep individual read/model limits intact; the combined evidence stage may
    // use their sum, still capped by the unchanged remaining processing budget.
    const evidenceCap=l.readMs+l.modelMs;
    const ledger=(method,evidence,late=false)=>this.b.withClaim(this.owner,eventId,token,()=>this.b.prospectV1[method](this.owner,eventId,evidence),{open:!late,allowExpired:late});
    const done=(category,reason,extra={})=>{
      stage(category==='suppressed'?'suppressed':category==='deferred'?'deferred':category==='failed'?'failed':'completed',reason);
      const retryReason=({evidence_timeout:'tool_timeout',preflight_timeout:'tool_timeout',adapter_failure:'temporary_tool_failure',ghl_network_failure:'temporary_tool_failure',ghl_provider_unavailable:'temporary_tool_failure',ghl_request_timeout:'tool_timeout',ghl_request_aborted:'tool_timeout',model_transport_failed:'model_failure',model_provider_unavailable:'model_failure',model_response_invalid:'model_failure',assessment_schema_invalid:'model_failure',consent_evidence_unknown:'temporary_tool_failure',generation_timeout:'model_timeout',generation_incomplete:'model_failure',processing_budget_exhausted:'temporary_tool_failure'})[reason]??reason;
      if(!permit?.permitted&&new Set(['no_pending_inbound','thread_changed','another_event_owns_candidate','prior_send_unresolved_or_failed','tool_timeout','temporary_tool_failure','model_timeout','model_failure','fresh_snapshot_required']).has(retryReason)){
        const deferred=this.b.deferProcessing(this.owner,eventId,{claim_token:token,reason:retryReason,retryAfterMs:1000});
        return {category:deferred.deferred?'deferred':'failed',reason,originalCategory:category,...extra,recovery:deferred,trace,totalMs:this.now()-start};
      }
      this.b.processed(this.owner,eventId,['handoff','failed'].includes(category)?'escalated':category==='suppressed'?'skipped':'observed',{claim_token:token,stage:trace.at(-1).stage,reason:safeReason(reason)});
      return {category,reason,...extra,trace,totalMs:this.now()-start};
    };
    const handoff=async(reason,snapshot,messageId,decision)=>{
      stage('handoff',reason);
      messageId??=snapshot?.reads?.filter(r=>r.operationId==='get-messages').flatMap(r=>r.result.data.messages.messages).find(m=>m.direction==='inbound')?.id;
      if(snapshot)snapshot={...snapshot,reads:snapshot.reads.filter(r=>['search-conversation','get-contact','get-messages','get-appointments-for-contact'].includes(r.operationId))};
      const h=ledger('handoff',{reason,message_id:messageId,snapshot,decision_needed:decision||'Cole to review the question and contact state.',context:'Incoming conversation requires human input. See exact question and booking state in the alert.'});
      if(!h.should_alert)return done('handoff','handoff_already_recorded');
      let receipt;
      try{receipt=this.a.notify?await call(signal=>this.a.notify(h.alert,{signal}),l.readMs,'handoff_timeout'):null;}
      catch{receipt=null;}
      const status=receipt?.success===true&&validId(receipt.notificationId)?'notified':receipt?.queued===true?'queued':this.a.notify?'failed':'unavailable';
      ledger('recordHandoff',{status,success:status==='notified',route:'existing_cloud_chat',notificationId:receipt?.notificationId??null},true);
      // No customer acknowledgment is sent here; a host can only claim successful
      // notification after its actual configured action returns a receipt.
      return done('handoff',status==='notified'?'handoff_notified':'handoff_'+status,{alert:h.alert,notificationVerified:status==='notified'});
    };
    let generated=false,permit=null,snapshot,prep,refreshes=0,visibility=0,generations=0,repair=null;
    try{
      while(remaining()>0){
        stage('evidence',repair?'refresh_or_repair':'initial_snapshot');
        const historyRequirements=ledger('historyRequirements');
        snapshot=await call(signal=>this.a.snapshot({event,historyRequirements,signal}),evidenceCap,'evidence_timeout');
        prep=ledger('prepare',snapshot);
        if(!prep.prepared){
          if(prep.reason==='no_pending_inbound'&&snapshot.assessments?.length&&snapshot.assessments.every(a=>a.kind==='system'))return done('suppressed','ignored_system_events');
          if(prep.reason==='no_pending_inbound'&&++visibility<l.visibilityAttempts){await this.sleep(visibility*250);continue;}
          if(humanReasons.has(prep.reason))return await handoff(prep.reason,snapshot,null);
          return done(transient.has(prep.reason)?'deferred':'suppressed',prep.reason);
        }
        // One repair for incomplete/invalid model output, not endless regeneration.
        let result;
        for(let attempt=0;attempt<l.generationAttempts;attempt++){
          stage('generation',repair||'draft_batch');
          try{
            result=validateGeneration(await call(signal=>this.a.generate({event,snapshot,preparation:prep,repair,signal,maxOutputTokens:650,systemPrompt:this.systemPrompt,policyRevision:this.policyRevision}),l.modelMs,'generation_timeout'));
            if(!result.handoff&&result.draft.intent==='pricing')validatePriceDraft(result.draft);
            break;
          }catch(e){
            repair=failure(e);stage('generation',repair);
            if(new Set(['provider_rate_limited','model_authentication_failed','model_budget_exhausted','model_budget_hook_failed','model_usage_record_failed','model_request_rejected']).has(repair))return done('failed',repair,{generationFailed:true});
            if(attempt+1===l.generationAttempts)return done('failed',repair,{generationFailed:true});
          }
        }
        generations++;
        if(result.handoff)return await handoff(result.handoff.reason,snapshot,prep.message_id,result.handoff.decision_needed);
        generated=true;
        stage('preflight','fresh_consent_takeover_booking_and_thread');
        const fresh=await call(signal=>this.a.snapshot({event,candidateId:prep.message_id,historyRequirements:ledger('historyRequirements'),signal}),evidenceCap,'preflight_timeout');
        try{permit=ledger('begin',{snapshot:fresh,draft:result.draft,message_id:prep.message_id,message_ids:prep.message_ids,thread_version:prep.thread_version});}
        catch(e){
          const reason=failure(e);
          if(['invalid_draft','one_question_only','draft_review_required','approved_price_text_required','approved_price_facts_required','already_booked'].includes(reason)&&refreshes++<l.contextRefreshes){repair=reason;continue;}
          throw e;
        }
        if(!permit.permitted){
          if((permit.retryable||transient.has(permit.reason))&&refreshes++<l.contextRefreshes){repair=permit.reason;permit=null;continue;}
          if(humanReasons.has(permit.reason))return await handoff(permit.reason,fresh,prep.message_id);
          return done('suppressed',permit.reason,{generatedNotSent:true});
        }
        stage('send_reserved','one_attempt_consumed');
        if(Date.parse(permit.expires_at)<=this.now())return done('failed','permit_expired_before_send',{generatedNotSent:true,reconciliationRequired:true});
        // A rejected/timeout send may have reached the provider: consume once.
        let sent;
        try{sent=await call(signal=>this.a.send(permit,{signal}),l.sendMs,'send_timeout');}
        catch(e){ledger('record',{attempt_id:permit.attempt_id,outcome:'unknown'},true);return done('failed',failure(e),{generatedNotSent:false,sendAttempted:true,delivery:'unknown',reconciliationRequired:true});}
        stage('send_reserved','provider_response_received_unverified');
        // Preserve the actual accepted response identity before the first lookup.
        // History can lag the send response. This authorizes only later exact-ID
        // verification; the consumed attempt stays unknown and can never resend.
        ledger('record',{attempt_id:permit.attempt_id,outcome:'unknown',send_response:{messageId:sent?.messageId,conversationId:sent?.conversationId}},true);
        let verified;
        try{verified=await call(signal=>this.a.verify(sent,{event,permit,signal}),l.readMs,'delivery_verification_timeout');}
        catch(e){ledger('record',{attempt_id:permit.attempt_id,outcome:'unknown'},true);return done('failed',failure(e),{sendAttempted:true,delivery:'unknown',reconciliationRequired:true});}
        const outcome=verified?.outcome;
        if(!['queued','sent','delivered','failed'].includes(outcome)){ledger('record',{attempt_id:permit.attempt_id,outcome:'unknown'},true);return done('failed','send_status_unknown',{delivery:'unknown',reconciliationRequired:true});}
        ledger('record',{attempt_id:permit.attempt_id,...verified},true);
        if(outcome!=='failed')stage('send_accepted','verified_provider_message');
        if(outcome==='delivered')stage('delivery_confirmed','provider_confirmed_delivery');
        return done(outcome==='failed'?'failed':'replied',outcome==='failed'?'provider_delivery_failed':outcome,{delivery:outcome,generations,coveredMessageIds:prep.message_ids??[prep.message_id],reconciliationRequired:['queued','failed'].includes(outcome)});
      }
      return done('deferred','processing_budget_exhausted',{generatedNotSent:generated&&!permit?.permitted});
    }catch(e){
      const reason=failure(e);
      // Technical failures do not create a permanent contact-level human hold.
      return done('failed',reason,{generatedNotSent:generated&&!permit?.permitted,reconciliationRequired:!!permit?.permitted});
    }
  }
}

