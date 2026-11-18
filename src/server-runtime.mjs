import {readFileSync} from 'node:fs';
import {GhlAdapter} from './ghl-adapter.mjs';
import {createOpenAIAdapter} from './openai-adapter.mjs';
import {ModelBudget} from './model-budget.mjs';
import {collectEvidence,InboundRunner} from './inbound-runner.mjs';
import {ServerWorker} from './server-worker.mjs';
import {LOCATION,SERVER_WORKER_OWNER} from './bridge.mjs';
import {OwnerAlertEvents} from './owner-alert-events.mjs';
import {SendReconciler} from './send-reconciler.mjs';

export function serverOptions(env){
  const processingMode=env.PROCESSING_MODE||'native';
  if(!['native','server'].includes(processingMode))throw new Error('invalid_processing_mode');
  const enabled=env.SERVER_WORKER_ENABLED==='true';
  if(enabled&&processingMode!=='server')throw new Error('server_worker_mode_required');
  const allowedContacts=env.SERVER_ALLOWED_CONTACTS===undefined?[]:env.SERVER_ALLOWED_CONTACTS==='*'?null:env.SERVER_ALLOWED_CONTACTS.split(',').map(s=>s.trim()).filter(Boolean);
  if(allowedContacts!==null&&(allowedContacts.length>100||allowedContacts.some(c=>! /^[a-zA-Z0-9_-]{1,128}$/.test(c))))throw new Error('invalid_worker_allowlist');
  const concurrency=Number(env.SERVER_WORKER_CONCURRENCY||2),monthlyUsd=Number(env.MODEL_MONTHLY_BUDGET_USD||10);
  if(!Number.isInteger(concurrency)||concurrency<1||concurrency>4||!Number.isFinite(monthlyUsd)||monthlyUsd<=0||monthlyUsd>100)throw new Error('invalid_worker_limits');
  if((enabled||env.SERVER_CONNECTION_CHECK==='true')&&(!env.OPENAI_API_KEY||!env.GHL_PRIVATE_INTEGRATION_TOKEN||env.OPENAI_MODEL!=='gpt-6-luna'))throw new Error('server_api_configuration_required');
  if(enabled&&env.PROSPECT_V1_REPLIES_ENABLED!=='true')throw new Error('server_worker_requires_reply_flag');
  const messageLookup=env.GHL_MESSAGE_LOOKUP||'direct';
  if(!['direct','conversation_history'].includes(messageLookup))throw new Error('invalid_message_lookup');
  return {processingMode,serverWorkerNotBefore:env.SERVER_WORKER_NOT_BEFORE||null,operatorOwner:env.SITE_OWNER_SUBJECT||'',enabled,allowedContacts,concurrency,monthlyUsd,messageLookup,model:env.OPENAI_MODEL||null,check:env.SERVER_CONNECTION_CHECK==='true'};
}

export function createRuntime({bridge,env,options,fetchImpl=fetch}){
  const status={configured:false,worker_enabled:options.enabled,model:options.model,connection_check:null};
  if(!options.enabled&&!options.check)return {status,worker:null,check:async()=>null};
  const ownerAlerts=options.operatorOwner?new OwnerAlertEvents({bridge,operatorOwner:options.operatorOwner}):null;
  const alertDrains=new Set();
  const drainAlerts=()=>{
    if(!ownerAlerts)return;
    const p=Promise.resolve().then(()=>{
      if(bridge.processingMode==='server'){
        // Recovery can terminate an abandoned job without launching a runner.
        // Persist those failures too, and recover an enqueue interrupted by crash.
        const jobs=bridge.db.prepare("SELECT j.event_id FROM server_jobs j LEFT JOIN server_alerts a ON a.event_id=j.event_id WHERE j.state IN ('failed','needs_review') AND a.id IS NULL LIMIT 20").all();
        for(const job of jobs)bridge.persistTechnicalServerAlert(SERVER_WORKER_OWNER,job.event_id);
        const alerts=bridge.db.prepare('SELECT a.id,a.payload FROM server_alerts a LEFT JOIN owner_alert_receipts r ON r.alert_id=a.id WHERE r.alert_id IS NULL LIMIT 20').all();
        for(const row of alerts){const alert=bridge.vault.open(row.payload,row.id);ownerAlerts.enqueue(row.id,alert.kind??'handoff');}
      }
      return ownerAlerts.drain();
    }).catch(()=>bridge.audit('owner_alert_drain_failed'));alertDrains.add(p);p.finally(()=>alertDrains.delete(p));
  };
  const ghl=new GhlAdapter({token:env.GHL_PRIVATE_INTEGRATION_TOKEN,messageLookup:options.messageLookup||'direct',fetchImpl});
  const budget=new ModelBudget({db:bridge.db,monthlyLimitUsd:options.monthlyUsd,model:options.model,rates:{inputUsdPerMillion:0.10,outputUsdPerMillion:0.50},now:()=>bridge.now()});
  const model=createOpenAIAdapter({apiKey:env.OPENAI_API_KEY,model:options.model,reasoningEffort:'none',fetchImpl,allowRequest:r=>budget.allowRequest(r),recordUsage:r=>budget.recordUsage(r)});
  const adapters={snapshot:({event,candidateId,historyRequirements,signal})=>collectEvidence({execute:ghl.execute,assess:model.assess,contactId:event.data.contact_id,candidateId,historyRequirements,signal}),generate:model.generate,send:ghl.send,verify:ghl.verify,
    ...(ownerAlerts?{notify:async(alert,{storedAlertId})=>{ownerAlerts.enqueue(storedAlertId,'handoff');drainAlerts();return {success:false,queued:true,reason:'owner_alert_queued'};},notifyTechnical:async alertId=>{ownerAlerts.enqueue(alertId,'technical_failure');drainAlerts();}}:{})};
  const systemPrompt=readFileSync(new URL('../docs/SERVER-BOT-PROMPT.txt',import.meta.url),'utf8');
  const worker=options.enabled?new ServerWorker({bridge,adapters,concurrency:options.concurrency,allowedContacts:options.allowedContacts,runnerFactory:o=>new InboundRunner({...o,systemPrompt})}):null;
  const reconciler=options.enabled?new SendReconciler({bridge,ghl}):null;
  status.configured=true;
  const safeFailure=e=>/^[a-z][a-z0-9_]{0,100}$/.test(e?.reason??'')?e.reason:'connection_check_failed';
  // Owner-only read probe: no claim, enrollment, historical replay, GHL write or
  // customer message. The model probe uses invented data and cannot send.
  const check=async()=>{
    if(!options.check)return null;
    status.connection_check={state:'running',started_at:new Date(bridge.now()).toISOString()};
    const results={};
    try{
      const contactId=env.TEST_CONTACT_ID;
      const requests=[['contact','get-contact',{path:{contactId}}],['conversations','search-conversation',{query:{contactId,limit:2}}],['appointments','get-appointments-for-contact',{path:{contactId}}],['opportunities','search-opportunity',{query:{contactId,status:'all',limit:100}}],['transactions','list-transactions',{query:{altType:'location',locationId:LOCATION,contactId,limit:100,offset:0}}]];
      const readResults=await Promise.allSettled(requests.map(async([label,operationId,params])=>{
        const r=await ghl.execute({operationId,locationId:LOCATION,params});
        results[label]={status:r.status,success:r.success,data_keys:Object.keys(r.data??{})};
        if(label==='contact')results[label].record_keys=Object.keys(r.data?.contact??{});
        const list=r.data?.events??r.data?.opportunities??(Array.isArray(r.data?.data)?r.data.data:null);
        if(list)results[label].records=list.map(x=>({keys:Object.keys(x),status:x.status??null,appointmentStatus:x.appointmentStatus??null,scope_matches:x.contactId===contactId&&(x.locationId===LOCATION||label==='transactions'&&x.altId===LOCATION&&x.altType==='location')}));
        if(label==='conversations'&&r.data?.conversations?.length===1){
          const v=r.data.conversations[0];
          const ms=await ghl.execute({operationId:'get-messages',locationId:LOCATION,params:{path:{conversationId:v.id},query:{limit:50}}});
          const items=ms.data?.messages?.messages??[];
          results.messages={status:ms.status,success:ms.success,count:items.length,record_keys:items[0]?Object.keys(items[0]):[],message_types:[...new Set(items.map(m=>m.messageType))],scope_complete:items.every(m=>m.contactId===contactId&&m.locationId===LOCATION&&m.conversationId===v.id)};
          if(items.length){
            const first=items[0],one=await ghl.execute({operationId:'get-message',locationId:LOCATION,params:{path:{id:first.id}}});
            results.message_detail={status:one.status,success:one.success,source:one.source??null,record_keys:Object.keys(one.data?.message??{}),identity_matches:['id','contactId','locationId','conversationId','conversationProviderId','direction','messageType','body','dateAdded'].every(k=>one.data?.message?.[k]===first[k])};
          }else results.message_detail={success:false,reason:'no_owner_message_for_schema_probe'};
        }else if(label==='conversations'){
          results.messages={success:false,reason:'unique_owner_conversation_required'};
        }
      }));
      readResults.forEach((r,i)=>{if(r.status==='rejected')results[requests[i][0]]={success:false,reason:safeFailure(r.reason)};});
      const sample={id:'synthetic_connection_probe',contactId:'synthetic_connection_probe_contact',locationId:LOCATION,conversationId:'synthetic_connection_probe_thread',direction:'inbound',dateAdded:new Date().toISOString(),body:'What does Google Ads cost?',messageType:'TYPE_CUSTOM_PROVIDER_SMS'};
      const modelFailures=[];
      for(let attempt=1;attempt<=2;attempt++){
        try{
          const a=await model.assess({messages:[sample],contactId:sample.contactId,notBefore:'2026-01-01T00:00:00.000Z'}),kind=a[0]?.kind;
          results.model={success:kind==='normal',model:options.model,attempts:attempt,
            ...(kind==='normal'?{}:{reason:'model_probe_classification_unexpected',observed_kind:['normal','opt_out','system','handoff','unsafe'].includes(kind)?kind:'unrecognized'}),
            ...(modelFailures.length?{prior_failures:[...modelFailures]}:{})};
          break;
        }catch(e){
          const reason=safeFailure(e);modelFailures.push(reason);
          // One repeat of this invented read-only probe may recover malformed
          // response/assessment output. Auth, rate, quota, budget and semantic
          // classification failures never gain an automatic startup retry.
          if(attempt<2&&['model_response_invalid','assessment_schema_invalid'].includes(reason))continue;
          results.model={success:false,model:options.model,reason,attempts:attempt,failures:[...modelFailures]};break;
        }
      }
      status.connection_check={state:Object.values(results).every(x=>x.success===true&&x.scope_complete!==false&&x.identity_matches!==false)?'passed':'failed',checked_at:new Date(bridge.now()).toISOString(),results};
      return status.connection_check;
    }catch(e){status.connection_check={state:'failed',reason:safeFailure(e),results};return status.connection_check;}
  };
  return {status,worker,check,budget,ghl,model,ownerAlerts,reconciler,drainAlerts,waitForAlerts:()=>Promise.allSettled([...alertDrains])};
}

