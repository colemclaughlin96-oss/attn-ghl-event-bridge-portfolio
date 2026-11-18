import http from 'node:http';
import {prospectOptions} from './prospect-v1.mjs';
import {pathToFileURL} from 'node:url';
import fs from 'node:fs';
import path from 'node:path';
import {Bridge, LOCATION, RpcError} from './bridge.mjs';
import {equal,verifyGhl,hash} from './security.mjs';
import {serverOptions,createRuntime} from './server-runtime.mjs';
import {inspectOwnerHold,resolveOwnerHold} from './owner-hold-resolution.mjs';
import {OWNER_ALERT_EVENT} from './owner-alert-events.mjs';

const tool = (name,description,properties={},required=[],write=false) => ({name,description,inputSchema:{type:'object',properties,required,additionalProperties:false},annotations:{readOnlyHint:!write,destructiveHint:false,idempotentHint:!['attn_owner_reply_ledger','attn_sales_reply_ledger','attn_owner_ai_pilot_ledger','attn_prospect_v1_ledger'].includes(name),openWorldHint:false}});
export const TOOLS = [
  tool('attn_prospect_v1_ledger','Incoming ATTN prospect replies only. Disabled by default. prepare validates raw official MCP evidence; begin_attempt consumes one durable per-message permit; record_result distinguishes sent from delivered; handoff persists contact hold and returns one final-chat alert. Approved booking/checkout links only, no booking writes or new sequences or backend GHL calls.',{action:{type:'string',enum:['prepare','begin_attempt','record_result','handoff','record_handoff','history_requirements']},event_id:{type:'string'},claim_token:{type:'string'},evidence:{type:'object'}},['action','event_id','evidence'],true),
  tool('attn_owner_ai_pilot_ledger','Distinct owner AI pilot. Disabled by default, fixed owner and iMessage provider, at most ONE ordinary send attempt ever. Raw official MCP reads required. Handoff remains available after cap/window without another send; final chat alert is not a notification/read receipt.',{action:{type:'string',enum:['prepare','begin_attempt','record_result','handoff']},event_id:{type:'string'},claim_token:{type:'string'},evidence:{type:'object'}},['action','event_id','evidence'],true),
  tool('attn_sales_reply_ledger','Review-only sales safety ledger. Runtime remains disabled with empty enrollment; no environment toggle enables customer sending. prepare, begin_attempt, record_result and handoff only manage durable state; GHL actions stay in the official cloud connection.',{action:{type:'string',enum:['prepare','begin_attempt','record_result','handoff']},event_id:{type:'string'},claim_token:{type:'string'},evidence:{type:'object'}},['action','event_id','evidence'],true),
  tool('attn_owner_reply_ledger','Owner-only ONE-attempt reply test, disabled by default. prepare requires a claimed event and actual GHL snapshot; begin_attempt requires another fresh snapshot and consumes a nonrenewable send permit. Never retry a send after an uncertain result. No GHL network calls are made by this tool.',{action:{type:'string',enum:['prepare','begin_attempt','record_result']},event_id:{type:'string'},claim_token:{type:'string'},evidence:{type:'object'}},['action','event_id','evidence'],true),
  tool('attn_listener_status','Read sanitized listener status. HTTP acceptance and chat processing are separate counters.'),
  tool('attn_server_alerts','Owner-private saved alerts. Read returns details and cloud-consumer metadata; acknowledge records cloud processing only, never human read or push delivery. No customer send.',{action:{type:'string',enum:['list','read','acknowledge']},alert_id:{type:'string'},limit:{type:'number'},before:{type:'number'}},['action'],true),
  tool('attn_owner_hold_review','Inspect or resolve one reviewed stale handoff. Requires fresh full scoped evidence and exact state hash; never clears opt-outs, clients, active manual pauses or uncertain sends. Resolution sends nothing and applies only to future inbound.',{action:{type:'string',enum:['inspect','resolve']},contact_id:{type:'string'},snapshot:{type:'object'},expected_state_hash:{type:'string'},reason:{type:'string'},note:{type:'string'}},['action','contact_id','snapshot'],true),
  tool('attn_claim_event','Atomically claim a delivered event before reading or responding. If claimed is false, do not repeat its processing.',{event_id:{type:'string'},fenced:{type:'boolean'},recover:{type:'boolean'}},['event_id'],true),
  tool('attn_record_processed','Record that this chat actually processed an event after reading the relevant GHL conversation. Never use HTTP delivery alone as proof.',{event_id:{type:'string'},claim_token:{type:'string'},stage:{type:'string'},reason:{type:'string'},outcome:{type:'string',enum:['observed','escalated','skipped','test_reply_sent']}},['event_id','outcome'],true),
  tool('attn_processing_trace','Persist a sanitized stage and internal reason for this claimed event. No customer send.',{event_id:{type:'string'},claim_token:{type:'string'},stage:{type:'string'},reason:{type:'string'}},['event_id','claim_token','stage','reason'],true),
  tool('attn_defer_processing','Bounded recovery for an unsent fenced event. Requeues the existing callback; never clears human holds or retries uncertain sends.',{event_id:{type:'string'},claim_token:{type:'string'},reason:{type:'string'},retry_after_ms:{type:'number'}},['event_id','claim_token','reason'],true),
];
function object(value) {return value && typeof value==='object'&&!Array.isArray(value);}
function invokeLedger(bridge,ledger,method,owner,a){
  const claim=bridge.db.prepare('SELECT generation FROM processing WHERE event_id=? AND owner=?').get(a.event_id,owner);
  const run=()=>ledger[method](owner,a.event_id,a.evidence);
  return claim?.generation?bridge.withClaim(owner,a.event_id,a.claim_token,run,{open:a.action!=='record_result',allowExpired:a.action==='record_result'}):run();
}
function prospectCall(bridge,owner,a,method){
  const options={open:!['record_result','record_handoff'].includes(a.action),allowExpired:['record_result','record_handoff'].includes(a.action)};
  // Validate the fence before diagnostics too: a caller cannot append to another
  // run or alter the current worker's trace with a stale token.
  bridge.assertClaim(owner,a.event_id,{...options,claimToken:a.claim_token});
  try{
    const result=bridge.withClaim(owner,a.event_id,a.claim_token,()=>bridge.prospectV1[method](owner,a.event_id,a.evidence),options);
    if(result.prepared===false||result.permitted===false)bridge.recordStage(owner,a.event_id,{claim_token:a.claim_token,stage:'suppressed',reason:result.reason});
    return result;
  }catch(e){
    bridge.recordStage(owner,a.event_id,{claim_token:a.claim_token,stage:'failed',reason:/^[a-z][a-z0-9_]{0,100}$/.test(e.reason??'')?e.reason:'ledger_internal_error'});
    throw e;
  }
}
export async function rpc(bridge,owner,body) {
  if(!object(body)||body.jsonrpc!=='2.0'||typeof body.method!=='string'||(body.id!==undefined&&typeof body.id!=='string'&&typeof body.id!=='number'&&body.id!==null)) return {jsonrpc:'2.0',id:null,error:{code:-32600,message:'Invalid Request'}};
  if(body.id===undefined) return null;
  let result;
  try {
    const p=body.params??{};
    if(!object(p)) throw new RpcError(-32602,'Invalid params','object_required');
    switch(body.method) {
      case 'server/discover':result={resultType:'complete',supportedVersions:['2026-07-28'],capabilities:{tools:{},events:{}}};break;
      case 'initialize':result={protocolVersion:'2026-07-28',capabilities:{tools:{},events:{}},serverInfo:{name:'attn-incoming-bridge',version:'0.1.0'}};break;
      case 'ping': result={};break;
      case 'events/list':result={events:[...bridge.catalog(),...(bridge.runtime?.ownerAlerts?.catalog()??[])]};break;
      case 'events/subscribe':result=p.name===OWNER_ALERT_EVENT&&bridge.runtime?.ownerAlerts?await bridge.runtime.ownerAlerts.subscribe(owner,p):await bridge.subscribe(owner,p);break;
      case 'events/unsubscribe':result=p.name===OWNER_ALERT_EVENT&&bridge.runtime?.ownerAlerts?await bridge.runtime.ownerAlerts.unsubscribe(owner,p):await bridge.unsubscribe(owner,p);break;
      case 'tools/list':result={tools:TOOLS};break;
      case 'tools/call': {
        const t=TOOLS.find(t=>t.name===p.name),a=p.arguments??{};
        if(!t||!object(a)||Object.keys(a).some(k=>!(k in t.inputSchema.properties))||t.inputSchema.required.some(k=>t.inputSchema.properties[k].type==='object'?!object(a[k]):typeof a[k]!=='string')) throw new RpcError(-32602,'Invalid tool arguments','tool_arguments');
        let data;
        if(p.name==='attn_prospect_v1_ledger'){
          if(!bridge.prospectV1.ingress)throw new RpcError(-32602,'Prospect gate blocked','prospect_v1_disabled');
          const methods={prepare:'prepare',begin_attempt:'begin',record_result:'record',handoff:'handoff',record_handoff:'recordHandoff',history_requirements:'historyRequirements'};
          if(!Object.hasOwn(methods,a.action))throw new RpcError(-32602,'Invalid action','prospect_v1_action');
          try{data=prospectCall(bridge,owner,a,methods[a.action]);}catch(e){if(e.reason)throw new RpcError(-32602,'Prospect gate blocked',e.reason);throw e;}
        }
        if(p.name==='attn_owner_ai_pilot_ledger') {
          const methods={prepare:'prepare',begin_attempt:'begin',record_result:'record',handoff:'handoff'};
          if(!Object.hasOwn(methods,a.action)) throw new RpcError(-32602,'Invalid action','owner_ai_action');
          try {data=invokeLedger(bridge,bridge.ownerAiPilot,methods[a.action],owner,a);}
          catch(e) {if(e.reason) throw new RpcError(-32602,'Owner AI gate blocked',e.reason);throw e;}
        }
        if(p.name==='attn_sales_reply_ledger') {
          const methods={prepare:'prepare',begin_attempt:'begin',record_result:'record',handoff:'handoff'};
          if(!Object.hasOwn(methods,a.action)) throw new RpcError(-32602,'Invalid action','sales_reply_action');
          try {data=invokeLedger(bridge,bridge.salesReply,methods[a.action],owner,a);}
          catch(e) {if(e.reason) throw new RpcError(-32602,'Sales reply gate blocked',e.reason);throw e;}
        }
        if(p.name==='attn_owner_reply_ledger') {
          const methods={prepare:'prepare',begin_attempt:'begin',record_result:'record'};
          if(!Object.hasOwn(methods,a.action)) throw new RpcError(-32602,'Invalid action','owner_reply_action');
          try {data=invokeLedger(bridge,bridge.ownerReply,methods[a.action],owner,a);}
          catch(e) {if(e.reason) throw new RpcError(-32602,'Owner reply gate blocked',e.reason);throw e;}
        }
        if(p.name==='attn_listener_status') data={...bridge.status(owner),server_runtime:bridge.runtime?.status??null,worker_live:bridge.runtime?.worker?.status()??null,send_reconciliation:bridge.runtime?.reconciler?.status()??null,owner_alert_events:bridge.runtime?.ownerAlerts?.status(owner)??null,model_budget:bridge.runtime?.budget?.status?.()??null};
        if(p.name==='attn_server_alerts'){
          if(a.action==='list')data=bridge.runtime?.ownerAlerts?bridge.runtime.ownerAlerts.list(owner,{limit:a.limit,before:a.before}):bridge.listServerAlerts(owner,{limit:a.limit,before:a.before});
          else if(a.action==='read')data={...bridge.readServerAlert(owner,a.alert_id),consumer:bridge.runtime?.ownerAlerts?.read(owner,a.alert_id)??null};
          else if(a.action==='acknowledge'&&bridge.runtime?.ownerAlerts)data=bridge.runtime.ownerAlerts.ack(owner,a.alert_id);
          else throw new RpcError(-32602,'Invalid action','server_alert_action');
        }
        if(p.name==='attn_owner_hold_review'){
          const input={contactId:a.contact_id,snapshot:a.snapshot,expectedStateHash:a.expected_state_hash,reason:a.reason,note:a.note};
          if(a.action==='inspect')data=inspectOwnerHold(bridge,owner,input);
          else if(a.action==='resolve')data=resolveOwnerHold(bridge,owner,input);
          else throw new RpcError(-32602,'Invalid action','owner_hold_action');
        }
        if(p.name==='attn_claim_event') data=bridge.claim(owner,a.event_id,{fenced:a.fenced,recover:a.recover});
        if(p.name==='attn_record_processed') data=bridge.processed(owner,a.event_id,a.outcome,{claim_token:a.claim_token,stage:a.stage,reason:a.reason});
        if(p.name==='attn_processing_trace') data=bridge.recordStage(owner,a.event_id,{claim_token:a.claim_token,stage:a.stage,reason:a.reason});
        if(p.name==='attn_defer_processing') data=bridge.deferProcessing(owner,a.event_id,{claim_token:a.claim_token,reason:a.reason,retryAfterMs:a.retry_after_ms});
        result={content:[{type:'text',text:JSON.stringify(data)}],structuredContent:data};break;
      }
      default:throw new RpcError(-32601,'Method not found','method_not_found');
    }
    return {jsonrpc:'2.0',id:body.id,result};
  } catch(e) {
    return {jsonrpc:'2.0',id:body.id,error:e instanceof RpcError?{code:e.code,message:e.message,data:{reason:e.reason}}:{code:-32603,message:'Internal error'}};
  }
}
async function readBody(req) {
  let size=0;const chunks=[];
  for await(const part of req) {size+=part.length;if(size>262144) throw new Error('body_too_large');chunks.push(part);}
  return Buffer.concat(chunks);
}
export function createServer({bridge,proxyToken,workflowToken,publicKey,proxyPrincipal,onAccepted}) {
  if(!proxyToken||proxyToken.length<32||!workflowToken||workflowToken.length<32||equal(proxyToken,workflowToken)) throw new Error('Distinct proxy and webhook secrets of at least 32 characters required');
  if(!proxyPrincipal||proxyPrincipal.length>256) throw new Error('A pinned Sites owner principal is required');
  const buckets=new Map();
  return http.createServer({requestTimeout:15000,headersTimeout:10000,maxHeaderSize:16384},async(req,res)=>{
    const send=(status,data,headers={})=>{res.writeHead(status,{'content-type':'application/json','cache-control':'no-store','x-content-type-options':'nosniff',...headers});res.end(data===null?'':JSON.stringify(data));};
    try {
      const url=new URL(req.url,'http://localhost');
      if(req.method==='GET'&&url.pathname==='/healthz') return send(200,{ok:true});
      if(req.method!=='POST') return send(405,{error:'method_not_allowed'},{allow:'POST'});
      // No browser-facing service API. Authenticated Sites proxy supplies the user identity.
      if(req.headers.origin) return send(403,{error:'origin_not_allowed'});
      const mcp=url.pathname==='/mcp',workflow=url.pathname==='/webhooks/ghl-workflow',loop=url.pathname==='/webhooks/loop-workflow',ghl=url.pathname==='/webhooks/ghl';
      if(!mcp&&!workflow&&!loop&&!ghl) return send(404,{error:'not_found'});
      if(mcp&&(!equal(req.headers.authorization??'','Bearer '+proxyToken)||!equal(req.headers['x-attn-principal']??'',proxyPrincipal))) return send(401,{error:'unauthorized'});
      if((workflow||loop)&&!equal(req.headers['x-attn-webhook-key']??'',workflowToken)) return send(401,{error:'unauthorized'});
      if(!String(req.headers['content-type']??'').toLowerCase().startsWith('application/json')) return send(415,{error:'json_required'});
      const key=hash(req.socket.remoteAddress??'unknown'),now=Date.now();
      const bucket=buckets.get(key)??{count:0,until:now+60000};
      if(bucket.until<=now){bucket.count=0;bucket.until=now+60000;}
      bucket.count++;buckets.set(key,bucket);
      if(buckets.size>2048) for(const [k,v] of buckets) if(v.until<=now)buckets.delete(k);
      if(bucket.count>300||buckets.size>4096) return send(429,{error:'rate_limited'});
      const raw=await readBody(req);
      if(ghl&&!verifyGhl(raw,req.headers['x-ghl-signature'],publicKey)) {bridge.audit('source_signature_rejected');return send(401,{error:'invalid_signature'});}
      let body;try{body=JSON.parse(raw.toString('utf8'));}catch{return send(400,{error:'invalid_json'});}
      if(mcp) {const response=await rpc(bridge,proxyPrincipal,body);return send(response===null?202:200,response);}
      const result=bridge.ingest(body,ghl?'ghl_signed':loop?'loop_workflow':'ghl_workflow');
      send(result.status,result);
      // An event, not an interval, starts first delivery. The persisted outbox timer only retries accepted notifications.
      if(result.accepted){if(onAccepted)onAccepted();else void bridge.drain().catch(()=>bridge.audit('drain_failure'));}
    } catch(e) {
      if(e instanceof RpcError) {bridge.audit('inbound_shape_rejected');return send(422,{error:e.reason});}
      if(e.message==='body_too_large') return send(413,{error:'body_too_large'});
      bridge.audit('request_failure');if(!res.headersSent)send(500,{error:'internal_error'});
    }
  });
}
export function ownerAiOptions(env){
  const enabled=env.OWNER_AI_PILOT_ENABLED==='true';
  if(!enabled)return {enabled:false};
  if(env.OWNER_REPLY_TEST_ENABLED==='true'||!env.OWNER_AI_PILOT_CONFIG_JSON)throw new Error('Owner AI requires separate config and closed legacy test');
  return {enabled:true,config:JSON.parse(env.OWNER_AI_PILOT_CONFIG_JSON)};
}
export function main(env=process.env) {
  const dbPath=env.DB_PATH||'/data/bridge.sqlite';
  fs.mkdirSync(path.dirname(dbPath),{recursive:true,mode:0o700});
  process.umask(0o077);
  const options=serverOptions(env);
  const bridge=new Bridge({dbPath,vaultKey:env.VAULT_KEY,testContactId:env.TEST_CONTACT_ID,enabled:env.LISTENER_ENABLED==='true',ownerAiPilot:ownerAiOptions(env),prospectV1:prospectOptions(env),processingMode:options.processingMode,serverWorkerNotBefore:options.serverWorkerNotBefore,operatorOwner:options.operatorOwner,ownerReply:{enabled:env.OWNER_REPLY_TEST_ENABLED==='true',notBefore:env.OWNER_REPLY_TEST_NOT_BEFORE||''}});
  const runtime=createRuntime({bridge,env,options});bridge.runtime=runtime;
  let stopping=false,boot=Promise.resolve();const drains=new Set();
  const drain=()=>{const p=bridge.drain().catch(()=>bridge.audit('drain_failure'));drains.add(p);p.finally(()=>drains.delete(p));};
  const server=createServer({bridge,proxyToken:env.PROXY_TOKEN,workflowToken:env.WORKFLOW_TOKEN,proxyPrincipal:env.SITE_OWNER_SUBJECT,onAccepted:()=>{if(!stopping)runtime.worker?.wake();drain();}});
  server.listen(Number(env.PORT)||3000,'0.0.0.0',()=>{
    console.log(JSON.stringify({action:'bridge_started',location:LOCATION,mode:options.processingMode,worker_enabled:options.enabled}));
    // Configuration checks have no customer-send capability. A failed probe
    // leaves the worker stopped and its queued events durably inspectable.
    boot=runtime.check().then(result=>{if(stopping)return;if(!result||result.state==='passed'){runtime.worker?.start();runtime.reconciler?.start();}else bridge.audit('server_connection_check_failed');}).catch(()=>bridge.audit('server_connection_check_failed'));
  });
  const retry=setInterval(drain,1000);
  const alertRetry=setInterval(()=>runtime.drainAlerts?.(),1000);
  const cleanup=setInterval(()=>bridge.cleanup(),3600000);
  const stop=async()=>{if(stopping)return;stopping=true;clearInterval(retry);clearInterval(alertRetry);clearInterval(cleanup);await new Promise(resolve=>server.close(resolve));await boot;await runtime.worker?.stop();await runtime.reconciler?.stop();await runtime.waitForAlerts?.();await Promise.allSettled([...drains]);bridge.enabled=false;bridge.close();process.exit(0);};
  process.on('SIGTERM',stop);process.on('SIGINT',stop);
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href) main();


