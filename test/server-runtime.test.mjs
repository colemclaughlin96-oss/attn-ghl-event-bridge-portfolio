import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync,existsSync} from 'node:fs';
import {serverOptions,createRuntime} from '../src/server-runtime.mjs';
import {LOCATION,PROVIDER} from '../src/sales-policy.mjs';
import {Vault} from '../src/security.mjs';

const C='synthetic_owner_contact',V='synthetic_owner_thread',M='synthetic_owner_message';
const GHL='synthetic_ghl_private_token',OPENAI='synthetic_openai_api_key';
const base=()=>({OPENAI_API_KEY:OPENAI,GHL_PRIVATE_INTEGRATION_TOKEN:GHL,OPENAI_MODEL:'gpt-6-luna',TEST_CONTACT_ID:C,SITE_OWNER_SUBJECT:'synthetic_pinned_operator',SERVER_CONNECTION_CHECK:'true',MODEL_MONTHLY_BUDGET_USD:'7'});
function fixture(t,env=base(),{conversationCount=1,emptyHistory=false,detailChange={},ghlFailure=null,modelFailure=null,modelReplies=[],modelKind='normal'}={}){
 const db=new DatabaseSync(':memory:');t.after(()=>db.close());let reservations=0;
 const bridge={db,vault:new Vault(Buffer.alloc(32,84).toString('base64')),operatorOwner:env.SITE_OWNER_SUBJECT,post:async(url,body)=>({status:200,body:JSON.stringify({challenge:JSON.parse(body).challenge})}),now:Date.now,processingMode:env.PROCESSING_MODE||'native',reserveServerJobs:()=>{reservations++;return [];},serverWorkerStatus:()=>({queued:0}),audit:()=>{}};
 const calls=[];
 const message={id:M,locationId:LOCATION,contactId:C,conversationId:V,conversationProviderId:PROVIDER,messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound',body:'Synthetic private customer text, never used by model connection check.',dateAdded:new Date().toISOString()};
 const fetchImpl=async(url,init)=>{
  calls.push({url,init});const u=new URL(url);let data;
  if(u.origin==='https://api.openai.com'){
   assert.equal(u.pathname,'/v1/responses');assert.equal(init.method,'POST');assert.equal(init.headers.Authorization,'Bearer '+OPENAI);
   if(modelFailure)return new Response(modelFailure.body,{status:modelFailure.status});
   const request=JSON.parse(init.body),context=JSON.parse(request.input.find(i=>i.role==='user').content);
   assert.equal(request.store,false);assert.equal(request.model,'gpt-6-luna');assert.equal(request.reasoning.effort,'none');assert.ok(!init.body.includes(message.body));assert.ok(!init.body.includes(C));
   if(modelReplies.length){const reply=modelReplies.shift();if(reply)return typeof reply==='function'?reply({request,context}):reply;}
   data={status:'completed',output:[{type:'message',status:'completed',content:[{type:'output_text',text:JSON.stringify({assessments:Object.fromEntries(context.messages.filter(m=>m.direction==='inbound').map(m=>[m.id,modelKind]))})}]}],usage:{input_tokens:100,output_tokens:20,total_tokens:120}};
  }else{
   assert.equal(u.origin,'https://services.leadconnectorhq.com');assert.equal(init.method,'GET');assert.equal(init.headers.Authorization,'Bearer '+GHL);assert.equal(init.headers.Version,'2021-07-28');
   if(ghlFailure&&u.pathname==='/contacts/'+C)return new Response(ghlFailure.body,{status:ghlFailure.status});
   if(u.pathname==='/contacts/'+C)data={contact:{id:C,locationId:LOCATION,dnd:false,type:'lead',tags:[]}};
   else if(u.pathname==='/conversations/search')data={total:conversationCount,conversations:Array.from({length:conversationCount},(_,i)=>({id:i?V+'_'+i:V,locationId:LOCATION,contactId:C}))};
   else if(u.pathname==='/conversations/'+V+'/messages')data={messages:{messages:emptyHistory?[]:[message],nextPage:false,lastMessageId:emptyHistory?null:M}};
   else if(u.pathname==='/conversations/messages/'+M)data={...message,...detailChange};
   else if(u.pathname==='/contacts/'+C+'/appointments')data={events:[]};
   else if(u.pathname==='/opportunities/search')data={opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}};
   else if(u.pathname==='/payments/transactions')data={data:[],totalCount:0};
   else throw new Error('unexpected synthetic endpoint');
  }
  return new Response(JSON.stringify(data),{status:200});
 };
 const options=serverOptions(env),runtime=createRuntime({bridge,env,options,fetchImpl});
 return {runtime,options,calls,bridge,get reservations(){return reservations;}};
}

test('server options default to inactive native runtime and zero-contact canary',async()=>{
 const options=serverOptions({});assert.equal(options.processingMode,'native');assert.equal(options.enabled,false);assert.equal(options.check,false);assert.deepEqual(options.allowedContacts,[]);
 let calls=0;const runtime=createRuntime({bridge:{},env:{},options,fetchImpl:()=>{calls++;throw new Error('must not call');}});
 assert.equal(runtime.worker,null);assert.equal(runtime.status.configured,false);assert.equal(await runtime.check(),null);assert.equal(calls,0);
});

test('worker requires explicit server mode, credentials, configured model and reply flag',()=>{
 assert.throws(()=>serverOptions({SERVER_WORKER_ENABLED:'true'}),/server_worker_mode_required/);
 assert.throws(()=>serverOptions({PROCESSING_MODE:'invalid'}),/invalid_processing_mode/);
 for(const missing of ['OPENAI_API_KEY','GHL_PRIVATE_INTEGRATION_TOKEN','OPENAI_MODEL']){
  const env={...base(),PROCESSING_MODE:'server',SERVER_WORKER_ENABLED:'true',PROSPECT_V1_REPLIES_ENABLED:'true'};delete env[missing];assert.throws(()=>serverOptions(env),/server_api_configuration_required/);
 }
 assert.throws(()=>serverOptions({...base(),PROCESSING_MODE:'server',SERVER_WORKER_ENABLED:'true'}),/server_worker_requires_reply_flag/);
 assert.throws(()=>serverOptions({...base(),OPENAI_MODEL:'unreviewed_model'}),/server_api_configuration_required/);
 assert.throws(()=>serverOptions({...base(),GHL_MESSAGE_LOOKUP:'guess_by_text'}),/invalid_message_lookup/);
});

test('explicit canary/wildcard and resource bounds are preserved',()=>{
 assert.deepEqual(serverOptions({...base(),SERVER_ALLOWED_CONTACTS:C+', synthetic_other '}).allowedContacts,[C,'synthetic_other']);
 assert.equal(serverOptions({...base(),SERVER_ALLOWED_CONTACTS:'*'}).allowedContacts,null);
 assert.deepEqual(serverOptions({...base(),SERVER_ALLOWED_CONTACTS:''}).allowedContacts,[]);
 for(const change of [{SERVER_ALLOWED_CONTACTS:'../bad'},{SERVER_WORKER_CONCURRENCY:'0'},{SERVER_WORKER_CONCURRENCY:'5'},{MODEL_MONTHLY_BUDGET_USD:'0'},{MODEL_MONTHLY_BUDGET_USD:'101'}])assert.throws(()=>serverOptions({...base(),...change}));
});

test('connection check completes scoped GHL reads plus synthetic Responses without GHL writes or starting worker',async t=>{
 const env={...base(),PROCESSING_MODE:'server',SERVER_WORKER_ENABLED:'true',PROSPECT_V1_REPLIES_ENABLED:'true',SERVER_ALLOWED_CONTACTS:C};
 const f=fixture(t,env);assert.equal(f.runtime.status.configured,true);assert.equal(f.runtime.worker.running,false);assert.equal(f.calls.length,0);assert.equal(f.runtime.budget.status().monthlyLimitUsd,7);
 const check=await f.runtime.check();assert.equal(check.state,'passed');assert.equal(f.runtime.worker.running,false);assert.equal(f.reservations,0);
 const paths=f.calls.filter(c=>new URL(c.url).hostname==='services.leadconnectorhq.com').map(c=>new URL(c.url).pathname);
 for(const p of ['/contacts/'+C,'/conversations/search','/conversations/'+V+'/messages','/conversations/messages/'+M,'/contacts/'+C+'/appointments','/opportunities/search','/payments/transactions'])assert.ok(paths.includes(p),p);
 assert.equal(check.results.messages.scope_complete,true);assert.equal(check.results.message_detail.identity_matches,true);
 assert.equal(f.calls.filter(c=>c.init.method==='POST').length,1);assert.equal(f.runtime.budget.status().requests,1);assert.equal(f.runtime.budget.status().unresolvedUsage,0);
 assert.equal(JSON.stringify(check).includes(GHL),false);assert.equal(JSON.stringify(check).includes(OPENAI),false);assert.equal(JSON.stringify(check).includes('Synthetic private customer text'),false);
 const runner=f.runtime.worker.runnerFactory({bridge:f.bridge,owner:'synthetic',adapters:{snapshot(){},generate(){},send(){},verify(){}}});assert.equal(runner.systemPrompt,readFileSync(new URL('../docs/SERVER-BOT-PROMPT.txt',import.meta.url),'utf8'));
 f.runtime.worker.start({poll:false});assert.equal(f.reservations,1);await f.runtime.worker.stop();assert.equal(f.runtime.worker.running,false);
});

test('read-only connection check can run with worker disabled and reports sanitized provider failures',async t=>{
 for(const failure of ['ghl','model']){
  const opts=failure==='ghl'?{ghlFailure:{status:403,body:'sensitive '+GHL}}:{modelFailure:{status:401,body:'sensitive '+OPENAI}},f=fixture(t,base(),opts);
  assert.equal(f.runtime.worker,null);const result=await f.runtime.check();assert.equal(result.state,'failed');assert.equal(JSON.stringify(result).includes('sensitive'),false);assert.equal(JSON.stringify(result).includes(GHL),false);assert.equal(JSON.stringify(result).includes(OPENAI),false);assert.ok(f.calls.filter(c=>new URL(c.url).hostname==='services.leadconnectorhq.com').every(c=>c.init.method==='GET'));
 }
});

test('startup repeats only one malformed response or assessment and records the recovered reason',async t=>{
 const invalidAssessment={status:'completed',output:[{type:'message',status:'completed',content:[{type:'output_text',text:JSON.stringify({assessments:{unknown_id:'normal'}})}]}],usage:{input_tokens:100,output_tokens:20,total_tokens:120}};
 for(const [reason,response] of [['model_response_invalid',new Response('private invalid response '+OPENAI)],['assessment_schema_invalid',new Response(JSON.stringify(invalidAssessment))]]){
  const f=fixture(t,base(),{modelReplies:[response,null]}),result=await f.runtime.check();
  assert.equal(result.state,'passed');assert.deepEqual(result.results.model,{success:true,model:'gpt-6-luna',attempts:2,prior_failures:[reason]});assert.equal(f.runtime.budget.status().requests,2);assert.equal(f.reservations,0);
  assert.equal(f.calls.filter(c=>new URL(c.url).hostname==='api.openai.com').length,2);assert.ok(!JSON.stringify(result).includes(OPENAI));assert.ok(f.calls.filter(c=>new URL(c.url).hostname==='services.leadconnectorhq.com').every(c=>c.init.method==='GET'));
 }
});

test('two malformed startup responses remain failed without starting the enabled worker',async t=>{
 const env={...base(),PROCESSING_MODE:'server',SERVER_WORKER_ENABLED:'true',PROSPECT_V1_REPLIES_ENABLED:'true',SERVER_ALLOWED_CONTACTS:C};
 const f=fixture(t,env,{modelFailure:{status:200,body:'private malformed '+OPENAI}}),result=await f.runtime.check();
 assert.equal(result.state,'failed');assert.deepEqual(result.results.model,{success:false,model:'gpt-6-luna',reason:'model_response_invalid',attempts:2,failures:['model_response_invalid','model_response_invalid']});assert.equal(f.runtime.worker.running,false);assert.equal(f.reservations,0);assert.equal(f.runtime.budget.status().requests,2);assert.equal(JSON.stringify(result).includes(OPENAI),false);
});

test('startup authentication, rate/quota and rejected requests get no automatic retry',async t=>{
 for(const [status,reason] of [[401,'model_authentication_failed'],[403,'model_authentication_failed'],[429,'provider_rate_limited'],[400,'model_request_rejected'],[503,'model_provider_unavailable']]){
  const f=fixture(t,base(),{modelFailure:{status,body:'private provider error '+OPENAI}}),result=await f.runtime.check();
  assert.equal(result.state,'failed');assert.equal(result.results.model.reason,reason);assert.equal(result.results.model.attempts,1);assert.equal(f.calls.filter(c=>new URL(c.url).hostname==='api.openai.com').length,1);assert.equal(JSON.stringify(result).includes(OPENAI),false);
 }
});

test('unexpected startup classification has a fixed safe reason and does not retry or pass',async t=>{
 for(const kind of ['opt_out','system','handoff','unsafe']){
  const f=fixture(t,base(),{modelKind:kind}),result=await f.runtime.check();assert.equal(result.state,'failed');assert.deepEqual(result.results.model,{success:false,model:'gpt-6-luna',attempts:1,reason:'model_probe_classification_unexpected',observed_kind:kind});assert.equal(f.calls.filter(c=>new URL(c.url).hostname==='api.openai.com').length,1);
 }
});

test('explicit history lookup boot probe verifies the exact message and reports its actual transport',async t=>{
 const f=fixture(t,{...base(),GHL_MESSAGE_LOOKUP:'conversation_history'}),checked=await f.runtime.check();
 assert.equal(checked.state,'passed');assert.equal(checked.results.message_detail.identity_matches,true);
 assert.deepEqual(checked.results.message_detail.source,{transport:'conversation_history',operationId:'get-messages',method:'GET',path:'/conversations/{conversationId}/messages',scopeVerifiedBy:'search-conversation',matchedBy:'exact_message_id',pages:1});
 assert.equal(f.calls.some(c=>new URL(c.url).pathname==='/conversations/messages/'+M),false);
 assert.equal(f.calls.filter(c=>new URL(c.url).pathname==='/conversations/'+V+'/messages').length,2);
});

test('boot probe cannot pass if owner conversation is missing/ambiguous or has no message for exact-read verification',async t=>{
 for(const scenario of [{conversationCount:0},{conversationCount:2},{emptyHistory:true}]){
  const f=fixture(t,base(),scenario),result=await f.runtime.check();assert.equal(result.state,'failed');
  if(scenario.emptyHistory)assert.equal(result.results.message_detail.reason,'no_owner_message_for_schema_probe');
  else assert.equal(result.results.messages.reason,'unique_owner_conversation_required');
  assert.equal(f.calls.some(c=>new URL(c.url).pathname==='/conversations/messages/'+M),false);
 }
});

test('boot probe fails when exact message content or scope differs from the history read',async t=>{
 for(const detailChange of [{body:'Unexpected changed message'},{direction:'outbound'},{contactId:'other_contact'}]){
  const f=fixture(t,base(),{detailChange}),result=await f.runtime.check();assert.equal(result.state,'failed');
  assert.ok(result.results.message_detail?.identity_matches===false||result.results.conversations?.success===false);
 }
});

test('deployment image includes both prompts required by runtime imports and explicit worker prompt',()=>{
 const docker=readFileSync(new URL('../Dockerfile',import.meta.url),'utf8');
 for(const name of ['INBOUND-RELIABILITY-PROMPT.txt','SERVER-BOT-PROMPT.txt']){assert.ok(existsSync(new URL('../docs/'+name,import.meta.url)));assert.ok(readFileSync(new URL('../docs/'+name,import.meta.url),'utf8').trim().length>100);assert.ok(docker.split(/\r?\n/).some(line=>/^COPY\s/.test(line)&&line.includes('docs/'+name)&&line.includes('./docs/')));}
 assert.match(docker,/COPY[^\n]*src \.\/src/);
});

