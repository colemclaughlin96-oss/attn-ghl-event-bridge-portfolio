import test from 'node:test';
import assert from 'node:assert/strict';
import {inspect} from 'node:util';
import {GhlAdapter,GHL_ORIGIN,GHL_VERSION,GHL_REQUIRED_SCOPES} from '../src/ghl-adapter.mjs';
import {LOCATION,PROVIDER} from '../src/sales-policy.mjs';
import {collectEvidence} from '../src/inbound-runner.mjs';
import {Bridge,SERVER_WORKER_OWNER} from '../src/bridge.mjs';
import {BOOKING_LINK,CALENDAR,priceAnswer} from '../src/sales-policy.mjs';

// Synthetic only: no credentials or customer/provider network requests.
const TOKEN='synthetic_private_integration_token',C='synthetic_contact',V='synthetic_conversation',M='synthetic_inbound',O='synthetic_outbound';
const now=Date.parse('2026-10-22T17:00:00.000Z');
const message=(changes={})=>({id:M,locationId:LOCATION,contactId:C,conversationId:V,conversationProviderId:PROVIDER,messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound',dateAdded:new Date(now-1000).toISOString(),body:'How does it work?',status:'delivered',...changes});
const permit=(changes={})=>({permitted:true,attempt_id:'attempt_1',message_id:M,expires_at:new Date(now+15000).toISOString(),operationId:'send-a-new-message',locationId:LOCATION,idempotencyKey:'attempt_1',params:{body:{type:'Custom',contactId:C,conversationProviderId:PROVIDER,message:'We manage Google and Facebook ads.',replyMessageId:M}},...changes});
const event={data:{location_id:LOCATION,contact_id:C,conversation_id:V}};
const receipt={messageId:O,conversationId:V};
const query=(operationId,params)=>({operationId,locationId:LOCATION,params});
const contactRequest=query('get-contact',{path:{contactId:C}});
const searchRequest=query('search-conversation',{query:{contactId:C,limit:2}});
const messagesRequest=query('get-messages',{path:{conversationId:V},query:{limit:50}});
const detailRequest=query('get-message',{path:{id:M}});
function mock(responses=[],opts={}){
 const calls=[];
 const fetchImpl=async(url,init)=>{
  calls.push({url,init});const item=responses.shift();
  if(item instanceof Error)throw item;
  if(typeof item==='function')return item(url,init);
  if(item instanceof Response)return item;
  if(item===undefined)throw new Error('unexpected synthetic request');
  return new Response(JSON.stringify(item),{status:200,headers:{'Content-Type':'application/json'}});
 };
 return {calls,adapter:new GhlAdapter({token:TOKEN,fetchImpl,now:()=>now,...opts})};
}
const rejects=(promise,reason)=>assert.rejects(promise,e=>e.reason===reason);
const conversation=()=>({conversations:[{id:V,contactId:C,locationId:LOCATION}],total:1});
const page=()=>({messages:{messages:[message()],nextPage:false,lastMessageId:M}});
async function primed(rest=[],opts={}){const x=mock([message(),...rest],opts);await x.adapter.execute(detailRequest);return x;}
const outbound=(changes={})=>message({id:O,direction:'outbound',dateAdded:new Date(now).toISOString(),body:permit().params.body.message,status:'sent',...changes});

test('requires explicitly supplied server token, hides token in serialization and inspection',()=>{
 for(const token of [undefined,'','short','with spaces in synthetic token'])assert.throws(()=>new GhlAdapter({token}),/ghl_private_integration_token_required/);
 const {adapter}=mock();assert.ok(!JSON.stringify(adapter).includes(TOKEN));assert.ok(!inspect(adapter).includes(TOKEN));
 assert.deepEqual(GHL_REQUIRED_SCOPES,['contacts.readonly','conversations.readonly','conversations/message.readonly','conversations/message.write','opportunities.readonly','payments/transactions.readonly']);
});

test('all evidence operation HTTP contracts, authorization/version, raw envelope mappings',async()=>{
 const contact={id:C,locationId:LOCATION,dnd:false,type:'lead',timezone:'America/Los_Angeles'};
 const appointments={events:[{id:'appointment_1',contactId:C,locationId:LOCATION,calendarId:'calendar_1',appointmentStatus:'confirmed'}]};
 const opportunities={opportunities:[{id:'opportunity_1',contactId:C,locationId:LOCATION,status:'open'}],meta:{total:1,nextPageUrl:null,nextPage:''}};
 const transactions={data:[{_id:'transaction_1',altId:LOCATION,altType:'location',contactId:C,status:'pending',liveMode:'false'}],totalCount:1};
 const {adapter,calls}=mock([conversation(),{contact},page(),message(),appointments,opportunities,transactions]);
 const requests=[searchRequest,contactRequest,messagesRequest,detailRequest,query('get-appointments-for-contact',{path:{contactId:C}}),query('search-opportunity',{query:{contactId:C,status:'all',limit:100}}),query('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:C,limit:100,offset:0}})];
 const results=[];for(const r of requests)results.push(await adapter.execute(r));
 const expected=[['/conversations/search',{locationId:LOCATION,contactId:C,limit:'2'}],['/contacts/'+C,{}],['/conversations/'+V+'/messages',{limit:'50'}],['/conversations/messages/'+M,{}],['/contacts/'+C+'/appointments',{}],['/opportunities/search',{location_id:LOCATION,contact_id:C,status:'all',limit:'100'}],['/payments/transactions',{altType:'location',locationId:LOCATION,contactId:C,limit:'100',offset:'0',altId:LOCATION}]];
 calls.forEach((c,i)=>{const u=new URL(c.url);assert.equal(u.origin,GHL_ORIGIN);assert.equal(u.pathname,expected[i][0]);assert.deepEqual(Object.fromEntries(u.searchParams),expected[i][1]);assert.equal(c.init.method,'GET');assert.equal(c.init.headers.Authorization,'Bearer '+TOKEN);assert.equal(c.init.headers.Version,GHL_VERSION);assert.equal(c.init.redirect,'error');assert.equal(c.init.body,undefined);assert.ok(c.init.signal instanceof AbortSignal);assert.deepEqual(Object.keys(results[i]).sort(),['data','operationId','status','success']);assert.equal(results[i].operationId,requests[i].operationId);assert.equal(results[i].status,200);assert.equal(results[i].success,true);});
 assert.deepEqual(results[1].data,{contact});assert.deepEqual(results[3].data,{message:message()});assert.deepEqual(results[4].data,appointments);assert.deepEqual(results[5].data,opportunities);assert.deepEqual(results[6].data,transactions);
});

test('pagination preserves provider cursor and does not fabricate completeness',async()=>{
 const {adapter,calls}=mock([conversation(),{messages:{messages:[message()],nextPage:true,lastMessageId:M}}, {messages:{messages:[],nextPage:false}}]);
 await adapter.execute(searchRequest);const first=await adapter.execute(messagesRequest);assert.equal(first.data.messages.nextPage,true);
 const last=await adapter.execute(query('get-messages',{path:{conversationId:V},query:{limit:50,lastMessageId:M}}));
 assert.equal(new URL(calls[2].url).searchParams.get('lastMessageId'),M);assert.equal(Object.hasOwn(last.data.messages,'lastMessageId'),false);
});

test('matches collectEvidence raw MCP contract end to end without fabricating missing contact facts',async()=>{
 const route=async(url)=>{const p=new URL(url).pathname;let data;
  if(p==='/contacts/'+C)data={contact:{id:C,locationId:LOCATION}};
  else if(p==='/conversations/search')data=conversation();
  else if(p==='/conversations/'+V+'/messages')data=page();
  else if(p==='/conversations/messages/'+M)data=message();
  else if(p==='/contacts/'+C+'/appointments')data={events:[]};
  else if(p==='/opportunities/search')data={opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}};
  else if(p==='/payments/transactions')data={data:[],totalCount:0};
  else throw new Error('unrecognized path');return new Response(JSON.stringify(data));};
 const adapter=new GhlAdapter({token:TOKEN,fetchImpl:route,now:()=>now});
 const evidence=await collectEvidence({execute:adapter.execute,contactId:C,candidateId:M,now:()=>now,assess:async({messages})=>messages.map(m=>({messageId:m.id,kind:'normal'}))});
 assert.equal(evidence.reads.length,7);const c=evidence.reads.find(r=>r.operationId==='get-contact').result.data.contact;
 assert.equal(Object.hasOwn(c,'dnd'),false);assert.equal(Object.hasOwn(c,'timezone'),false);assert.equal(Object.hasOwn(c,'type'),false);
 assert.deepEqual(evidence.reads.find(r=>r.operationId==='get-message').result.data.message,message());
});

test('rejects unauthorized operation, location, extra fields and unverified conversation before I/O',async()=>{
 const {adapter,calls}=mock();
 for(const r of [{...contactRequest,locationId:'other_location'},query('send-a-new-message',{body:{}}),query('get-contact',{path:{contactId:C},query:{locationId:LOCATION}}),query('get-contact',{path:{contactId:'../escape'}}),query('search-conversation',{query:{contactId:C,limit:100}}),query('list-transactions',{query:{altType:'location',locationId:'other',contactId:C,limit:100,offset:0}}),messagesRequest])await assert.rejects(adapter.execute(r));
 assert.equal(calls.length,0);
});

test('observed nonempty contact appointments retain explicit scope/status and allow answers without offering a second booking',async t=>{
 // Sanitized shape from the read-only 2026-10-21 official MCP probe: the contact
 // endpoint carries scope and appointmentStatus, but its times lack an offset.
 const appointments=['confirmed','cancelled'].map((appointmentStatus,i)=>({id:'appointment_'+i,contactId:C,locationId:LOCATION,calendarId:CALENDAR,appointmentStatus,appoinmentStatus:appointmentStatus,startTime:'2026-10-23 09:30:00',endTime:'2026-10-23 10:00:00',deleted:false}));
 const route=async url=>{const p=new URL(url).pathname;let data;
  if(p==='/contacts/'+C)data={contact:{id:C,locationId:LOCATION,type:'lead',dnd:false,tags:[]}};
  else if(p==='/conversations/search')data=conversation();
  else if(p==='/conversations/'+V+'/messages')data=page();
  else if(p==='/conversations/messages/'+M)data=message();
  else if(p==='/contacts/'+C+'/appointments')data={events:appointments,traceId:'synthetic_trace'};
  else if(p==='/opportunities/search')data={opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}};
  else if(p==='/payments/transactions')data={data:[],totalCount:0};
  else throw new Error('unrecognized synthetic route');return new Response(JSON.stringify(data));};
 const adapter=new GhlAdapter({token:TOKEN,fetchImpl:route,now:()=>now}),cut=new Date(now-2000).toISOString();
 const b=new Bridge({dbPath:':memory:',vaultKey:Buffer.alloc(32,71).toString('base64'),enabled:true,now:()=>now,processingMode:'server',serverWorkerNotBefore:cut,operatorOwner:'synthetic_operator',prospectV1:{ingress:true,enabled:true,notBefore:cut}});t.after(()=>b.close());
 const e=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:C,direction:'inbound',loopMessageId:'synthetic_appointment_loop'},'loop_workflow'),claim=b.claim(SERVER_WORKER_OWNER,e.eventId,{fenced:true});
 const evidence=await collectEvidence({execute:adapter.execute,contactId:C,candidateId:M,now:()=>now,assess:async({messages})=>messages.map(m=>({messageId:m.id,kind:'normal'}))});
 assert.deepEqual(evidence.reads.find(r=>r.operationId==='get-appointments-for-contact').result.data.events,appointments);
 const call=fn=>b.withClaim(SERVER_WORKER_OWNER,e.eventId,claim.claim_token,fn),initial={...evidence,reads:evidence.reads.filter(r=>r.operationId!=='get-message')},prep=call(()=>b.prospectV1.prepare(SERVER_WORKER_OWNER,e.eventId,initial));assert.equal(prep.prepared,true);
 const draft={text:BOOKING_LINK,intent:'booking_link',answersMessageId:M,answersMessageIds:prep.message_ids,policyReviewed:true,untrustedContentIgnored:true};
 const begin=draft=>call(()=>b.prospectV1.begin(SERVER_WORKER_OWNER,e.eventId,{snapshot:evidence,draft,message_id:M,thread_version:prep.thread_version}));
 assert.throws(()=>begin(draft),e=>e.reason==='already_booked');
 assert.equal(begin({...draft,text:priceAnswer('google'),intent:'pricing',priceKey:'google'}).permitted,true);
});

test('rejects incomplete and cross-contact/location evidence without adding missing scope',async()=>{
 const cases=[
  [searchRequest,{conversations:[{id:V,contactId:'other',locationId:LOCATION}],total:1}],
  [searchRequest,{conversations:[],total:'0'}],
  [contactRequest,{contact:{id:C}}],
  [contactRequest,{contact:{id:'other',locationId:LOCATION}}],
  [detailRequest,message({locationId:'other'})],
  [detailRequest,message({id:'other'})],
  [detailRequest,{message:message()}],
  [query('get-appointments-for-contact',{path:{contactId:C}}),{events:[{id:'a',calendarId:'calendar_1',status:'booked'}]}],
  [query('search-opportunity',{query:{contactId:C,status:'all',limit:100}}),{opportunities:[{id:'o',contactId:'other',locationId:LOCATION}],meta:{total:1}}],
  [query('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:C,limit:100,offset:0}}),{data:[{_id:'t',altId:'other',altType:'location',contactId:C}],totalCount:1}],
  [query('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:C,limit:100,offset:0}}),{data:[]}]
 ];
 for(const [r,d] of cases){const {adapter}=mock([d]);await assert.rejects(adapter.execute(r));}
});

test('known conversation rejects a cross-contact message or changed message identity',async()=>{
 const {adapter}=mock([conversation(),{messages:{messages:[message({contactId:'other'})],nextPage:false,lastMessageId:M}},message(),message({contactId:'other',conversationId:'other_conversation'})]);
 await adapter.execute(searchRequest);await rejects(adapter.execute(messagesRequest),'ghl_message_scope');await adapter.execute(detailRequest);await rejects(adapter.execute(detailRequest),'ghl_message_scope');
});

test('missing page flag/cursor and more than requested page size fail closed',async()=>{
 for(const p of [{messages:[]},{messages:[],nextPage:true},{messages:[],nextPage:false,lastMessageId:'../bad'},{messages:Array.from({length:51},()=>message()),nextPage:false,lastMessageId:M}]){
  const {adapter}=mock([conversation(),{messages:p}]);await adapter.execute(searchRequest);await rejects(adapter.execute(messagesRequest),'ghl_response_contract');
 }
});

test('redacts HTTP, network, redirect and JSON errors; does not parse rejected response body',async()=>{
 for(const status of [301,401,403,404,500]){
  let parsed=false;const {adapter}=mock([()=>({status,headers:new Headers({'x-private':TOKEN}),json:async()=>{parsed=true;return {error:TOKEN};}})]);
  await assert.rejects(adapter.execute(contactRequest),e=>{assert.ok(!String(e.stack).includes(TOKEN));assert.ok(!JSON.stringify(e).includes(TOKEN));assert.equal(e.status,status);return true;});assert.equal(parsed,false);
 }
 const {adapter}=mock([new Error('network leaked '+TOKEN)]);await assert.rejects(adapter.execute(contactRequest),e=>e.reason==='ghl_network_failure'&&!String(e.stack).includes(TOKEN));
 const malformed=mock([new Response('not json '+TOKEN)]);await rejects(malformed.adapter.execute(contactRequest),'ghl_invalid_json');
});

test('429 stops subsequent requests during provider cooldown with no retry',async()=>{
 let time=now;const {adapter,calls}=mock([new Response('sensitive '+TOKEN,{status:429,headers:{'Retry-After':'2'}}),{contact:{id:C,locationId:LOCATION}}],{now:()=>time});
 await rejects(adapter.execute(contactRequest),'provider_rate_limited');await rejects(adapter.execute(contactRequest),'provider_rate_limited');assert.equal(calls.length,1);
 time+=2001;await adapter.execute(contactRequest);assert.equal(calls.length,2);
});

test('bounded read timeout and pre-aborted request never automatically retry',async()=>{
 const {adapter,calls}=mock([()=>new Promise(()=>{})],{timeoutMs:10});await rejects(adapter.execute(contactRequest),'ghl_request_timeout');assert.equal(calls.length,1);
 const aborted=new AbortController();aborted.abort();await rejects(adapter.execute({...contactRequest,signal:aborted.signal}),'ghl_request_aborted');assert.equal(calls.length,1);
});

test('send uses exact approved Loop body once, with no undocumented idempotency header',async()=>{
 const {adapter,calls}=await primed([new Response(JSON.stringify(receipt),{status:201})]);const p=permit();assert.deepEqual(await adapter.send(p),receipt);
 const post=calls[1];assert.equal(post.url,GHL_ORIGIN+'/conversations/messages');assert.equal(post.init.method,'POST');assert.deepEqual(JSON.parse(post.init.body),p.params.body);assert.equal(post.init.headers['Content-Type'],'application/json');assert.equal(Object.keys(post.init.headers).some(k=>/idempotency/i.test(k)),false);
 await rejects(adapter.send(p),'ghl_send_attempt_already_used');assert.equal(calls.filter(c=>c.init.method==='POST').length,1);
});

test('send rejects wrong permit, provider, recipient, reply, extra channel fields and expiry without POST',async()=>{
 const {adapter,calls}=await primed();const good=permit(),b=good.params.body;
 const bad=[{...good,permitted:false},{...good,locationId:'other'},{...good,idempotencyKey:'other'},{...good,expires_at:new Date(now).toISOString()},{...good,expires_at:new Date(now+15001).toISOString()},...[
  {type:'SMS'},{contactId:'other'},{conversationProviderId:'other'},{replyMessageId:'other'},{message:''},{attachments:['https://example.com/file']},{email:'other@example.com'}
 ].map(change=>({...good,params:{body:{...b,...change}}}))];
 for(const p of bad)await assert.rejects(adapter.send(p));assert.equal(calls.length,1);
 await rejects(mock().adapter.send(good),'ghl_send_reply_scope_unverified');
});

test('outbound or other provider inbound cannot establish send authorization',async()=>{
 for(const m of [message({direction:'outbound'}),message({conversationProviderId:'other'}),message({messageType:'TYPE_SMS'}),message({conversationProviderId:undefined})]){
  const {adapter,calls}=mock([m]);await adapter.execute(detailRequest);await rejects(adapter.send(permit()),'ghl_send_reply_scope_unverified');assert.equal(calls.length,1);
 }
});

test('send timeout remains consumed and never retries or falls back to another channel',async()=>{
 const {adapter,calls}=await primed([()=>new Promise(()=>{})],{timeoutMs:10});await rejects(adapter.send(permit()),'ghl_send_unknown');await rejects(adapter.send(permit()),'ghl_send_attempt_already_used');assert.equal(calls.length,2);
});

test('send network error, 429 and incomplete/wrong conversation receipt remain consumed',async()=>{
 for(const response of [new Error(TOKEN),new Response(TOKEN,{status:429}),{}, {messageId:O,conversationId:'other'}, {messageId:M,conversationId:V}]){
  const {adapter,calls}=await primed([response]);await assert.rejects(adapter.send(permit()),e=>!String(e.stack).includes(TOKEN));await rejects(adapter.send(permit()),'ghl_send_attempt_already_used');assert.equal(calls.length,2);
 }
});

test('verify exact outbound ID maps status without claiming accepted/unknown as delivered',async()=>{
 for(const [status,outcome] of [['pending','queued'],['queued','queued'],['sent','sent'],['delivered','delivered'],['read','delivered'],['failed','failed'],['undelivered','unknown'],['scheduled','unknown'],['__proto__','unknown'],[undefined,'unknown']]){
  const m=outbound({status});const {adapter,calls}=mock([m]);const result=await adapter.verify(receipt,{permit:permit(),event});assert.equal(result.outcome,outcome);assert.deepEqual(result.outbound,JSON.parse(JSON.stringify(m)));assert.deepEqual(result.send_response,receipt);assert.equal(calls[0].url,GHL_ORIGIN+'/conversations/messages/'+O);assert.equal(calls[0].init.method,'GET');
 }
});

test('verify can reconcile known receipt after permit expiry without re-sending',async()=>{
 const {adapter,calls}=mock([outbound()],{now:()=>now+3600000});assert.equal((await adapter.verify(receipt,{permit:permit(),event})).outcome,'sent');assert.equal(calls.length,1);assert.equal(calls[0].init.method,'GET');
});

test('verify refuses scope/body/provider/direction mismatch or missing evidence',async()=>{
 for(const change of [{id:'other'},{contactId:'other'},{locationId:'other'},{conversationId:'other'},{conversationProviderId:'other'},{messageType:'TYPE_SMS'},{direction:'inbound'},{body:'Different text'},{conversationProviderId:undefined}]){
  const {adapter}=mock([outbound(change)]);await assert.rejects(adapter.verify(receipt,{permit:permit(),event}));
 }
 for(const change of [{location_id:'other'},{contact_id:'other'},{conversation_id:'other'}]){
  const {adapter,calls}=mock();await rejects(adapter.verify(receipt,{permit:permit(),event:{data:{...event.data,...change}}}),'ghl_verification_scope');assert.equal(calls.length,0);
 }
});

test('verification read failure never posts again',async()=>{
 const {adapter,calls}=await primed([receipt,new Response(TOKEN,{status:503})]);const sent=await adapter.send(permit());await rejects(adapter.verify(sent,{permit:permit(),event}),'ghl_http_error');assert.deepEqual(calls.map(c=>c.init.method),['GET','POST','GET']);await rejects(adapter.send(permit()),'ghl_send_attempt_already_used');
});

test('401 never implicitly changes the direct message transport or spoofs source/channel headers',async()=>{
 const {adapter,calls}=mock([new Response('Token source/channel mismatch with request source/channel',{status:401}),conversation(),page()]);
 await rejects(adapter.execute(detailRequest),'ghl_authorization_failed');assert.equal(calls.length,1);assert.equal(new URL(calls[0].url).pathname,'/conversations/messages/'+M);assert.deepEqual(Object.keys(calls[0].init.headers).sort(),['Accept','Authorization','Version']);
 assert.throws(()=>mock([],{messageLookup:'automatic'}),e=>e.reason==='ghl_adapter_configuration');
 assert.throws(()=>mock([],{messageLookupMaxPages:4}),e=>e.reason==='ghl_adapter_configuration');
});

test('explicit history logical get-message requires prior observed ID and refreshes unique conversation scope',async()=>{
 const {adapter,calls}=mock([conversation(),page(),conversation(),page()],{messageLookup:'conversation_history'});
 await rejects(adapter.execute(detailRequest),'ghl_message_scope_unverified');assert.equal(calls.length,0);
 await adapter.execute(searchRequest);await adapter.execute(messagesRequest);const got=await adapter.execute(detailRequest);
 assert.deepEqual(got.data.message,message());assert.deepEqual(got.source,{transport:'conversation_history',operationId:'get-messages',method:'GET',path:'/conversations/{conversationId}/messages',scopeVerifiedBy:'search-conversation',matchedBy:'exact_message_id',pages:1});
 assert.equal(got.operationId,'get-message');assert.deepEqual(calls.map(c=>new URL(c.url).pathname),['/conversations/search','/conversations/'+V+'/messages','/conversations/search','/conversations/'+V+'/messages']);
});

test('restart-safe exact history read takes explicit scope and only accepts the actual ID on bounded later page',async()=>{
 const first={messages:{messages:[message({id:'other',body:message().body})],nextPage:true,lastMessageId:'other'}};
 const {adapter,calls}=mock([conversation(),first,page()],{messageLookup:'conversation_history'});
 const r=await adapter.readExactMessage({id:M,contactId:C,conversationId:V});assert.equal(r.data.message.id,M);assert.equal(r.source.pages,2);assert.equal(new URL(calls[2].url).searchParams.get('lastMessageId'),'other');assert.ok(calls.every(c=>c.init.method==='GET'));
});

test('history exact lookup never substitutes matching text/time and stops at configured page bound',async()=>{
 const otherPage={messages:{messages:[message({id:'different'})],nextPage:false,lastMessageId:'different'}},missing=mock([conversation(),otherPage],{messageLookup:'conversation_history'});
 await rejects(missing.adapter.readExactMessage({id:M,contactId:C,conversationId:V}),'ghl_exact_message_not_found');assert.equal(missing.calls.length,2);
 for(const max of [2,3]){
  const pages=Array.from({length:max+1},(_,i)=>({messages:{messages:[message({id:'m_'+i})],nextPage:true,lastMessageId:'m_'+i}})),x=mock([conversation(),...pages],{messageLookup:'conversation_history',messageLookupMaxPages:max});
  await rejects(x.adapter.readExactMessage({id:M,contactId:C,conversationId:V}),'ghl_exact_message_page_limit');assert.equal(x.calls.length,1+max);
 }
});

test('history scope changes, ambiguity, missing scope and duplicate IDs cannot establish an exact read',async()=>{
 for(const response of [{conversations:[],total:0},{conversations:[{id:'other',contactId:C,locationId:LOCATION}],total:1},{conversations:[{id:V,contactId:C,locationId:LOCATION},{id:'other',contactId:C,locationId:LOCATION}],total:2}]){
  const x=mock([response],{messageLookup:'conversation_history'});await rejects(x.adapter.readExactMessage({id:M,contactId:C,conversationId:V}),'ghl_unique_conversation_required');assert.equal(x.calls.length,1);
 }
 for(const response of [{messages:{messages:[message({contactId:'other'})],nextPage:false,lastMessageId:M}},{messages:{messages:[message(),message()],nextPage:false,lastMessageId:M}}]){
  const x=mock([conversation(),response],{messageLookup:'conversation_history'});await assert.rejects(x.adapter.readExactMessage({id:M,contactId:C,conversationId:V}));
 }
 const bad=mock([],{messageLookup:'conversation_history'});await rejects(bad.adapter.readExactMessage({id:M,contactId:C}),'ghl_message_scope');assert.equal(bad.calls.length,0);
});

test('history send still makes one POST, then verifies only returned outbound ID with permit and truthful provenance',async()=>{
 const out=outbound({status:'delivered'}),history={messages:{messages:[out,message()],nextPage:false,lastMessageId:M}},x=mock([conversation(),page(),receipt,conversation(),history],{messageLookup:'conversation_history'});
 await x.adapter.execute(searchRequest);await x.adapter.execute(messagesRequest);const sent=await x.adapter.send(permit()),verified=await x.adapter.verify(sent,{permit:permit(),event});
 assert.equal(verified.outcome,'delivered');assert.equal(verified.outbound.id,O);assert.equal(verified.source.transport,'conversation_history');assert.equal(x.calls.filter(c=>c.init.method==='POST').length,1);assert.equal(x.calls.some(c=>new URL(c.url).pathname==='/conversations/messages/'+O),false);
 await rejects(x.adapter.send(permit()),'ghl_send_attempt_already_used');
});

test('history verification after restart needs exact response ID, scope and body; not-found/429 stay uncertain',async()=>{
 for(const response of [{messages:{messages:[outbound({id:'wrong',body:permit().params.body.message})],nextPage:false,lastMessageId:'wrong'}},{messages:{messages:[outbound({body:'wrong body'})],nextPage:false,lastMessageId:O}},new Response('unprinted error',{status:429})]){
  const x=mock([conversation(),response],{messageLookup:'conversation_history',now:()=>now+3600000});await assert.rejects(x.adapter.verify(receipt,{permit:permit(),event}));assert.equal(x.calls.filter(c=>c.init.method==='POST').length,0);
 }
});

