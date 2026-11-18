// Direct, server-owned HighLevel adapter. Supply a NEW location-scoped private
// integration token; never reuse the ChatGPT MCP or webhook secrets.
// Contracts verified 2026-10-20 against the official 2021-07-28 reference:
// https://marketplace.gohighlevel.com/docs/2021-07-28/ghl/conversations/get-message
// https://marketplace.gohighlevel.com/docs/2021-07-28/ghl/opportunities/search-opportunity
// https://marketplace.gohighlevel.com/docs/2021-07-28/ghl/payments/list-transactions
import {LOCATION,PROVIDER,validId} from './sales-policy.mjs';

export const GHL_ORIGIN='https://services.leadconnectorhq.com';
export const GHL_VERSION='2021-07-28';
export const GHL_REQUIRED_SCOPES=Object.freeze(['contacts.readonly','conversations.readonly','conversations/message.readonly','conversations/message.write','opportunities.readonly','payments/transactions.readonly']);
const object=x=>x!==null&&typeof x==='object'&&!Array.isArray(x);
const exactKeys=(x,keys)=>object(x)&&Object.keys(x).length===keys.length&&keys.every(k=>Object.hasOwn(x,k));
const count=x=>Number.isSafeInteger(x)&&x>=0;
const has=(x,k)=>Object.hasOwn(x,k);
const fail=(reason,status,operationId)=>{throw new GhlAdapterError(reason,{status,operationId});};
export class GhlAdapterError extends Error {
 constructor(reason,{status,operationId}={}){super(reason);this.name='GhlAdapterError';this.reason=reason;if(Number.isInteger(status))this.status=status;if(typeof operationId==='string')this.operationId=operationId;}
}

export class GhlAdapter {
 #token;#fetch;#timeoutMs;#now;#messageLookup;#messageLookupMaxPages;#conversations=new Map();#messages=new Map();#attempts=new Set();#rateLimitedUntil=0;
 constructor({token,fetchImpl=globalThis.fetch,timeoutMs=8000,now=Date.now,messageLookup='direct',messageLookupMaxPages=3}={}){
  if(typeof token!=='string'||token.length<16||token.length>8192||/[\s\x00-\x1f\x7f]/.test(token))fail('ghl_private_integration_token_required');
  if(typeof fetchImpl!=='function'||typeof now!=='function'||!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>60000||!['direct','conversation_history'].includes(messageLookup)||!Number.isSafeInteger(messageLookupMaxPages)||messageLookupMaxPages<1||messageLookupMaxPages>3)fail('ghl_adapter_configuration');
  this.#token=token;this.#fetch=fetchImpl;this.#timeoutMs=timeoutMs;this.#now=now;this.#messageLookup=messageLookup;this.#messageLookupMaxPages=messageLookupMaxPages;
  this.execute=this.execute.bind(this);this.send=this.send.bind(this);this.verify=this.verify.bind(this);this.readExactMessage=this.readExactMessage.bind(this);
 }
 toJSON(){return {adapter:'ghl_direct',location_id:LOCATION,provider_id:PROVIDER,api_version:GHL_VERSION,message_lookup:this.#messageLookup};}
 #rememberMessage(m){
  const prior=this.#messages.get(m.id);
  if(prior&&(prior.contactId!==m.contactId||prior.conversationId!==m.conversationId||prior.locationId!==m.locationId))fail('ghl_message_scope_changed');
  this.#messages.set(m.id,{id:m.id,contactId:m.contactId,conversationId:m.conversationId,locationId:m.locationId,direction:m.direction,conversationProviderId:m.conversationProviderId,messageType:m.messageType});
 }
 #messageScope(m,{id,conversationId,contactId}={}){
  if(!object(m)||!validId(m.id)||m.locationId!==LOCATION||!validId(m.contactId)||!validId(m.conversationId)||id&&m.id!==id||conversationId&&m.conversationId!==conversationId||contactId&&m.contactId!==contactId)fail('ghl_message_scope');
  const known=this.#conversations.get(m.conversationId),prior=this.#messages.get(m.id);
  if(known&&known!==m.contactId||prior&&(prior.contactId!==m.contactId||prior.conversationId!==m.conversationId))fail('ghl_message_scope');
  // Direction, provider, type, consent, time and body are not inferred here.
  this.#rememberMessage(m);return m;
 }
 async #request(operationId,method,path,query,body,signal){
  if(this.#rateLimitedUntil>this.#now())fail('provider_rate_limited',429,operationId);
  if(signal?.aborted)fail('ghl_request_aborted',undefined,operationId);
  const url=new URL(path,GHL_ORIGIN);for(const [k,v] of Object.entries(query??{}))url.searchParams.set(k,String(v));
  const controller=new AbortController();let timer;
  const combined=signal?AbortSignal.any([signal,controller.signal]):controller.signal;
  const work=async()=>{
   let response;
   try{response=await this.#fetch(url.href,{method,headers:{Authorization:'Bearer '+this.#token,Version:GHL_VERSION,Accept:'application/json',...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{}),signal:combined,redirect:'error'});}
   catch{fail(combined.aborted?(method==='POST'?'ghl_send_unknown':'ghl_request_aborted'):'ghl_network_failure',undefined,operationId);}
   const status=response?.status;
   if(!Number.isInteger(status))fail('ghl_invalid_response',undefined,operationId);
   if(status===429){
    const seconds=response.headers?.get?.('retry-after'),wait=typeof seconds==='string'&&/^\d+$/.test(seconds)?Math.min(3600000,Math.max(1000,Number(seconds)*1000)):60000;
    this.#rateLimitedUntil=this.#now()+wait;
   }
   if(status!==200&&!(method==='POST'&&status===201)){
    // Never attach upstream body, URL, headers, token, or thrown fetch details.
    if(response.body?.cancel)void response.body.cancel().catch(()=>{});
    fail(status===429?'provider_rate_limited':status===401||status===403?'ghl_authorization_failed':'ghl_http_error',status,operationId);
   }
   let data;try{data=await response.json();}catch{fail('ghl_invalid_json',status,operationId);}
   if(!object(data))fail('ghl_invalid_response',status,operationId);
   return {status,data};
  };
  try{
   return await Promise.race([work(),new Promise((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new GhlAdapterError(method==='POST'?'ghl_send_unknown':'ghl_request_timeout',{operationId}));},this.#timeoutMs);})]);
  }finally{clearTimeout(timer);}
 }
 async execute({operationId,locationId,params,signal}={}){
  if(locationId!==LOCATION)fail('ghl_location_scope');
  let path,query={},contactId,conversationId,messageId;
  switch(operationId){
   case 'search-conversation':
    if(!exactKeys(params,['query'])||!exactKeys(params.query,['contactId','limit'])||!validId(params.query.contactId)||params.query.limit!==2)fail('ghl_request_contract');
    contactId=params.query.contactId;path='/conversations/search';query={locationId:LOCATION,contactId,limit:2};break;
   case 'get-contact':case 'get-appointments-for-contact':
    if(!exactKeys(params,['path'])||!exactKeys(params.path,['contactId'])||!validId(params.path.contactId))fail('ghl_request_contract');
    contactId=params.path.contactId;path='/contacts/'+contactId+(operationId==='get-contact'?'':'/appointments');break;
   case 'get-messages':
    if(!exactKeys(params,['path','query'])||!exactKeys(params.path,['conversationId'])||!validId(params.path.conversationId)||!object(params.query)||!exactKeys(params.query,has(params.query,'lastMessageId')?['limit','lastMessageId']:['limit'])||params.query.limit!==50||has(params.query,'lastMessageId')&&!validId(params.query.lastMessageId))fail('ghl_request_contract');
    conversationId=params.path.conversationId;contactId=this.#conversations.get(conversationId);if(!contactId)fail('ghl_conversation_scope_unverified');
    path='/conversations/'+conversationId+'/messages';query={...params.query};break;
   case 'get-message':
    if(!exactKeys(params,['path'])||!exactKeys(params.path,['id'])||!validId(params.path.id))fail('ghl_request_contract');
    messageId=params.path.id;path='/conversations/messages/'+messageId;break;
   case 'search-opportunity':
    if(!exactKeys(params,['query'])||!exactKeys(params.query,['contactId','status','limit'])||!validId(params.query.contactId)||params.query.status!=='all'||params.query.limit!==100)fail('ghl_request_contract');
    contactId=params.query.contactId;path='/opportunities/search';query={location_id:LOCATION,contact_id:contactId,status:'all',limit:100};break;
   case 'list-transactions':
    if(!exactKeys(params,['query'])||!exactKeys(params.query,['altType','locationId','contactId','limit','offset'])||params.query.altType!=='location'||params.query.locationId!==LOCATION||!validId(params.query.contactId)||params.query.limit!==100||params.query.offset!==0)fail('ghl_request_contract');
    contactId=params.query.contactId;path='/payments/transactions';query={...params.query,altId:LOCATION};break;
   default:fail('ghl_operation_not_allowed');
  }
  if(operationId==='get-message'&&this.#messageLookup==='conversation_history'){
   const known=this.#messages.get(messageId);
   if(!known)fail('ghl_message_scope_unverified');
   return this.readExactMessage({id:messageId,contactId:known.contactId,conversationId:known.conversationId,signal});
  }
  const response=await this.#request(operationId,'GET',path,query,null,signal);let data=response.data;
  if(operationId==='search-conversation'){
   if(!Array.isArray(data.conversations)||!count(data.total)||data.total<data.conversations.length)fail('ghl_response_contract');
   for(const c of data.conversations){if(!object(c)||!validId(c.id)||c.locationId!==LOCATION||c.contactId!==contactId)fail('ghl_conversation_scope');const prior=this.#conversations.get(c.id);if(prior&&prior!==contactId)fail('ghl_conversation_scope');this.#conversations.set(c.id,contactId);}
  }else if(operationId==='get-contact'){
   if(!object(data.contact)||data.contact.id!==contactId||data.contact.locationId!==LOCATION)fail('ghl_contact_scope');
  }else if(operationId==='get-messages'){
   const p=data.messages;
   if(!object(p)||!Array.isArray(p.messages)||typeof p.nextPage!=='boolean'||p.messages.length>50||(p.nextPage||p.lastMessageId!=null)&&!validId(p.lastMessageId))fail('ghl_response_contract');
   for(const m of p.messages)this.#messageScope(m,{conversationId,contactId});
  }else if(operationId==='get-message'){
   // Direct GET is a bare message object; only the envelope changes.
   data={message:this.#messageScope(data,{id:messageId})};
  }else if(operationId==='get-appointments-for-contact'){
   if(!Array.isArray(data.events))fail('ghl_response_contract');
   if(data.events.some(a=>!object(a)||!validId(a.id)||a.contactId!==contactId||a.locationId!==LOCATION))fail('ghl_appointment_scope');
  }else if(operationId==='search-opportunity'){
   if(!Array.isArray(data.opportunities)||!object(data.meta)||!count(data.meta.total))fail('ghl_response_contract');
   if(data.opportunities.some(o=>!object(o)||!validId(o.id)||o.contactId!==contactId||o.locationId!==LOCATION))fail('ghl_opportunity_scope');
  }else if(operationId==='list-transactions'){
   if(!Array.isArray(data.data)||!count(data.totalCount))fail('ghl_response_contract');
   if(data.data.some(t=>!object(t)||!validId(t._id)||t.contactId!==contactId||t.altType!=='location'||t.altId!==LOCATION||has(t,'locationId')&&t.locationId!==LOCATION))fail('ghl_transaction_scope');
  }
  return {operationId,success:true,status:response.status,data};
 }
 async readExactMessage({id,contactId,conversationId,signal}={}){
  if(!validId(id)||!validId(contactId)||!validId(conversationId))fail('ghl_message_scope');
  if(this.#messageLookup==='direct'){
   const result=await this.execute({operationId:'get-message',locationId:LOCATION,params:{path:{id}},signal});
   this.#messageScope(result.data.message,{id,contactId,conversationId});return result;
  }
  // Explicit alternative transport for the observed PIT source/channel mismatch.
  // Never triggered by a 401. Re-establish the contact's unique conversation on
  // every exact read; no cached body or text/time matching is accepted as proof.
  const scoped=await this.execute({operationId:'search-conversation',locationId:LOCATION,params:{query:{contactId,limit:2}},signal});
  if(scoped.data.total!==1||scoped.data.conversations.length!==1||scoped.data.conversations[0].id!==conversationId)fail('ghl_unique_conversation_required');
  let cursor=null;const cursors=new Set(),seen=new Set();
  for(let page=1;page<=this.#messageLookupMaxPages;page++){
   const response=await this.execute({operationId:'get-messages',locationId:LOCATION,params:{path:{conversationId},query:{limit:50,...(cursor?{lastMessageId:cursor}:{})}},signal});
   const data=response.data.messages;
   for(const m of data.messages){if(seen.has(m.id))fail('ghl_history_duplicate_message');seen.add(m.id);}
   const matches=data.messages.filter(m=>m.id===id);
   if(matches.length===1)return {operationId:'get-message',success:true,status:200,data:{message:this.#messageScope(matches[0],{id,contactId,conversationId})},source:{transport:'conversation_history',operationId:'get-messages',method:'GET',path:'/conversations/{conversationId}/messages',scopeVerifiedBy:'search-conversation',matchedBy:'exact_message_id',pages:page}};
   if(!data.nextPage)fail('ghl_exact_message_not_found');
   if(!validId(data.lastMessageId)||cursors.has(data.lastMessageId))fail('ghl_history_pagination_chain');
   cursors.add(data.lastMessageId);cursor=data.lastMessageId;
  }
  fail('ghl_exact_message_page_limit');
 }
 #permit(permit,{late=false}={}){
  const p=permit,b=p?.params?.body;
  if(!object(p)||p.permitted!==true||p.locationId!==LOCATION||p.operationId!=='send-a-new-message'||!validId(p.attempt_id)||p.idempotencyKey!==p.attempt_id||!validId(p.message_id)||!exactKeys(p.params,['body'])||!exactKeys(b,['type','contactId','conversationProviderId','message','replyMessageId'])||b.type!=='Custom'||b.conversationProviderId!==PROVIDER||!validId(b.contactId)||b.replyMessageId!==p.message_id||typeof b.message!=='string'||!b.message.trim()||b.message.length>1500)fail('ghl_send_permit_required');
  const expiry=Date.parse(p.expires_at);
  if(!Number.isFinite(expiry)||!late&&(expiry<=this.#now()||expiry>this.#now()+15000))fail('ghl_send_permit_expired');
  return b;
 }
 async send(permit,{signal}={}){
  const body=this.#permit(permit),reply=this.#messages.get(body.replyMessageId);
  if(!reply||reply.locationId!==LOCATION||reply.contactId!==body.contactId||reply.direction!=='inbound'||reply.conversationProviderId!==PROVIDER||reply.messageType!=='TYPE_CUSTOM_PROVIDER_SMS')fail('ghl_send_reply_scope_unverified');
  if(this.#attempts.has(permit.attempt_id))fail('ghl_send_attempt_already_used');
  // The durable ledger owns cross-process dedupe. This additional in-process
  // fence is consumed before I/O even when a timeout leaves delivery unknown.
  this.#attempts.add(permit.attempt_id);
  const {data}=await this.#request('send-a-new-message','POST','/conversations/messages',null,body,signal);
  if(!validId(data.messageId)||data.messageId===body.replyMessageId||data.conversationId!==reply.conversationId)fail('ghl_send_response_incomplete');
  return {messageId:data.messageId,conversationId:data.conversationId};
 }
 async verify(sendResult,{permit,event,signal}={}){
  const body=this.#permit(permit,{late:true});
  if(!object(sendResult)||!validId(sendResult.messageId)||sendResult.messageId===body.replyMessageId||!validId(sendResult.conversationId)||event?.data?.location_id!==LOCATION||event.data.contact_id!==body.contactId||event.data.conversation_id&&event.data.conversation_id!==sendResult.conversationId)fail('ghl_verification_scope');
  const result=await this.readExactMessage({id:sendResult.messageId,contactId:body.contactId,conversationId:sendResult.conversationId,signal}),m=result.data.message;
  if(m.contactId!==body.contactId||m.conversationId!==sendResult.conversationId||m.conversationProviderId!==PROVIDER||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS'||m.direction!=='outbound'||m.body!==body.message)fail('ghl_outbound_mismatch');
  const outcomes=new Map([['queued','queued'],['pending','queued'],['sent','sent'],['delivered','delivered'],['read','delivered'],['failed','failed']]);
  const outcome=outcomes.get(m.status)??'unknown';
  return {outcome,send_response:{messageId:sendResult.messageId,conversationId:sendResult.conversationId},outbound:m,...(result.source?{source:result.source}:{})};
 }
}

