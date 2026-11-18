// Narrow owner-pilot adapter for actual official MCP request/response envelopes.
// The authenticated agent supplies evidence; this is validation, not a GHL signature.
import {OWNER_SCOPE as S} from './owner-reply.mjs';
import {blocked,utc,validId} from './sales-policy.mjs';
import {hash,canonical} from './security.mjs';
export const AUTOMATED_OWNER_ACK=Object.freeze({id:'portfolio_fixture_09',dateAdded:'2026-10-20T20:27:10.767Z',body:'ATTN cloud owner test received. This is the one-time test acknowledgement.',source:'api',messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'outbound',...S});
export function exactPriorAutomation(m){return Object.entries(AUTOMATED_OWNER_ACK).every(([k,v])=>m[k]===v);}
export const CONFIRMED_MANUAL=Object.freeze({...S,id:'portfolio_fixture_10',dateAdded:'2026-10-19T23:04:31.339Z',body:'hi this is chat bro',source:'api',direction:'outbound',messageType:'TYPE_CUSTOM_PROVIDER_SMS',conversationProviderId:'portfolio_fixture_11'});
export const MANUAL_RELEASE='2026-10-20T23:04:31.339Z';
export function exactConfirmedManual(m){return Object.entries(CONFIRMED_MANUAL).every(([k,v])=>m[k]===v);}
const object=x=>x&&typeof x==='object'&&!Array.isArray(x);
// The v2021 transaction response documents status/liveMode and scoped
// altId/altType/contactId. Do not turn an attempted payment into a paid customer.
// Only these explicit negative states clear this gate; all unknowns stay held.
// Source: https://marketplace.gohighlevel.com/docs/2021-07-28/ghl/payments/list-transactions
export function transactionPaymentEvidence(transactions,{locationId,contactId}={}){
  if(!Array.isArray(transactions)||!validId(locationId)||!validId(contactId))return {paid:null,classification:'unknown'};
  if(!transactions.length)return {paid:false,classification:'no_records'};
  let unknown=false,paid=false;const ids=new Set();
  for(const t of transactions){
    if(!object(t)||!validId(t._id)||ids.has(t._id)||t.altType!=='location'||t.altId!==locationId||t.contactId!==contactId||Object.hasOwn(t,'locationId')&&t.locationId!==locationId){unknown=true;continue;}
    ids.add(t._id);
    if(t.status==='succeeded'&&(t.liveMode===true||t.liveMode==='true')){paid=true;continue;}
    if(!['failed','pending','processing'].includes(t.status)){unknown=true;continue;}
    // A prior refund or contradictory captured charge means the non-paid status
    // cannot establish prospect eligibility. No provider-specific status guessing.
    const refund=t.amountRefunded;
    if(Object.hasOwn(t,'amountRefunded')&&!(refund===0||typeof refund==='string'&&/^0(?:\.0+)?$/.test(refund)))unknown=true;
    if(object(t.chargeSnapshot)&&t.chargeSnapshot.status==='succeeded')unknown=true;
  }
  return paid?{paid:true,classification:'paid'}:unknown?{paid:null,classification:'unknown'}:{paid:false,classification:'not_paid'};
}
function exact(a,b){return canonical(a)===canonical(b);}
function checkedResponse(read,op,params,S){
  if(!read||read.operationId!==op||read.locationId!==S.locationId||!exact(read.params,params)||read.result?.success!==true||read.result.status!==200||read.result.operationId!==op||!object(read.result.data))blocked('official_read_evidence_required');
  return read.result.data;
}
export function ownerEvidence(raw,options={}){return scopedEvidence(raw,{...options,scope:S,ownerOnly:true});}
export function scopedEvidence(raw,{full=true,candidateId=null,workflowRef=null,scope:S,ownerOnly=false,maxPages=2}={}){
  const response=(read,op,params)=>checkedResponse(read,op,params,S);
  if(!Number.isInteger(maxPages)||maxPages<1||maxPages>10||!object(raw)||!Array.isArray(raw.reads)||raw.reads.length>5+maxPages)blocked('official_reads_required');
  utc(raw.read_started_at);utc(raw.read_completed_at);
  const get=op=>{const rs=raw.reads.filter(r=>r.operationId===op);if(rs.length!==1)blocked('missing_or_duplicate_official_read');return rs[0];};
  const contact=response(get('get-contact'),'get-contact',{path:{contactId:S.contactId}}).contact;
  if(!object(contact)||contact.id!==S.contactId||contact.locationId!==S.locationId)blocked('contact_scope');
  const pageReads=raw.reads.filter(r=>r.operationId==='get-messages');
  if(pageReads.length<1||pageReads.length>maxPages)blocked('bounded_messages_required');
  const pages=pageReads.map((r,i)=>{
    const previous=i? pageReads[i-1].result?.data?.messages:null;
    if(i&&(!previous?.nextPage||!validId(previous.lastMessageId)))blocked('pagination_chain');
    const query=i?{limit:50,lastMessageId:previous.lastMessageId}:{limit:50};
    const p=response(r,'get-messages',{path:{conversationId:S.conversationId},query}).messages;
    if(!object(p))blocked('invalid_messages_response');
    return {request_last_message_id:i?previous.lastMessageId:null,nextPage:p.nextPage,lastMessageId:p.lastMessageId,messages:p.messages};
  });
  const allowed=['get-contact','get-messages'];
  let appointments=null,opportunities=null,transactions=null;
  if(full){
    appointments=response(get('get-appointments-for-contact'),'get-appointments-for-contact',{path:{contactId:S.contactId}}).events;
    const o=response(get('search-opportunity'),'search-opportunity',{query:{contactId:S.contactId,status:'all',limit:100}});
    opportunities=o.opportunities;
    const t=response(get('list-transactions'),'list-transactions',{query:{altType:'location',locationId:S.locationId,contactId:S.contactId,limit:100,offset:0}});transactions=t.data;
    // Owner test persona only. Any unexpected business state requires review, not inferred clearance.
    if(!Array.isArray(appointments)||!Array.isArray(opportunities)||!Array.isArray(transactions)||o.meta?.total!==opportunities.length||o.meta?.nextPageUrl!==null||![null,''].includes(o.meta?.nextPage)||t.totalCount!==transactions.length)blocked('incomplete_crm_evidence');
    allowed.push('get-appointments-for-contact','search-opportunity','list-transactions');
  }else{
    const a=raw.reads.filter(r=>r.operationId==='get-appointments-for-contact');
    if(a.length){appointments=response(get('get-appointments-for-contact'),'get-appointments-for-contact',{path:{contactId:S.contactId}}).events;if(!Array.isArray(appointments))blocked('invalid_appointments');allowed.push('get-appointments-for-contact');}
  }
  if(candidateId){
    const m=response(get('get-message'),'get-message',{path:{id:candidateId}}).message;
    const listed=pages.flatMap(p=>p.messages??[]).find(x=>x.id===candidateId);
    for(const k of ['id','locationId','contactId','conversationId','conversationProviderId','direction','messageType','dateAdded','body'])if(!m||!listed||m[k]!==listed[k])blocked('selected_message_mismatch');
    if(!exact(m.attachments??[],listed.attachments??[]))blocked('selected_message_mismatch');allowed.push('get-message');
  }
  if(raw.reads.some(r=>!allowed.includes(r.operationId)))blocked('unexpected_read_operation');
  const ref='mcp_'+hash(canonical(raw.reads));
  return {read_started_at:raw.read_started_at,read_completed_at:raw.read_completed_at,contact,pages,assessments:raw.assessments??[],crm:full?{locationId:S.locationId,contactId:S.contactId,complete:true,evidenceRef:ref,client:contact.type!=='lead',paid:ownerOnly?transactions.length>0:transactionPaymentEvidence(transactions,S).paid,sold:ownerOnly?opportunities.length>0:opportunities.some(o=>o.status==='won'||o.pipelineStageId==='9aef9c63-f86a-41c1-b241-ff013cf4a1ff'),conflict:ownerOnly?appointments.length>0:false,opportunities,transactions,appointments}:null,
    workflowSuppression:{locationId:S.locationId,contactId:S.contactId,verified:!!workflowRef,evidenceRef:workflowRef?.id,profileRevision:workflowRef?.id},
    channelPermission:{providerId:S.conversationProviderId,allowed:true,evidenceRef:ownerOnly?'owner_loop_send_receipt_20261020':'cole_loop_channel_approval_20261020'},
    bookingEvidence:appointments===null?'unknown':appointments.length?appointments:'no_appointments_returned'};
}


