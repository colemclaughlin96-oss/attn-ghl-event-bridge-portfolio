import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,EVENT,LOCATION} from '../../src/bridge.mjs';
import {PROVIDER,priceAnswer,BOOKING_LINK,CHECKOUT_LINK} from '../../src/sales-policy.mjs';
import {prospectOptions} from '../../src/prospect-v1.mjs';
import {rpc} from '../../src/server.mjs';
// All fixtures are offline synthetic data, never real contact enrollment or events.
export const C='synthetic_prospect',V='synthetic_thread',owner='synthetic_principal',cut='2026-10-21T15:00:00.000Z';
const rejects=(f,r)=>assert.throws(f,e=>e.reason===r);
export function fixture(t,{enabled=true,ingress=true}={}){
 let now=Date.parse(cut)+10000;
 const dir=mkdtempSync(path.join(tmpdir(),'prospect-v1-'));
 const opts={dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,71).toString('base64'),testContactId:'synthetic_owner_contact',enabled:true,now:()=>now,prospectV1:{enabled,ingress,notBefore:ingress?cut:null,exclusions:ingress?[{contactId:'synthetic_sold',reason:'sold_owner_confirmed',evidenceRef:'synthetic_owner_sold'},{contactId:'synthetic_conflict',reason:'sales_status_conflict',evidenceRef:'synthetic_conflict_receipt'}]:[]},post:async(u,b)=>({status:200,body:JSON.stringify({challenge:JSON.parse(b).challenge})})};
 let b=new Bridge(opts);t.after(()=>{b.close();rmSync(dir,{recursive:true,force:true});});
 const m=(id='in_1',x={})=>({id,locationId:LOCATION,contactId:C,conversationId:V,conversationProviderId:PROVIDER,messageType:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound',dateAdded:'2026-10-21T15:00:01.000Z',body:'What does Google Ads cost?',attachments:[],...x});
 const read=(operationId,params,data)=>({operationId,locationId:LOCATION,params,result:{operationId,status:200,success:true,data}});
 const snap=(messages=[m()],{full=true,detail=null}={})=>{
  const records=[...messages,m('timezone',{dateAdded:'2026-10-20T14:00:00.000Z',body:"I'm in Pacific time."})].sort((a,b)=>Date.parse(b.dateAdded)-Date.parse(a.dateAdded));
  const reads=[read('search-conversation',{query:{contactId:C,limit:2}},{total:1,conversations:[{id:V,contactId:C,locationId:LOCATION}]}),read('get-contact',{path:{contactId:C}},{contact:{id:C,locationId:LOCATION,dnd:false,country:'US',type:'lead',tags:['imessage'],timezone:'America/Los_Angeles'}}),read('get-messages',{path:{conversationId:V},query:{limit:50}},{messages:{messages:records,nextPage:false,lastMessageId:records.at(-1).id}})];
  if(full)reads.push(read('get-appointments-for-contact',{path:{contactId:C}},{events:[]}),read('search-opportunity',{query:{contactId:C,status:'all',limit:100}},{opportunities:[],meta:{total:0,nextPageUrl:null,nextPage:''}}),read('list-transactions',{query:{altType:'location',locationId:LOCATION,contactId:C,limit:100,offset:0}},{data:[],totalCount:0}));
  if(detail)reads.push(read('get-message',{path:{id:detail.id}},{message:detail}));
  return {read_started_at:new Date(now).toISOString(),read_completed_at:new Date(now).toISOString(),reads,assessments:messages.filter(x=>x.direction==='inbound').map(x=>({messageId:x.id,kind:'normal'}))};
 };
 const subscribe=()=>b.subscribe(owner,{name:EVENT,arguments:{location_id:LOCATION,scope:'attn_incoming_prospects'},delivery:{mode:'webhook',url:'https://callback.example.com/',secret:'whsec_'+Buffer.alloc(32,21).toString('base64')}});
 const event=async(id='loop1',contact=C,claim=true)=>{await subscribe();const e=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:contact,direction:'inbound',loopMessageId:id},'loop_workflow');if(claim)b.claim(owner,e.eventId);return e.eventId;};
 const draft={text:priceAnswer('google'),intent:'pricing',priceKey:'google',answersMessageId:'in_1',policyReviewed:true,untrustedContentIgnored:true};
 const prepare=(id,s=snap())=>b.prospectV1.prepare(owner,id,s);
 const begin=(id,p,s=snap([m()],{detail:m()}),d=draft)=>b.prospectV1.begin(owner,id,{snapshot:s,draft:d,message_id:p.message_id,thread_version:p.thread_version});
 return {get b(){return b;},get l(){return b.prospectV1;},opts,m,read,snap,event,subscribe,prepare,begin,draft,get now(){return now;},advance:n=>now+=n,restart:()=>{b.close();b=new Bridge(opts);}};
}
export const result=(f,p,status='sent')=>({attempt_id:p.attempt_id,outcome:status,send_response:{messageId:'out1',conversationId:V},outbound:f.m('out1',{direction:'outbound',body:f.draft.text,status,dateAdded:new Date(f.now).toISOString()})});

