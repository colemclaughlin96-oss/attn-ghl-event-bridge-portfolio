import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {Bridge,EVENT,LOCATION} from '../src/bridge.mjs';
import {PROVIDER,PRICING,priceAnswer,BOOKING_LINK,CHECKOUT_LINK} from '../src/sales-policy.mjs';
import {prospectOptions} from '../src/prospect-v1.mjs';
import {rpc} from '../src/server.mjs';
// All fixtures are offline synthetic data, never real contact enrollment or events.
const C='synthetic_prospect',V='synthetic_thread',owner='synthetic_principal',cut='2026-10-21T15:00:00.000Z';
const rejects=(f,r)=>assert.throws(f,e=>e.reason===r);
function fixture(t,{enabled=true,ingress=true}={}){
 let now=Date.parse(cut)+10000;
 const dir=mkdtempSync(path.join(tmpdir(),'prospect-v1-'));
 const opts={dbPath:path.join(dir,'db'),vaultKey:Buffer.alloc(32,71).toString('base64'),testContactId:'synthetic_owner_contact',enabled:true,now:()=>now,prospectV1:{enabled,ingress,notBefore:ingress?cut:null,exclusions:ingress?[{contactId:'synthetic_sold',reason:'sold_owner_confirmed',evidenceRef:'synthetic_owner_sold'},{contactId:'synthetic_conflict',reason:'sales_status_conflict',evidenceRef:'synthetic_conflict_receipt'}]:[]},post:async(u,b)=>({status:200,body:JSON.stringify({challenge:JSON.parse(b).challenge})})};
 const peers=[];let b=new Bridge(opts);t.after(()=>{for(const peer of peers)peer.close();b.close();rmSync(dir,{recursive:true,force:true});});
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
 const event=async(id='loop1',contact=C)=>{await subscribe();const e=b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:contact,direction:'inbound',loopMessageId:id},'loop_workflow');b.claim(owner,e.eventId);return e.eventId;};
 const draft={text:priceAnswer('google'),intent:'pricing',priceKey:'google',answersMessageId:'in_1',policyReviewed:true,untrustedContentIgnored:true};
 const prepare=(id,s=snap())=>b.prospectV1.prepare(owner,id,s);
 const begin=(id,p,s=snap([m()],{detail:m()}),d=draft)=>b.prospectV1.begin(owner,id,{snapshot:s,draft:d,message_id:p.message_id,thread_version:p.thread_version});
 return {get b(){return b;},get l(){return b.prospectV1;},opts,m,read,snap,event,subscribe,prepare,begin,draft,openBridge:()=>{const peer=new Bridge(opts);peers.push(peer);return peer;},get now(){return now;},advance:n=>now+=n,restart:()=>{b.close();b=new Bridge(opts);}};
}
const result=(f,p,status='sent')=>({attempt_id:p.attempt_id,outcome:status,send_response:{messageId:'out1',conversationId:V},outbound:f.m('out1',{direction:'outbound',body:f.draft.text,status,dateAdded:new Date(f.now).toISOString()})});
test('default runtime and wildcard disabled; invalid activation cannot combine legacy tests',async t=>{const f=fixture(t,{enabled:false,ingress:false});assert.equal(f.l.status().enabled,false);assert.deepEqual(prospectOptions({}),{ingress:false,enabled:false});assert.throws(()=>prospectOptions({PROSPECT_V1_REPLIES_ENABLED:'true'}));await assert.rejects(f.subscribe(),e=>e.reason==='test_scope_required');assert.equal(f.b.ingest({type:'LoopInboundMessage',locationId:LOCATION,contactId:C,direction:'inbound',loopMessageId:'x'},'loop_workflow').accepted,false);const r=await rpc(f.b,owner,{jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'attn_prospect_v1_ledger',arguments:{action:'prepare',event_id:'none',evidence:{}}}});assert.equal(r.error.data.reason,'prospect_v1_disabled');});
test('scope-enabled Loop notifications retain real contact and reject cross-location/invalid IDs; single wildcard delivery',async t=>{const f=fixture(t),id=await f.event();assert.equal(f.b.eventForOwner(owner,id).data.contact_id,C);assert.equal(f.b.db.prepare('SELECT count(*) n FROM subscriptions').get().n,1);assert.equal(f.b.db.prepare('SELECT count(*) n FROM deliveries').get().n,1);const bad={type:'LoopInboundMessage',locationId:'other',contactId:C,direction:'inbound',loopMessageId:'x'};assert.equal(f.b.ingest(bad,'loop_workflow').accepted,false);assert.equal(f.b.ingest({...bad,locationId:LOCATION,contactId:123},'loop_workflow').accepted,false);await f.b.drain();assert.equal(f.b.db.prepare('SELECT status FROM deliveries').get().status,'accepted');});
test('genuine event establishes one contact from actual MCP envelopes, immutable cutoff excludes historical unread',async t=>{const f=fixture(t),id=await f.event(),p=f.prepare(id);assert.equal(p.message_id,'in_1');assert.equal(f.l.rows(C).length,1);assert.equal(f.l.rows(C).some(r=>r.messageId==='timezone'),false);const e=f.l.read('prospect_entries',f.l.key(C));assert.equal(e.contactId,C);assert.equal(e.conversationId,V);assert.equal(e.timezone.source,'ghl_get_contact');assert.equal(e.timezone.field,'contact.timezone');assert.match(e.timezone.evidenceRef,/^mcp_/);assert.equal(e.notBefore,cut);f.opts.prospectV1.notBefore='2026-10-21T15:01:00.000Z';rejects(()=>new Bridge(f.opts),'prospect_cutoff_immutable');});
test('first contact event lookback is at most 120 seconds and missing candidate causes no polling/send',async t=>{const f=fixture(t);f.advance(3600000);const id=await f.event(),p=f.prepare(id);assert.equal(p.reason,'no_pending_inbound');assert.equal(f.l.read('prospect_entries',f.l.key(C)).notBefore,new Date(f.now-120000).toISOString());});
test('missing saved timezone learns a supported fresh contact statement without a GHL write',async t=>{const f=fixture(t),id=await f.event(),s=f.snap();delete s.reads[1].result.data.contact.timezone;assert.equal(f.prepare(id,s).prepared,true);const tz=f.l.read('prospect_entries',f.l.key(C)).timezone;assert.equal(tz.source,'contact_statement');assert.equal(tz.provisional,false);assert.equal(s.reads[1].result.data.contact.timezone,undefined);});
test('raw request/response scope, ambiguous threads and incomplete payments fail closed',async t=>{for(const kind of ['scope','many','payment']){const f=fixture(t),id=await f.event(),s=f.snap();if(kind==='scope')s.reads[0].result.data.conversations[0].contactId='other';if(kind==='many')s.reads[0].result.data.total=2;if(kind==='payment')s.reads[5].result.data.totalCount=1;rejects(()=>f.prepare(id,s),kind==='scope'?'conversation_scope':kind==='many'?'unique_contact_conversation_required':'incomplete_crm_evidence');assert.equal(f.l.rows(C).length,0);}});
test('current customer, any payment, won/paid-stage opportunity and unknown DND hold',async t=>{for(const k of ['customer','payment','won','paidstage','dnd']){const f=fixture(t),id=await f.event(),s=f.snap();if(k==='customer')s.reads[1].result.data.contact.type='customer';if(k==='dnd')delete s.reads[1].result.data.contact.dnd;if(k==='payment'){s.reads[5].result.data.data=[{id:'payment'}];s.reads[5].result.data.totalCount=1;}if(['won','paidstage'].includes(k)){s.reads[4].result.data.opportunities=[{id:'opp',locationId:LOCATION,contactId:C,status:k==='won'?'won':'open',pipelineStageId:k==='paidstage'?'9aef9c63-f86a-41c1-b241-ff013cf4a1ff':'other'}];s.reads[4].result.data.meta.total=1;}assert.equal(f.prepare(id,s).prepared,false);}});
test('ordinary open prospect evidence allows an answer; booked prospect cannot get another booking link',async t=>{const f=fixture(t),id=await f.event(),s=f.snap();s.reads[4].result.data.opportunities=[{id:'opp',locationId:LOCATION,contactId:C,status:'open',pipelineStageId:'demo'}];s.reads[4].result.data.meta.total=1;assert.equal(f.prepare(id,s).prepared,true);const g=fixture(t),e=await g.event(),z=g.snap();z.reads[3].result.data.events=[{id:'appt',locationId:LOCATION,contactId:C,calendarId:'portfolio_fixture_03',appointmentStatus:'confirmed'}];const p=g.prepare(e,z);const second=g.snap([g.m()],{detail:g.m()});second.reads[3]=z.reads[3];rejects(()=>g.begin(e,p,second,{...g.draft,intent:'booking_link',text:BOOKING_LINK}),'already_booked');});
test('known workflow source can coexist; unclassified api send holds rather than being declared automatic',async t=>{const f=fixture(t),id=await f.event(),workflow=f.m('workflow',{direction:'outbound',source:'workflow',body:'Would you like help?',dateAdded:'2026-10-21T14:59:59.000Z'});assert.equal(f.prepare(id,f.snap([f.m(),workflow])).prepared,true);const g=fixture(t),eid=await g.event();assert.equal(g.prepare(eid,g.snap([g.m(),{...workflow,source:'api'}])).reason,'unverified_external_sender');assert.equal(g.l.contactState(g.l.read('prospect_entries',g.l.key(C))).manualUntil,Date.parse(workflow.dateAdded)+86400000);});
test('second snapshot STOP, unknown provider, stale evidence and unapproved checkout block sends',async t=>{const f=fixture(t),id=await f.event(),p=f.prepare(id);rejects(()=>f.begin(id,p,undefined,{...f.draft,intent:'answer',text:'Pay at https://checkout.example.invalid'}),'unapproved_link');const stop=f.m('stop',{body:'STOP',dateAdded:'2026-10-21T15:00:02.000Z'});assert.equal(f.begin(id,p,f.snap([stop,f.m()],{detail:f.m()})).reason,'opt_out_or_dnd');const g=fixture(t),e=await g.event();assert.equal(g.prepare(e,g.snap([g.m('x',{conversationProviderId:'wrong'})])).reason,'provider_mismatch');});
test('concurrent events share message ledger; consumed unknown cannot be retried on new event/restart',async t=>{const f=fixture(t),a=await f.event('a'),b=await f.event('b'),p=f.prepare(a);const other=f.openBridge();assert.equal(other.prospectV1.prepare(owner,b,f.snap()).reason,'another_event_owns_candidate');assert.equal(f.begin(a,p).permitted,true);assert.equal(other.prospectV1.begin(owner,a,{snapshot:f.snap([f.m()],{detail:f.m()}),draft:f.draft,message_id:p.message_id,thread_version:p.thread_version}).permitted,false);f.restart();assert.equal(f.prepare(b).reason,'prior_send_unresolved_or_failed');});
test('sent result stays sent; later genuine new inbound can advance without resetting old attempt',async t=>{const f=fixture(t),id=await f.event(),p=f.begin(id,f.prepare(id)),r=result(f,p);assert.equal(f.l.record(owner,id,r).phase,'sent');f.advance(1000);const e=await f.event('next'),m=f.m('in2',{dateAdded:new Date(f.now-1).toISOString(),body:'How do I book?'});const s=f.snap([m,r.outbound,f.m()]);assert.equal(f.prepare(e,s).message_id,'in2');assert.equal(f.l.rows(C).find(r=>r.messageId==='in_1').phase,'sent');});
test('unknown evidence can create contact hold without fabricated thread/question; once only alert',async t=>{const f=fixture(t),id=await f.event();const r=f.l.handoff(owner,id,{reason:'official_contact_read_failed',decision_needed:'Cole to review the contact; no message was read.'});assert.equal(r.alert.exact_question,null);assert.equal(r.alert.conversation_id,null);assert.equal(r.locked,true);assert.equal(f.prepare(id).reason,'unresolved_handoff');f.restart();assert.equal(f.l.handoff(owner,id,{reason:'again',decision_needed:'review'}).should_alert,false);});
test('reply permission remains off in ingress-only review mode, while handoff can persist',async t=>{const f=fixture(t,{enabled:false}),id=await f.event();assert.equal(f.prepare(id).reason,'prospect_replies_disabled');assert.equal(f.l.handoff(owner,id,{reason:'review_only',decision_needed:'Review.'}).send_authorized,false);});

test('approved checkout requires actual selected buying-intent message, never a general pricing question',async t=>{const f=fixture(t),id=await f.event(),p=f.prepare(id);rejects(()=>f.begin(id,p,undefined,{...f.draft,text:'You can get started here: '+CHECKOUT_LINK,intent:'checkout_link',buyingIntentVerified:true}),'relevant_buying_intent_required');const g=fixture(t),eid=await g.event(),m=g.m('in_1',{body:"I'm ready to buy. Send me the checkout link."}),prep=g.prepare(eid,g.snap([m]));assert.equal(g.begin(eid,prep,g.snap([m],{detail:m}),{...g.draft,text:'You can get started here: '+CHECKOUT_LINK,intent:'checkout_link',buyingIntentVerified:true}).permitted,true);});

test('fresh saved IANA timezone is approved without phone/country/declaration inference',async t=>{
 const f=fixture(t),id=await f.event(),s=f.snap();delete s.reads[1].result.data.contact.country;s.reads[2].result.data.messages.messages.pop();
 assert.equal(f.prepare(id,s).prepared,true);const tz=f.l.read('prospect_entries',f.l.key(C)).timezone;
 assert.equal(tz.value,'America/Los_Angeles');assert.equal(tz.source,'ghl_get_contact');assert.equal(tz.contactId,C);assert.equal(tz.locationId,LOCATION);assert.equal(tz.verified_at,s.read_completed_at);
});
function blank(f,messages=[f.m()],opts={}){const s=f.snap(messages,opts);delete s.reads[1].result.data.contact.timezone;s.reads[2].result.data.messages.messages=s.reads[2].result.data.messages.messages.filter(m=>m.id!=='timezone');s.reads[2].result.data.messages.lastMessageId=s.reads[2].result.data.messages.messages.at(-1).id;return s;}
test('missing and invalid timezones permit active pricing and international incoming answers',async t=>{
 for(const value of [undefined,null,'','   ','America/Imaginary','PST',123,false]){
  const f=fixture(t),id=await f.event(),s=blank(f);if(value!==undefined)s.reads[1].result.data.contact.timezone=value;
  s.reads[1].result.data.contact.country='GB';s.reads[1].result.data.contact.phone='+442079460123';
  const p=f.prepare(id,s);assert.equal(p.prepared,true);assert.equal(p.delivery_kind,'active_inbound');assert.equal(p.timezone_evidence.value,null);
  assert.equal(p.timezone_evidence.status,[undefined,null,'','   '].includes(value)?'missing':'invalid');
  const pre=blank(f,[f.m()],{detail:f.m()});pre.reads[1].result.data.contact=s.reads[1].result.data.contact;
  assert.equal(f.begin(id,p,pre).permitted,true);assert.equal(s.reads[1].result.data.contact.timezone,value);
 }
});
test('active incoming answers are independent of hours and timezone changes during preparation',async t=>{
 const f=fixture(t),id=await f.event(),s=blank(f);s.reads[1].result.data.contact.timezone='Asia/Tokyo';
 const p=f.prepare(id,s);assert.equal(p.prepared,true);
 const pre=blank(f,[f.m()],{detail:f.m()});pre.reads[1].result.data.contact.timezone='America/Imaginary';assert.equal(f.begin(id,p,pre).permitted,true);
});
test('conflicting timezone evidence remains visible without blocking unrelated answers',async t=>{
 const f=fixture(t),id=await f.event(),s=f.snap();s.reads[2].result.data.messages.messages.at(-1).body="I'm in Eastern time.";
 const p=f.prepare(id,s);assert.equal(p.prepared,true);assert.equal(p.timezone_evidence.status,'conflicting');assert.equal(p.timezone_evidence.bookingConfirmed,false);
});
test('timezone can be learned or removed without lifting an existing human handoff',async t=>{
 const f=fixture(t),id=await f.event();f.prepare(id,blank(f));f.l.handoff(owner,id,{reason:'needs_Cole_terms',decision_needed:'Review terms'});f.b.processed(owner,id,'escalated');f.advance(1000);
 const e=await f.event('next'),s=blank(f);s.reads[1].result.data.contact.timezone='America/New_York';assert.equal(f.prepare(e,s).reason,'unresolved_handoff');
 const entry=f.l.read('prospect_entries',f.l.key(C));assert.equal(entry.timezone.value,'America/New_York');assert.equal(f.l.contactState(entry).handoff.reason,'needs_Cole_terms');
 const g=fixture(t),gid=await g.event();g.prepare(gid);g.b.processed(owner,gid,'observed');g.advance(1000);const ge=await g.event('next');assert.equal(g.prepare(ge,blank(g)).prepared,true);assert.equal(g.l.read('prospect_entries',g.l.key(C)).timezone.status,'missing');
});
test('stale evidence still rejects before any timezone state is saved',async t=>{
 const f=fixture(t),id=await f.event(),s=blank(f);f.advance(31000);rejects(()=>f.prepare(id,s),'fresh_snapshot_required');assert.equal(f.l.read('prospect_entries',f.l.key(C)),null);
});
test('unknown timezone booking request receives a question without a booking mutation',async t=>{
 const f=fixture(t),id=await f.event(),m=f.m('in_1',{body:'Can I book?'}),p=f.prepare(id,blank(f,[m]));
 const d={...f.draft,intent:'answer',text:'Sure. What timezone are you in?'};
 const permit=f.begin(id,p,blank(f,[m],{detail:m}),d);assert.equal(permit.permitted,true);assert.equal(permit.operationId,'send-a-new-message');assert.equal(f.l.status().booking_writes_enabled,false);
});
test('rapid incoming questions prepare one complete batch with latest reply anchor and one durable attempt',async t=>{
 const f=fixture(t),id=await f.event(),first=f.m('in_1',{body:'What does Google Ads cost?'}),last=f.m('in_2',{body:'And how does it work?',dateAdded:'2026-10-21T15:00:02.000Z'}),s=f.snap([last,first]),p=f.prepare(id,s);
 assert.deepEqual(p.message_ids,['in_1','in_2']);assert.equal(p.message_id,'in_2');
 // This supplied draft answers both questions; this is state/accounting coverage,
 // not an independent test that a native language model will write this answer.
 const text=priceAnswer('google')+' We build and manage Google Ads around the work you want more of.',d={...f.draft,text,priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:true},answersMessageId:'in_2',answersMessageIds:p.message_ids};
 rejects(()=>f.begin(id,p,f.snap([last,first],{detail:last}),{...d,answersMessageIds:['in_2']}),'draft_batch_review_required');
 const permit=f.begin(id,p,f.snap([last,first],{detail:last}),d);assert.equal(permit.params.body.replyMessageId,'in_2');assert.equal(new Set(f.l.rows(C).map(r=>r.attemptId)).size,1);assert.equal(f.l.rows(C).every(r=>r.phase==='attempt_unknown'),true);
 const r=result(f,permit);r.outbound.body=text;assert.equal(f.l.record(owner,id,r).phase,'sent');assert.equal(f.l.rows(C).every(r=>r.phase==='sent'&&r.outboundId==='out1'),true);
 assert.equal(f.prepare(id,s).reason,'no_pending_inbound');
});
test('new question during generation invalidates then reprepares same event including both questions',async t=>{
 const f=fixture(t),id=await f.event(),p=f.prepare(id),last=f.m('in_2',{body:'Can I book instead?',dateAdded:'2026-10-21T15:00:02.000Z'}),ms=[last,f.m()];
 assert.deepEqual(f.begin(id,p,f.snap(ms,{detail:f.m()})),{permitted:false,reason:'thread_changed',retryable:true});
 const q=f.prepare(id,f.snap(ms));assert.deepEqual(q.message_ids,['in_1','in_2']);assert.equal(q.message_id,'in_2');assert.equal(f.l.rows(C).some(r=>r.attemptId),false);
 const d={...f.draft,text:priceAnswer('google')+' For a review, what timezone are you in?',priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:true},answersMessageId:q.message_id,answersMessageIds:q.message_ids};assert.equal(f.begin(id,q,f.snap(ms,{detail:last}),d).permitted,true);
});
test('batch send timeout stays uncertain for every question and cannot retry',async t=>{
 const f=fixture(t),id=await f.event(),last=f.m('in_2',{dateAdded:'2026-10-21T15:00:02.000Z'}),ms=[last,f.m()],p=f.prepare(id,f.snap(ms)),d={...f.draft,answersMessageId:p.message_id,answersMessageIds:p.message_ids};
 const permit=f.begin(id,p,f.snap(ms,{detail:last}),d);assert.equal(f.l.record(owner,id,{attempt_id:permit.attempt_id,outcome:'unknown'}).retry_allowed,false);f.restart();
 assert.equal(f.l.rows(C).every(r=>r.phase==='attempt_unknown'),true);assert.equal(f.prepare(id,f.snap(ms)).reason,'prior_send_unresolved_or_failed');
});
test('STOP or manual takeover during batch preparation prevents every queued answer',async t=>{
 for(const kind of ['stop','manual']){
  const f=fixture(t),id=await f.event(),p=f.prepare(id),newer=f.m('new',{dateAdded:'2026-10-21T15:00:02.000Z',...(kind==='stop'?{body:'STOP'}:{direction:'outbound',source:'api',body:'Cole here, I will help.'})});
  const r=f.begin(id,p,f.snap([newer,f.m()],{detail:f.m()}));assert.equal(r.reason,kind==='stop'?'opt_out_or_dnd':'unverified_external_sender');assert.equal(f.l.rows(C).some(r=>r.attemptId),false);
 }
});
test('system reaction or provider error does not block a separate normal question or become a reply candidate',async t=>{
 const f=fixture(t),id=await f.event(),system=f.m('reaction',{conversationProviderId:'system',messageType:'TYPE_SMS',body:'Liked a message',dateAdded:'2026-10-21T15:00:02.000Z'}),s=f.snap([system,f.m()]);s.assessments.find(a=>a.messageId==='reaction').kind='system';const p=f.prepare(id,s);assert.deepEqual(p.message_ids,['in_1']);assert.equal(f.l.rows(C).find(r=>r.messageId==='reaction').phase,'ignored_system');
});
test('handoff receipt stores notification failure or success without claiming delivery or read',async t=>{
 const f=fixture(t),id=await f.event();f.prepare(id);f.l.handoff(owner,id,{reason:'custom_terms',decision_needed:'Review custom terms',snapshot:f.snap([f.m()],{full:false}),message_id:'in_1'});
 rejects(()=>f.l.recordHandoff(owner,id,{status:'notified',route:'existing_cloud_chat',success:true}),'handoff_receipt_required');
 const failed=f.l.recordHandoff(owner,id,{status:'failed',route:'existing_cloud_chat',reason:'tool_timeout'});assert.equal(failed.notified,false);assert.equal(f.l.read('prospect_alerts',f.l.key(C)).notification_receipt.reason,'tool_timeout');
 const ok=f.l.recordHandoff(owner,id,{status:'notified',route:'existing_cloud_chat',success:true,notificationId:'native_alert_1'});assert.equal(ok.notified,true);assert.equal(ok.push_delivered,false);assert.equal(ok.owner_read,false);assert.equal(f.prepare(id).reason,'unresolved_handoff');
});

function paginate(f,s,records){
 const rest=s.reads.filter(r=>r.operationId!=='get-messages'),pages=[];
 for(let i=0;i<records.length;i+=50){const messages=records.slice(i,i+50),previous=i?records[i-1].id:null;pages.push(f.read('get-messages',{path:{conversationId:V},query:{limit:50,...(previous?{lastMessageId:previous}:{})}},{messages:{messages,nextPage:i+50<records.length,lastMessageId:messages.at(-1).id}}));}
 s.reads=[...rest,...pages];return s;
}
test('history beyond two pages remains bounded and covers takeover evidence before preparing a batch',async t=>{
 const f=fixture(t);f.advance(3600000);const id=await f.event(),messages=Array.from({length:120},(_,i)=>f.m('burst_'+i,{body:'Message '+i,dateAdded:new Date(f.now-1000-i).toISOString()})),s=f.snap(messages),records=s.reads[2].result.data.messages.messages;
 const p=f.prepare(id,paginate(f,s,records));assert.equal(p.prepared,true);assert.equal(p.message_ids.length,120);assert.equal(p.message_id,'burst_0');
});
test('durable verified history checkpoint avoids requiring the original enrollment boundary forever',async t=>{
 const f=fixture(t),id=await f.event(),permit=f.begin(id,f.prepare(id)),r=result(f,permit);f.l.record(owner,id,r);f.b.processed(owner,id,'observed');f.advance(2*86400000);
 const next=await f.event('later'),newest=f.m('new_question',{dateAdded:new Date(f.now-1).toISOString(),body:'How do I book?'}),s=blank(f,[newest,r.outbound,f.m()]);s.reads.find(r=>r.operationId==='get-messages').result.data.messages.nextPage=true;
 assert.equal(f.prepare(next,s).message_id,'new_question');const state=f.l.contactState(f.l.read('prospect_entries',f.l.key(C)));assert.equal(state.historyCheckpoint.messageId,'new_question');
});
test('rolling history checkpoint cannot skip a last24h takeover gap or change prior message identity',async t=>{
 const f=fixture(t),id=await f.event();f.prepare(id);const s=blank(f);s.reads[2].result.data.messages.nextPage=true;
 rejects(()=>f.prepare(id,s),'takeover_24h_history_incomplete');
 const changed=blank(f);changed.reads[2].result.data.messages.messages[0].body='Changed history';rejects(()=>f.prepare(id,changed),'history_checkpoint_mismatch');
});

test('contact hash index avoids decrypting another contact and backfills without rewriting encrypted history',async t=>{
 const f=fixture(t),id=await f.event();f.prepare(id);const key=f.l.key('other_contact','other_message');f.l.write('sales_messages',key,{contactId:'other_contact',messageId:'other_message',phase:'pending'});
 const before=f.b.db.prepare('SELECT payload FROM prospect_messages WHERE id=?').get(key).payload;
 f.b.db.prepare('DELETE FROM prospect_message_index WHERE message_key=?').run(key);f.restart();
 assert.equal(f.b.db.prepare('SELECT payload FROM prospect_messages WHERE id=?').get(key).payload,before);
 const open=f.b.vault.open.bind(f.b.vault);let readOther=false;f.b.vault.open=(payload,id)=>{if(id===key)readOther=true;return open(payload,id);};
 assert.deepEqual(f.l.rows(C).map(r=>r.messageId),['in_1']);assert.equal(readOther,false);assert.equal(f.l.rows('other_contact').length,1);
});

test('history fetch requirements are scoped to the claimed event and preserve a full takeover window',async t=>{
 const f=fixture(t),id=await f.event(),initial=f.l.historyRequirements(owner,id);assert.equal(initial.contact_id,C);assert.equal(initial.conversation_id,null);assert.equal(initial.initial_cutoff,cut);assert.equal(initial.max_pages,10);assert.equal(initial.takeover_before,new Date(f.now-86400000).toISOString());
 rejects(()=>f.l.historyRequirements('another_owner',id),'event_not_accessible');f.prepare(id);const known=f.l.historyRequirements(owner,id);assert.equal(known.conversation_id,V);assert.equal(known.checkpoint_id,'in_1');assert.equal(known.baseline_id,'timezone');assert.equal(known.initial_cutoff,null);
});
test('same-time burst questions are all covered instead of freezing on ambiguous chronology',async t=>{
 const f=fixture(t),id=await f.event(),ms=[f.m('in_2',{body:'And what is setup?'}),f.m()];const p=f.prepare(id,f.snap(ms));assert.equal(p.prepared,true);assert.deepEqual(p.message_ids,['in_1','in_2']);
});

test('matching contact statement updates durable booking confirmation without changing the saved timezone',async t=>{
 const f=fixture(t),id=await f.event(),s=blank(f);s.reads[1].result.data.contact.timezone='America/Los_Angeles';f.prepare(id,s);
 assert.equal(f.l.read('prospect_entries',f.l.key(C)).timezone.bookingConfirmed,false);
 const statement=f.m('confirmation',{body:"I'm in Pacific time.",dateAdded:'2026-10-21T15:00:02.000Z'}),fresh=blank(f,[statement,f.m()]);fresh.reads[1].result.data.contact.timezone='America/Los_Angeles';f.prepare(id,fresh);
 const entry=f.l.read('prospect_entries',f.l.key(C));assert.equal(entry.timezone.bookingConfirmed,true);assert.equal(entry.timezone.messageId,'confirmation');assert.equal(entry.timezoneHistory.length,1);
 f.restart();assert.equal(f.l.read('prospect_entries',f.l.key(C)).timezone.bookingConfirmed,true);
});

