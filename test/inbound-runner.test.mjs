import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,owner} from './helpers/prospect-fixture.mjs';
import {InboundRunner,collectEvidence,bounded} from '../src/inbound-runner.mjs';
import {priceAnswer,PRICING,validatePriceDraft,validateGeneration,BOOKING_LINK} from '../src/sales-policy.mjs';

function harness(t,{messages,mutate,generate,send,verify,notify,snapshot,limits={}}={}){
  const f=fixture(t);let current=messages?.(f)??[f.m()],sent=[],generations=0,reads=0;
  const defaultDraft=p=>({finishReason:'stop',complete:true,draft:{...f.draft,priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:true},answersMessageId:p.message_id,answersMessageIds:p.message_ids}});
  const adapters={
    snapshot:async args=>{
      reads++;if(snapshot)return snapshot(args,{f,current,reads});
      const s=f.snap(current,{detail:args.candidateId?current.find(m=>m.id===args.candidateId):null});
      if(mutate)mutate(s,{reads,f});return s;
    },
    generate:async args=>{generations++;return generate?generate(args,{f,setMessages:m=>current=m,current,generations,defaultDraft}):defaultDraft(args.preparation);},
    send:async p=>{
      sent.push(p);
      if(send)return send(p,{f,setMessages:m=>current=m,current});
      return {messageId:'synthetic_out',conversationId:'synthetic_thread',text:p.params.body.message};
    },
    verify:async(s,{permit})=>verify?verify(s,{permit,f}):({outcome:'sent',send_response:s,outbound:f.m(s.messageId,{direction:'outbound',status:'sent',body:s.text,dateAdded:new Date(f.now).toISOString()})}),
    notify
  };
  const runner=new InboundRunner({bridge:f.b,owner,adapters,now:()=>f.now,sleep:async()=>{},limits});
  return {f,runner,sent,get generations(){return generations;},get reads(){return reads;},run:async()=>runner.run(await f.event('runner_event',C,false))};
}
function unknownTimezone(s){delete s.reads.find(r=>r.operationId==='get-contact').result.data.contact.timezone;s.reads.find(r=>r.operationId==='get-messages').result.data.messages.messages=s.reads.find(r=>r.operationId==='get-messages').result.data.messages.messages.filter(m=>m.id!=='timezone');}
test('unknown/invalid timezone and international numbers answer pricing',async t=>{
  for(const timezone of [undefined,'not/a_zone']){
    const h=harness(t,{mutate:s=>{unknownTimezone(s);const c=s.reads.find(r=>r.operationId==='get-contact').result.data.contact;c.phone='+442079460018';c.country='GB';if(timezone)c.timezone=timezone;}});
    const r=await h.run();assert.equal(r.category,'replied');assert.equal(r.delivery,'sent');assert.equal(h.sent.length,1);assert.match(h.sent[0].params.body.message,/\$597/);
  }
});
test('unknown timezone booking responds with a useful scheduling question',async t=>{
 const h=harness(t,{messages:f=>[f.m('in_1',{body:'Can I book?'} )],mutate:unknownTimezone,generate:async({preparation:p},{defaultDraft})=>{const r=defaultDraft(p);r.draft={...r.draft,intent:'answer',text:'Sure. What timezone are you in?'};return r;}});
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.sent[0].params.body.message,'Sure. What timezone are you in?');
});
test('rapid messages share one reply covering every pending ID',async t=>{
 const h=harness(t,{messages:f=>[f.m('in2',{body:'And how does it work?',dateAdded:'2026-10-21T15:00:02.000Z'}),f.m()],generate:async({preparation:p},{defaultDraft})=>{const r=defaultDraft(p);r.draft.text+=' We build and manage campaigns around the work you want more of.';return r;}});
 const r=await h.run();assert.equal(r.category,'replied');assert.deepEqual(r.coveredMessageIds,['in_1','in2']);assert.equal(h.sent.length,1);assert.equal(h.sent[0].params.body.replyMessageId,'in2');assert.equal(h.f.l.rows(C).filter(r=>r.phase==='sent').length,2);
});
test('new question during generation refreshes before one send and answers both',async t=>{
 const h=harness(t,{generate:async({preparation:p},{f,current,setMessages,generations,defaultDraft})=>{
  if(generations===1)setMessages([f.m('in2',{body:'Can I book too?',dateAdded:'2026-10-21T15:00:02.000Z'}),...current]);
  const r=defaultDraft(p);if(generations===2)r.draft.text+=' What timezone are you in?';return r;
 }});
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.generations,2);assert.equal(h.sent.length,1);assert.deepEqual(r.coveredMessageIds,['in_1','in2']);assert.ok(r.trace.some(t=>t.reason==='thread_changed'));
});
test('complete longer answer is preserved; output is never sliced',async t=>{
 const answer=priceAnswer('google')+' The free review takes thirty minutes. Cole looks at your goals, service area, website and advertising, then discusses practical next steps. You can join from a computer to share your screen. No purchase is required, and you do not need to provide passwords or account access.';
 const h=harness(t,{generate:async({preparation:p},{defaultDraft})=>{const r=defaultDraft(p);r.draft.text=answer;return r;}});
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.sent[0].params.body.message,answer);
});
test('truncated generation is repaired once before any permit; repeated failure is visible',async t=>{
 for(const permanent of [false,true]){
  const h=harness(t,{generate:async({preparation:p},{generations,defaultDraft})=>generations===1||permanent?{finishReason:'length',complete:false,draft:{text:'Google Ads is'}}:defaultDraft(p)});
  const r=await h.run();assert.equal(h.generations,2);assert.equal(h.sent.length,permanent?0:1);assert.equal(r.category,permanent?'deferred':'replied');if(permanent){assert.equal(r.reason,'generation_incomplete');assert.equal(r.recovery.deferred,true);}
 }
});
test('manual reply or STOP arriving before send prevents queued bot interruption',async t=>{
 for(const mode of ['manual','stop']){
  const h=harness(t,{mutate:(s,{reads,f})=>{if(reads>1){const m=f.m('late',{body:mode==='stop'?'STOP':'Cole here.',direction:mode==='stop'?'inbound':'outbound',source:'api',dateAdded:'2026-10-21T15:00:03.000Z'});s.reads.find(r=>r.operationId==='get-messages').result.data.messages.messages.unshift(m);if(mode==='stop')s.assessments.push({messageId:'late',kind:'opt_out'});}}});
  const r=await h.run();assert.equal(h.sent.length,0);assert.ok(['suppressed','handoff'].includes(r.category));assert.ok(r.trace.some(x=>/opt_out|external_sender/.test(x.reason)));
 }
});
test('reaction/provider system events do not generate customer replies',async t=>{
 const h=harness(t,{mutate:s=>{s.assessments=s.assessments.map(a=>({...a,kind:'system'}));}});
 const r=await h.run();assert.equal(h.sent.length,0);assert.equal(h.generations,0);assert.equal(r.reason,'ignored_system_events');assert.equal(h.reads,1);
});
test('invisible provider message is retried in same run before replying',async t=>{
 const h=harness(t,{snapshot:async({candidateId},{f,reads})=>f.snap(reads===1?[]:[f.m()],{detail:candidateId?f.m():null})});
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.reads,3);assert.equal(h.sent.length,1);
});
test('model failure and tool timeout are visible and never create permanent human holds',async t=>{
 const h=harness(t,{generate:async()=>{throw new Error('provider down');}});
 const r=await h.run();assert.equal(r.category,'deferred');assert.equal(r.reason,'adapter_failure');assert.equal(r.recovery.deferred,true);assert.equal(h.f.l.contactState(h.f.l.read('prospect_entries',h.f.l.key(C))).handoff,null);
 const g=harness(t,{snapshot:()=>new Promise(()=>{}),limits:{readMs:5,modelMs:5}});
 const q=await g.run();assert.equal(q.reason,'evidence_timeout');assert.equal(g.sent.length,0);assert.ok(g.f.b.status(owner).recent_processing.length);
});

test('initial and preflight evidence allow read plus classification time beyond an individual read cap',async t=>{
 const h=harness(t,{limits:{readMs:5,modelMs:60,budgetMs:250},snapshot:async({candidateId,signal},{f,current})=>{
  await new Promise(resolve=>setTimeout(resolve,20));assert.equal(signal.aborted,false);
  return f.snap(current,{detail:candidateId?current.find(m=>m.id===candidateId):null});
 }});
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.reads,2);assert.equal(h.sent.length,1);
});

test('combined evidence allowance still aborts at the remaining total processing budget',async t=>{
 let aborted=false;
 const h=harness(t,{limits:{readMs:10,modelMs:40,budgetMs:100},generate:async({preparation:p},{f,defaultDraft})=>{f.advance(95);return defaultDraft(p);},snapshot:async({candidateId,signal},{f,current})=>{
  if(!candidateId)return f.snap(current);
  signal.addEventListener('abort',()=>{aborted=true;},{once:true});return new Promise(()=>{});
 }});
 const r=await h.run();assert.equal(r.reason,'preflight_timeout');assert.equal(r.category,'deferred');assert.equal(aborted,true);assert.equal(h.sent.length,0);
});
test('unknown consent defers the same event and verified evidence resumes without a sticky opt-out',async t=>{
 let unknown=true;
 const h=harness(t,{mutate:s=>{if(unknown)delete s.reads.find(r=>r.operationId==='get-contact').result.data.contact.dnd;}});
 const id=await h.f.event('consent_retry',C,false);
 const first=await h.runner.run(id);
 assert.equal(first.category,'deferred');assert.equal(first.reason,'consent_evidence_unknown');assert.equal(first.recovery.deferred,true);
 assert.equal(h.generations,0);assert.equal(h.sent.length,0);
 const state=h.f.l.contactState(h.f.l.read('prospect_entries',h.f.l.key(C)));
 assert.equal(state.suppressed,false);assert.equal(state.handoff,null);
 unknown=false;h.f.advance(1001);
 const second=await h.runner.run(id,{recover:true});
 assert.equal(second.category,'replied');assert.equal(h.sent.length,1);
 const duplicate=await h.runner.run(id,{recover:true});assert.equal(duplicate.category,'suppressed');assert.equal(h.sent.length,1);
});
test('send timeout remains consumed and cannot be resent by a duplicate event',async t=>{
 const h=harness(t,{send:()=>new Promise(()=>{}),limits:{sendMs:5}});
 const r=await h.run();assert.equal(r.delivery,'unknown');assert.equal(r.reconciliationRequired,true);assert.equal(h.sent.length,1);
 assert.ok(h.f.l.rows(C).every(row=>row.outboundId===undefined&&row.sendResponseRecorded!==true));
 const id=await h.f.event('later',C,false),again=await h.runner.run(id);assert.equal(h.sent.length,1);assert.equal(again.reason,'prior_send_unresolved_or_failed');
});

test('accepted send identity survives temporary verification failure for read-only reconciliation',async t=>{
 for(const failure of ['not_visible','timeout','unknown_status']){
  const h=harness(t,{
   messages:f=>[f.m('in2',{body:'How does it work?',dateAdded:'2026-10-21T15:00:02.000Z'}),f.m()],
   generate:async({preparation:p},{defaultDraft})=>{const r=defaultDraft(p);r.draft.text+=' We build and manage campaigns around the work you want more of.';return r;},
   verify:async()=>{if(failure==='timeout')return new Promise(()=>{});if(failure==='unknown_status')return {outcome:'unknown'};throw Object.assign(new Error('not visible'),{reason:'ghl_exact_message_not_found'});},
   limits:{readMs:5}
  });
  const result=await h.run();assert.equal(result.delivery,'unknown');assert.equal(result.reconciliationRequired,true);assert.equal(h.sent.length,1);
  const rows=h.f.l.rows(C).filter(row=>row.attemptId);assert.equal(rows.length,2);
  for(const row of rows){assert.equal(row.phase,'attempt_unknown');assert.equal(row.outboundId,'synthetic_out');assert.equal(row.sendResponseRecorded,true);assert.equal(row.sendResponseConversationId,'synthetic_thread');}
  const later=await h.f.event('verify_delayed_'+failure,C,false);await h.runner.run(later);assert.equal(h.sent.length,1);
 }
});

test('invalid send response identity cannot become a recoverable outbound receipt',async t=>{
 let verified=0;
 const h=harness(t,{send:async()=>({messageId:'synthetic_out',conversationId:'another_contact_thread'}),verify:async()=>{verified++;return {outcome:'sent'};}});
 const result=await h.run();assert.equal(result.category,'failed');assert.equal(result.reconciliationRequired,true);assert.equal(h.sent.length,1);assert.equal(verified,0);
 assert.ok(h.f.l.rows(C).every(row=>row.outboundId===undefined&&row.sendResponseRecorded!==true));
});
test('provider accepted status is not reported as phone delivery',async t=>{
 for(const status of ['queued','sent','delivered','failed']){
  const h=harness(t,{verify:async(s,{f})=>({outcome:status,send_response:s,outbound:f.m(s.messageId,{direction:'outbound',status,body:s.text,dateAdded:new Date(f.now).toISOString()})})});
  const r=await h.run();assert.equal(r.delivery,status);assert.equal(h.sent.length,1);assert.equal(r.category,status==='failed'?'failed':'replied');
 }
});
test('existing booking answers preparation; an actual conflicting-meeting request can hand off',async t=>{
 for(const count of [1,2]){
  const h=harness(t,{messages:f=>[f.m('in_1',{body:count===1?'What should I bring to the review?':'I see two meetings. Which one should I join?'})],mutate:s=>{s.reads.find(r=>r.operationId==='get-appointments-for-contact').result.data.events=Array.from({length:count},(_,i)=>({id:'apt'+i,locationId:'portfolio_fixture_01',contactId:C,calendarId:'portfolio_fixture_03',appointmentStatus:'confirmed'}));},generate:async({preparation:p},{defaultDraft})=>{if(p.booking_restrictions?.length)return {finishReason:'stop',complete:true,handoff:{reason:'conflicting_meeting_request',decision_needed:'Cole to confirm which of the two meetings the prospect should join.'}};const r=defaultDraft(p);r.draft={...r.draft,intent:'existing_meeting',text:'Joining from a computer is helpful for screen sharing. Bring any questions you have.'};return r;},notify:async()=>({success:true,notificationId:'cloud_alert_1'})});
  const r=await h.run();assert.equal(r.category,count===1?'replied':'handoff');assert.equal(h.sent.length,count===1?1:0);if(count===2)assert.equal(r.notificationVerified,true);
 }
});
test('multiple valid appointments do not prevent an unrelated pricing answer or create a human hold',async t=>{
 const h=harness(t,{mutate:s=>{s.reads.find(r=>r.operationId==='get-appointments-for-contact').result.data.events=['a','b'].map(id=>({id,locationId:'portfolio_fixture_01',contactId:C,calendarId:'portfolio_fixture_03',appointmentStatus:'confirmed'}));}});
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.sent.length,1);assert.match(h.sent[0].params.body.message,/\$597/);
 assert.equal(h.f.l.contactState(h.f.l.read('prospect_entries',h.f.l.key(C))).handoff,null);
});
test('booking conflict appearing at preflight regenerates a safe answer before sending',async t=>{
 const h=harness(t,{
  messages:f=>[f.m('in_1',{body:'Can I book a demo?'})],
  mutate:(s,{reads})=>{if(reads>1)s.reads.find(r=>r.operationId==='get-appointments-for-contact').result.data.events=['a','b'].map(id=>({id,locationId:'portfolio_fixture_01',contactId:C,calendarId:'portfolio_fixture_03',appointmentStatus:'confirmed'}));},
  generate:async({preparation:p},{defaultDraft})=>{const r=defaultDraft(p);r.draft={...r.draft,intent:p.booking_restrictions?.length?'answer':'booking_link',text:p.booking_restrictions?.length?'I see two bookings. Which meeting did you mean?':BOOKING_LINK};return r;}
 });
 const r=await h.run();assert.equal(r.category,'replied');assert.equal(h.generations,2);assert.equal(h.sent.length,1);assert.equal(h.sent[0].params.body.message,'I see two bookings. Which meeting did you mean?');
 assert.ok(r.trace.some(x=>x.reason==='booking_action_requires_review'));assert.equal(h.f.l.contactState(h.f.l.read('prospect_entries',h.f.l.key(C))).handoff,null);
});
test('handoff success and failure are recorded truthfully; no false customer acknowledgment',async t=>{
 for(const success of [true,false]){
  const h=harness(t,{generate:async()=>({finishReason:'stop',complete:true,handoff:{reason:'owner_requested',decision_needed:'Cole to answer custom territory question.'}}),notify:async alert=>{assert.ok(alert.exact_question);return {success,notificationId:success?'receipt_1':null};}});
  const r=await h.run();assert.equal(r.notificationVerified,success);assert.equal(r.reason,success?'handoff_notified':'handoff_failed');assert.equal(h.sent.length,0);
 }
});
test('pricing facts permit short Cole phrasing and reject invented amounts',()=>{
 const d={priceKey:'google',text:'Google Ads is $597/month plus $750 setup. Ad spend is separate. All prices are USD.',priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:true}};
 assert.equal(validatePriceDraft(d),true);assert.throws(()=>validatePriceDraft({...d,text:d.text.replace('$597','$497')}));
 assert.throws(()=>validateGeneration({finishReason:'length',complete:true,draft:{text:'Half an answer'}}));
});

test('a message arriving while another send is in flight is recovered exactly once',async t=>{
 let laterId,laterResult,number=0;
 const h=harness(t,{
  generate:async({preparation:p},{defaultDraft})=>{
   const r=defaultDraft(p);
   if(p.message_id==='new_booking')r.draft={...r.draft,intent:'answer',text:'Sure. What timezone are you in?'};
   return r;
  },
  send:async(p,{f,setMessages,current})=>{
   number++;
   if(number===1){
    setMessages([f.m('new_booking',{body:'Can I book?',dateAdded:'2026-10-21T15:00:03.000Z'}),...current]);
    laterId=await f.event('during_send',C,false);
    laterResult=await h.runner.run(laterId);
   }
   return {messageId:'synthetic_out_'+number,conversationId:'synthetic_thread',text:p.params.body.message};
  }
 });
 const first=await h.run();assert.equal(first.category,'replied');assert.equal(laterResult.category,'deferred');assert.equal(laterResult.recovery.deferred,true);assert.equal(h.sent.length,1);
 h.f.advance(1001);
 const second=await h.runner.run(laterId,{recover:true});
 assert.equal(second.category,'replied');assert.deepEqual(second.coveredMessageIds,['new_booking']);assert.equal(h.sent.length,2);assert.equal(h.sent[1].params.body.message,'Sure. What timezone are you in?');
 const duplicate=await h.runner.run(laterId,{recover:true});assert.equal(duplicate.category,'suppressed');assert.equal(h.sent.length,2);
});
test('thread refresh followed by a tool failure defers an unconsumed attempt',async t=>{
 const h=harness(t,{
  generate:async({preparation:p},{f,current,setMessages,defaultDraft})=>{setMessages([f.m('arrived',{dateAdded:'2026-10-21T15:00:02.000Z',body:'How does it work?'}),...current]);return defaultDraft(p);},
  snapshot:async({candidateId},{f,current,reads})=>{if(reads>=3)throw new Error('temporary');return f.snap(current,{detail:candidateId?current.find(m=>m.id===candidateId):null});}
 });
 const r=await h.run();assert.equal(r.category,'deferred');assert.equal(r.recovery.deferred,true);assert.equal(r.reconciliationRequired,false);assert.equal(h.sent.length,0);assert.ok(h.f.l.rows(C).every(r=>!r.attemptId));
});
test('persistent model failure exhausts bounded recovery with an internal reason',async t=>{
 const h=harness(t,{generate:async()=>{throw new Error('offline');}});
 const id=await h.f.event('persistent_failure',C,false);
 let r=await h.runner.run(id);assert.equal(r.recovery.retry_number,1);
 h.f.advance(1001);r=await h.runner.run(id,{recover:true});assert.equal(r.recovery.retry_number,2);
 h.f.advance(1001);r=await h.runner.run(id,{recover:true});assert.equal(r.category,'failed');assert.equal(r.recovery.exhausted,true);assert.equal(r.recovery.reason,'processing_deferral_exhausted');assert.equal(h.generations,6);assert.equal(h.sent.length,0);
});

test('malformed classifier output defers before a permit and the same event can recover once',async t=>{
 for(const reason of ['model_response_invalid','assessment_schema_invalid']){
  let malformed=true;
  const h=harness(t,{snapshot:async({candidateId},{f,current})=>{if(malformed)throw Object.assign(new Error(reason),{reason});return f.snap(current,{detail:candidateId?current.find(m=>m.id===candidateId):null});}}),id=await h.f.event('classifier_'+reason,C,false);
  const first=await h.runner.run(id);assert.equal(first.category,'deferred');assert.equal(first.reason,reason);assert.equal(first.recovery.retry_number,1);assert.equal(h.generations,0);assert.equal(h.sent.length,0);assert.equal(h.f.l.rows(C).length,0);
  malformed=false;h.f.advance(1001);const recovered=await h.runner.run(id,{recover:true});assert.equal(recovered.category,'replied');assert.equal(h.sent.length,1);assert.equal(h.f.l.contactState(h.f.l.read('prospect_entries',h.f.l.key(C))).handoff,null);
  await h.runner.run(id,{recover:true});assert.equal(h.sent.length,1);
 }
});

test('persistent classifier errors exhaust existing recovery while scope/rate errors never auto retry',async t=>{
 for(const reason of ['model_response_invalid','assessment_schema_invalid']){
  const h=harness(t,{snapshot:async()=>{throw Object.assign(new Error(reason),{reason});}}),id=await h.f.event('classifier_exhaust_'+reason,C,false);
  assert.equal((await h.runner.run(id)).recovery.retry_number,1);h.f.advance(1001);assert.equal((await h.runner.run(id,{recover:true})).recovery.retry_number,2);h.f.advance(1001);
  const last=await h.runner.run(id,{recover:true});assert.equal(last.category,'failed');assert.equal(last.recovery.exhausted,true);assert.equal(h.reads,3);assert.equal(h.sent.length,0);assert.equal(h.generations,0);
 }
 for(const reason of ['model_context_scope','provider_rate_limited','model_authentication_failed','model_budget_exhausted']){
  const h=harness(t,{snapshot:async()=>{throw Object.assign(new Error(reason),{reason});}}),r=await h.run();assert.equal(r.category,'failed');assert.equal(r.recovery,undefined);assert.equal(h.reads,1);assert.equal(h.sent.length,0);
 }
});
test('evidence collector parallelizes reads but keeps scope, paging and preflight detail',async()=>{
 const calls=[],started=[],releases=[];let active=0,max=0;
 const execute=async({operationId,locationId,params})=>{
  assert.equal(locationId,'portfolio_fixture_01');calls.push({operationId,params});max=Math.max(max,++active);await new Promise(r=>setTimeout(r,2));active--;
  return {operationId,status:200,success:true,data:operationId==='search-conversation'?{total:1,conversations:[{id:'thread',contactId:'contact',locationId}]}:operationId==='get-messages'?{messages:{messages:[{id:'latest',dateAdded:new Date().toISOString()}],nextPage:false,lastMessageId:'latest'}}:{}};
 };
 const s=await collectEvidence({execute,assess:async()=>[],contactId:'contact',candidateId:'latest'});
 assert.equal(s.reads.length,7);assert.ok(max>=4);assert.deepEqual(calls.find(c=>c.operationId==='get-message').params,{path:{id:'latest'}});
 await assert.rejects(collectEvidence({execute:async({operationId})=>({operationId,status:429,success:false}),assess:async()=>[],contactId:'contact'}),e=>e.reason==='provider_rate_limited');
});

test('budget exhaustion before a send is recoverable and supplies cached current instructions',async t=>{
 const h=harness(t,{generate:async({preparation:p,systemPrompt,policyRevision},{f,generations,defaultDraft})=>{
  assert.match(systemPrompt,/597/);assert.match(systemPrompt,/timezone/i);assert.match(policyRevision,/^[a-f0-9]{64}$/);
  if(generations===1)f.advance(45001);
  return defaultDraft(p);
 }});
 const id=await h.f.event('budget',C,false);
 const first=await h.runner.run(id);assert.equal(first.category,'deferred');assert.equal(first.reason,'processing_budget_exhausted');assert.equal(first.recovery.deferred,true);assert.equal(h.sent.length,0);
 h.f.advance(1001);const second=await h.runner.run(id,{recover:true});assert.equal(second.category,'replied');assert.equal(h.sent.length,1);
});

test('direct transport failures retain a recoverable event before any send',async t=>{
 for(const reason of ['ghl_network_failure','ghl_request_timeout','ghl_request_aborted','model_transport_failed','model_provider_unavailable']){
  const h=harness(t,{snapshot:async()=>{throw Object.assign(new Error(reason),{reason});}});
  const r=await h.run();assert.equal(r.category,'deferred',reason);assert.equal(r.recovery.deferred,true,reason);assert.equal(h.sent.length,0);
 }
 const h=harness(t,{snapshot:async()=>{throw Object.assign(new Error('server down'),{reason:'ghl_http_error',status:503});}});
 const r=await h.run();assert.equal(r.recovery.deferred,true);assert.equal(r.reason,'ghl_provider_unavailable');
});

test('rate limits, invalid credentials and exhausted budget do not repeat generation',async t=>{
 for(const reason of ['provider_rate_limited','model_authentication_failed','model_budget_exhausted','model_budget_hook_failed','model_usage_record_failed']){
  const h=harness(t,{generate:async()=>{throw Object.assign(new Error(reason),{reason});}});
  const r=await h.run();assert.equal(r.category,'failed',reason);assert.equal(h.generations,1,reason);assert.equal(h.sent.length,0);assert.equal(r.recovery,undefined);
 }
});

