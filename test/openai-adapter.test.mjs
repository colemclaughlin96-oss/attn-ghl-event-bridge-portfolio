import test from 'node:test';
import assert from 'node:assert/strict';
import {createOpenAIAdapter} from '../src/openai-adapter.mjs';
import {LOCATION,CALENDAR,PRICING,priceAnswer} from '../src/sales-policy.mjs';

// Mocked HTTP only. These fixtures never use saved secrets or a paid API call.
const KEY='sk-synthetic-secret-must-not-leak',MODEL='configured-test-model';
const C='synthetic_contact',V='synthetic_conversation';
const msg=(id='in_1',extra={})=>({id,locationId:LOCATION,contactId:C,conversationId:V,direction:'inbound',dateAdded:'2026-10-21T15:00:01.000Z',body:'What does Google Ads cost?',messageType:'TYPE_CUSTOM_PROVIDER_SMS',attachments:[],...extra});
const read=(operationId,data,params={})=>({operationId,locationId:LOCATION,params,result:{operationId,status:200,success:true,data}});
function input(messages=[msg()]) {
  return {event:{data:{contact_id:C,location_id:LOCATION}},snapshot:{reads:[
    read('get-contact',{contact:{id:C,locationId:LOCATION,type:'lead',timezone:'America/Los_Angeles',email:'private@example.com',phone:'+12065551234',address1:'PRIVATE ADDRESS'}}),
    read('search-conversation',{total:1,conversations:[{id:V,contactId:C,locationId:LOCATION}]}),
    read('get-messages',{messages:{messages}},{path:{conversationId:V}}),
    read('get-appointments-for-contact',{events:[]}),
    read('list-transactions',{data:[{private:'PRIVATE PAYMENT'}]})
  ]},preparation:{message_id:messages.filter(m=>m.direction==='inbound').at(-1).id,message_ids:messages.filter(m=>m.direction==='inbound').map(m=>m.id)},systemPrompt:'You are Cole\'s assistant. Use only approved ATTN facts.',policyRevision:'a'.repeat(64)};
}
function draft(extra={}) {
  return {action:'reply',draft:{text:priceAnswer('google'),intent:'pricing',answersMessageId:'in_1',answersMessageIds:['in_1'],policyReviewed:true,untrustedContentIgnored:true,priceKey:'google',priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:true},buyingIntentVerified:null,...extra},handoff:null};
}
const output=(value,extra={})=>({status:'completed',output:[{type:'message',role:'assistant',status:'completed',content:[{type:'output_text',text:JSON.stringify(value)}]}],usage:{input_tokens:120,output_tokens:40,total_tokens:160,input_tokens_details:{cached_tokens:80},output_tokens_details:{reasoning_tokens:0}},...extra});
const json=(value,status=200)=>new Response(JSON.stringify(value),{status});
function harness(response=output(draft()),opts={}) {
  const calls=[],usage=[];
  const adapter=createOpenAIAdapter({apiKey:KEY,model:MODEL,recordUsage:async x=>usage.push(x),fetchImpl:async(url,options)=>{calls.push({url,options,body:JSON.parse(options.body)});return typeof response==='function'?response(url,options):json(response);},...opts});
  return {adapter,calls,usage};
}
const reason=(p,expected)=>assert.rejects(p,e=>{assert.equal(e.reason,expected);assert.equal(e.message,expected);assert.equal(JSON.stringify(e).includes(KEY),false);return true;});

test('Responses request uses strict schema, configured model, no storage or tools, and real completion status',async()=>{
  const {adapter,calls,usage}=harness();
  const result=await adapter.generate(input());
  assert.equal(result.finishReason,'completed');assert.equal(result.complete,true);assert.equal(result.draft.text,priceAnswer('google'));
  assert.equal(calls.length,1);const {url,options,body}=calls[0];
  assert.equal(url,'https://api.openai.com/v1/responses');assert.equal(options.method,'POST');assert.equal(options.redirect,'error');
  assert.equal(options.headers.Authorization,'Bearer '+KEY);assert.equal(body.model,MODEL);assert.equal(body.store,false);assert.equal(body.stream,false);assert.equal(body.max_output_tokens,650);
  assert.equal('tools' in body,false);assert.deepEqual(body.input.map(x=>x.role),['system','user']);
  assert.equal(body.text.format.type,'json_schema');assert.equal(body.text.format.strict,true);assert.equal(body.text.format.schema.type,'object');assert.equal('anyOf' in body.text.format.schema,false);
  const visit=x=>{if(!x||typeof x!=='object')return;if(x.type==='object'){assert.equal(x.additionalProperties,false);assert.deepEqual(x.required,Object.keys(x.properties));}for(const value of Object.values(x))visit(value);};visit(body.text.format.schema);
  assert.deepEqual(usage[0].usage,{input_tokens:120,output_tokens:40,total_tokens:160,cached_input_tokens:80,reasoning_output_tokens:0});
  assert.equal(JSON.stringify(usage).includes(KEY),false);assert.equal(JSON.stringify(adapter),'{}');
});

test('generation includes only scoped minimal contact context and redacts body contact details',async()=>{
  const h=harness();const x=input([msg('in_1',{body:'What does Google Ads cost? Contact private@example.com or +12065551234.',attachments:[{url:'https://private.example/file'}]})]);
  x.snapshot.reads.find(r=>r.operationId==='get-appointments-for-contact').result.data.events=[{id:'appt_1',contactId:C,locationId:LOCATION,calendarId:CALENDAR,appointmentStatus:'confirmed',startTime:'2026-10-23T12:00:00Z',endTime:'2026-10-23T12:30:00Z',notes:'PRIVATE APPOINTMENT NOTES',address:'PRIVATE JOIN LINK'}];
  await h.adapter.generate(x);const serialized=JSON.stringify(h.calls[0].body),context=JSON.parse(h.calls[0].body.input[1].content);
  for(const hidden of ['private@example.com','+12065551234','PRIVATE ADDRESS','PRIVATE PAYMENT','PRIVATE APPOINTMENT NOTES','PRIVATE JOIN LINK','https://private.example/file'])assert.equal(serialized.includes(hidden),false,hidden);
  assert.match(context.messages[0].text,/\[email\].*\[phone\]/);assert.equal(context.messages[0].attachmentCount,1);
  assert.deepEqual(context.contact,{type:'lead',timezone:'America/Los_Angeles'});assert.equal(context.appointments[0].id,'appt_1');
});

test('scope mismatches, mixed conversations and missing pending IDs fail before any request',async()=>{
  for(const mutate of [
    x=>x.event.data.location_id='another_location',
    x=>x.snapshot.reads[0].result.data.contact.id='another_contact',
    x=>x.snapshot.reads[1].result.data.conversations[0].contactId='another_contact',
    x=>x.snapshot.reads[2].result.data.messages.messages[0].locationId='another_location',
    x=>x.snapshot.reads[2].result.data.messages.messages.push(msg('in_2',{conversationId:'another_thread'})),
    x=>x.preparation.message_ids=['missing'],
    x=>x.snapshot.reads[3].result.data.events.push({id:'appt',contactId:'another_contact',locationId:LOCATION})
  ]){const h=harness(),x=input();mutate(x);await reason(h.adapter.generate(x),'model_context_scope');assert.equal(h.calls.length,0);}
});

test('generation receives verified booking restrictions so it can still answer unrelated questions',async()=>{
  const h=harness(),x=input();x.preparation.booking_restrictions=['active_other_calendar','multiple_active_bookings'];
  await h.adapter.generate(x);const context=JSON.parse(h.calls[0].body.input[1].content);
  assert.deepEqual(context.preparation.booking_restrictions,x.preparation.booking_restrictions);
  for(const invalid of [['unverified'],['multiple_active_bookings','multiple_active_bookings'],'none']){
    const bad=input();bad.preparation.booking_restrictions=invalid;const z=harness();
    await reason(z.adapter.generate(bad),'model_context_scope');assert.equal(z.calls.length,0);
  }
});

test('batch IDs are preserved exactly and model cannot omit an unanswered pending message',async()=>{
  const x=input([msg('in_1'),msg('in_2',{body:'How does it work?',dateAdded:'2026-10-21T15:00:02.000Z'})]);
  const good=draft({text:priceAnswer('google')+' We build and manage campaigns around the work you want more of.',answersMessageId:'in_2',answersMessageIds:['in_1','in_2']});
  assert.deepEqual((await harness(output(good)).adapter.generate(x)).draft.answersMessageIds,['in_1','in_2']);
  for(const ids of [['in_2'],['in_2','in_1'],['in_1','in_2','unknown']])await reason(harness(output(draft({answersMessageId:'in_2',answersMessageIds:ids}))).adapter.generate(x),'generation_schema_invalid');
});

test('strict output rejects unknown fields, false review flags, incorrect price facts and invented metadata',async()=>{
  const bads=[draft({untrustedContentIgnored:false}),draft({unexpected:'ignored?'}),draft({priceKey:{toString:null}}),draft({priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:'yes'}}),{...draft(),complete:true},draft({priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:false},text:'Google Ads is $597/month plus $750 setup. Ad spend is separate; all prices USD.'})];
  for(const value of bads)await assert.rejects(harness(output(value)).adapter.generate(input()),e=>['generation_schema_invalid','approved_price_facts_required'].includes(e.reason));
  await reason(harness(output(draft({text:'Google Ads is $1/month plus $750 setup. Ad spend is separate; all prices USD.'}))).adapter.generate(input()),'approved_price_facts_required');
});

const observedPrice='Google Ads management is $750 setup plus $597 for the first month ($1,347 upfront), then $597/month. Ad spend is separate.';
test('observed correct price missing currency gains the verified USD label without another model call',async()=>{
  const h=harness(output(draft({text:observedPrice}))),result=await h.adapter.generate(input());
  assert.equal(result.draft.text,observedPrice+' All prices are USD.');assert.equal(h.calls.length,1);
  assert.match(h.calls[0].body.input[0].content,/application adds the separate-ad-spend and USD disclosures/);
  assert.deepEqual(result.draft.priceFacts,{...PRICING.google,currency:'USD',adSpendAdditional:true});
  const explicit=observedPrice+' All prices are USD.';
  assert.equal((await harness(output(draft({text:explicit}))).adapter.generate(input())).draft.text,explicit);
});

test('currency label cannot repair incorrect price facts, amounts, missing spend disclosure or terms',async()=>{
  for(const changes of [
    {text:observedPrice,priceFacts:{...PRICING.google,initial:1346,currency:'USD',adSpendAdditional:true}},
    {text:observedPrice,priceFacts:null},
    {text:observedPrice,priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:false}},
    {text:observedPrice.replace(/597/g,'598')},
    {text:observedPrice.replace('Ad spend is separate.','')}
  ])await reason(harness(output(draft(changes))).adapter.generate(input()),'approved_price_facts_required');
  await reason(harness(output(draft({text:observedPrice+' A discount is included.'}))).adapter.generate(input()),'unapproved_price_terms');
  await reason(harness(output(draft({text:observedPrice,priceFacts:{...PRICING.google,currency:'CAD',adSpendAdditional:true}}))).adapter.generate(input()),'generation_schema_invalid');
});

test('foreign currency markers remain rejected instead of receiving a conflicting USD label',async()=>{
  for(const marker of ['CAD','AUD','EUR','GBP','Canadian dollars','Australian dollars','euros','British pounds','C$','A$','NZ$','€','£','¥','₹']){
    for(const usd of ['', ' All prices are USD.']){
      await reason(harness(output(draft({text:observedPrice+' Currency: '+marker+'.'+usd}))).adapter.generate(input()),'approved_price_facts_required');
    }
  }
});

test('adding the currency label preserves the complete multi-question answer and every pending ID',async()=>{
  const text=observedPrice+' Setup includes getting the campaign ready. Are you looking to grow roof cleaning?',ids=['in_1','in_2'];
  const x=input([msg('in_1'),msg('in_2',{body:'How does setup work?',dateAdded:'2026-10-21T15:00:02.000Z'})]);
  const h=harness(output(draft({text,answersMessageId:'in_2',answersMessageIds:ids}))),result=await h.adapter.generate(x);
  assert.equal(result.draft.text,text+' All prices are USD.');assert.equal(result.draft.answersMessageId,'in_2');assert.deepEqual(result.draft.answersMessageIds,ids);assert.equal(h.calls.length,1);
});

test('handoff is a structured outcome, never a claim that Cole has been notified',async()=>{
  const h=harness(output({action:'handoff',draft:null,handoff:{reason:'refund_requested',decision_needed:'Cole must answer the requested refund terms.'}}));
  assert.deepEqual(await h.adapter.generate(input()),{finishReason:'completed',complete:true,handoff:{reason:'refund_requested',decision_needed:'Cole must answer the requested refund terms.'}});
  assert.equal(h.calls[0].body.text.format.schema.properties.handoff.anyOf[0].properties.reason.pattern,'^[a-z][a-z0-9_]{0,99}$');
  await reason(harness(output({action:'handoff',draft:null,handoff:{reason:'not valid',decision_needed:'Review'}})).adapter.generate(input()),'generation_schema_invalid');
});

test('incomplete API status cannot become a successful draft even with valid JSON',async()=>{
  const h=harness(output(draft(),{status:'incomplete',incomplete_details:{reason:'max_output_tokens'}}));
  await reason(h.adapter.generate(input()),'generation_incomplete');assert.equal(h.calls.length,1);assert.equal(h.usage[0].status,'incomplete');assert.equal(h.usage[0].usage.total_tokens,160);
});

test('refusal, uncompleted response, invalid JSON, multi-part output and unexpected tool output fail closed',async()=>{
  const cases=[
    [output(draft(),{output:[{type:'message',content:[{type:'refusal',refusal:'No'}]}]}),'generation_refused'],
    [output(draft(),{status:'in_progress'}),'model_response_not_completed'],
    [output(draft(),{output:[{type:'message',content:[{type:'output_text',text:'{'}]}]}),'generation_schema_invalid'],
    [output(draft(),{output:[{type:'message',content:[{type:'output_text',text:'{}'},{type:'output_text',text:'{}'}]}]}),'model_response_invalid'],
    [output(draft(),{output:[{type:'function_call',name:'send_message',arguments:'{}'}]}),'model_unexpected_tool_output']
  ];
  for(const [body,expected] of cases){const h=harness(body);await reason(h.adapter.generate(input()),expected);assert.equal(h.calls.length,1);}
});

test('HTTP failures are sanitized, classified and never automatically retried',async()=>{
  for(const [status,expected] of [[429,'provider_rate_limited'],[401,'model_authentication_failed'],[403,'model_authentication_failed'],[500,'model_provider_unavailable'],[400,'model_request_rejected']]){
    const h=harness(()=>json({error:{message:KEY+' private@example.com'}},status));
    await reason(h.adapter.generate(input()),expected);assert.equal(h.calls.length,1);assert.equal(h.usage.length,1);assert.equal(h.usage[0].usage,null);assert.equal(h.usage[0].httpStatus,status);assert.equal(JSON.stringify(h.usage).includes(KEY),false);
  }
});

test('network errors and provider key echoes cannot expose secrets',async()=>{
  const h=harness(()=>{throw new Error(KEY+' https://secret.example')});
  await reason(h.adapter.generate(input()),'model_transport_failed');assert.equal(JSON.stringify(h.usage).includes(KEY),false);
  await reason(harness(output(draft({text:KEY}))).adapter.generate(input()),'model_response_invalid');
  const malformed=harness(()=>new Response(KEY,{status:200}));await reason(malformed.adapter.generate(input()),'model_response_invalid');
});

test('request timeout aborts once and records unknown usage rather than zero',async()=>{
  let signal;const h=harness((_url,options)=>{signal=options.signal;return new Promise(()=>{});},{timeoutMs:15});
  await reason(h.adapter.generate(input()),'model_timeout');assert.equal(signal.aborted,true);assert.equal(h.calls.length,1);assert.equal(h.usage[0].usage,null);assert.equal(h.usage[0].status,'unknown');
});

test('caller cancellation never starts a pre-cancelled request',async()=>{
  const c=new AbortController();c.abort();const h=harness();
  await reason(h.adapter.generate({...input(),signal:c.signal}),'model_request_cancelled');assert.equal(h.calls.length,0);assert.equal(h.usage.length,0);
});

test('caller cancellation promptly settles even if a broken transport ignores its signal',async()=>{
  const c=new AbortController();let captured;
  const h=harness((_url,options)=>{captured=options.signal;c.abort();return new Promise(()=>{});},{timeoutMs:10000});
  await reason(h.adapter.generate({...input(),signal:c.signal}),'model_request_cancelled');assert.equal(captured.aborted,true);assert.equal(h.usage[0].usage,null);assert.equal(h.calls.length,1);
});

test('Luna reasoning none is explicit, while unverified overrides fail configuration',async()=>{
  const h=harness(output(draft()),{model:'gpt-6-luna',reasoningEffort:'none'});await h.adapter.generate(input());assert.deepEqual(h.calls[0].body.reasoning,{effort:'none'});
  for(const opts of [{model:'gpt-6-astra',reasoningEffort:'none'},{model:'gpt-6-luna',reasoningEffort:'minimal'}])assert.throws(()=>createOpenAIAdapter({apiKey:KEY,...opts}),e=>e.reason==='openai_reasoning_configuration_invalid');
});

test('budget authorization precedes every paid request and usage carries only reservation metadata',async()=>{
  const authorizations=[];const h=harness(output(draft()),{allowRequest:async x=>{authorizations.push(x);return {allowed:true,reservationId:'reservation_1'};}});
  await h.adapter.generate(input());await h.adapter.generate(input());
  assert.equal(authorizations.length,2);assert.equal(h.calls.length,2);assert.equal(authorizations[0].purpose,'generate');assert.equal(authorizations[0].maxOutputTokens,650);assert.ok(authorizations[0].inputBytes>0);
  assert.equal(authorizations[0].inputBytes,Buffer.byteLength(h.calls[0].options.body));
  assert.equal(h.usage[0].reservationId,'reservation_1');assert.equal('messages' in h.usage[0],false);assert.equal(JSON.stringify(authorizations).includes(KEY),false);
  const blocked=harness(output(draft()),{allowRequest:async()=>false});await reason(blocked.adapter.generate(input()),'model_budget_exhausted');assert.equal(blocked.calls.length,0);assert.equal(blocked.usage.length,0);
});

test('budget hooks fail closed without leaking their underlying exception',async()=>{
  const denied=harness(output(draft()),{allowRequest:async()=>{throw new Error(KEY);}});await reason(denied.adapter.generate(input()),'model_budget_hook_failed');assert.equal(denied.calls.length,0);
  const lost=harness(output(draft()),{recordUsage:async()=>{throw new Error(KEY);}});await reason(lost.adapter.generate(input()),'model_usage_record_failed');assert.equal(lost.calls.length,1);
});

test('assessment scopes pending questions but preserves explicit old opt-out evidence',async()=>{
  const messages=[msg('old_stop',{body:'STOP',dateAdded:'2026-10-20T15:00:01.000Z'}),msg('old_question',{body:'Can I speak to Cole?',dateAdded:'2026-10-20T15:01:01.000Z'}),msg('handled',{body:'Can I have a discount?'}),msg('new',{body:'Can I speak to Cole?',dateAdded:'2026-10-21T15:00:02.000Z'})];
  const h=harness(output({assessments:Object.fromEntries(messages.map(m=>[m.id,'handoff']))}));
  const rows=await h.adapter.assess({messages,contactId:C,notBefore:'2026-10-21T15:00:00.000Z',handledMessageIds:['handled']});
  assert.deepEqual(rows,[{messageId:'old_stop',kind:'opt_out'},{messageId:'old_question',kind:'normal'},{messageId:'handled',kind:'normal'},{messageId:'new',kind:'handoff'}]);
  assert.deepEqual(JSON.parse(h.calls[0].body.input[1].content).messages.map(m=>m.pending),[false,false,false,true]);
});

test('assessment context retains quotations and instructs unfamiliar service clarification without unsafe label',async()=>{
  const messages=[msg('question',{body:'How do I stop wasting ad spend?'}),msg('quoted',{body:'My lead replied:\nSTOP\nWhat should I do?'}),msg('unfamiliar',{body:'I do zoop cleaning'}),msg('out',{direction:'outbound',body:'Which service do you want more work for?'})];
  const h=harness(output({assessments:Object.fromEntries(messages.filter(m=>m.direction==='inbound').map(m=>[m.id,'normal']))}));
  assert.ok((await h.adapter.assess({messages,contactId:C})).every(x=>x.kind==='normal'));
  const prompt=h.calls[0].body.input[0].content;assert.match(prompt,/unfamiliar service phrase alone is normal/);assert.match(prompt,/quoted examples/);
  assert.equal(JSON.parse(h.calls[0].body.input[1].content).messages.at(-1).direction,'outbound');
});

test('assessment rejects invented, missing, duplicate, outbound or unsupported kind results',async()=>{
  const messages=[msg(),msg('out',{direction:'outbound'})];
  for(const assessments of [{},{not_in_thread:'normal'},{out:'normal'},{in_1:'guessed'},{in_1:{kind:'normal'}},{in_1:'normal',out:'normal'},[{messageId:'in_1',kind:'normal'},{messageId:'in_1',kind:'normal'}]]){
    await reason(harness(output({assessments})).adapter.assess({messages,contactId:C}),'assessment_schema_invalid');
  }
});

test('assessment cache avoids duplicate same-snapshot costs and changes invalidate it',async()=>{
  let now=1000;const h=harness((_url,options)=>{const context=JSON.parse(JSON.parse(options.body).input[1].content);return json(output({assessments:Object.fromEntries(context.messages.filter(m=>m.direction==='inbound').map(m=>[m.id,'normal']))}));},{now:()=>now});
  const first=await h.adapter.assess({messages:[msg()],contactId:C});first[0].kind='opt_out';
  assert.equal((await h.adapter.assess({messages:[msg()],contactId:C}))[0].kind,'normal');assert.equal(h.calls.length,1);
  await h.adapter.assess({messages:[msg(),msg('new')],contactId:C});assert.equal(h.calls.length,2);
  await h.adapter.assess({messages:[msg()],contactId:C,handledMessageIds:['in_1']});assert.equal(h.calls.length,3);
  now+=60001;await h.adapter.assess({messages:[msg()],contactId:C});assert.equal(h.calls.length,4);
});

test('assessment cannot receive another contact or oversized content',async()=>{
  const h=harness();
  await reason(h.adapter.assess({messages:[msg('other',{contactId:'other'})],contactId:C}),'model_context_scope');
  await reason(h.adapter.assess({messages:[msg('long',{body:'x'.repeat(12001)})],contactId:C}),'model_context_limit');assert.equal(h.calls.length,0);
  assert.deepEqual(await h.adapter.assess({messages:[],contactId:C}),[]);
});

test('assessment schema itself requires every exact inbound identity and forbids other identities',async()=>{
  const messages=[msg('in_first'),msg('out',{direction:'outbound'}),msg('in_second')];
  const h=harness(output({assessments:{in_second:'normal',in_first:'normal'}}));
  assert.deepEqual(await h.adapter.assess({messages,contactId:C}),[{messageId:'in_first',kind:'normal'},{messageId:'in_second',kind:'normal'}]);
  const schema=h.calls[0].body.text.format.schema.properties.assessments;
  assert.equal(schema.type,'object');assert.deepEqual(schema.required,['in_first','in_second']);assert.equal(schema.additionalProperties,false);
  assert.deepEqual(schema.properties.in_first.enum,['normal','opt_out','system','handoff','unsafe']);
});

test('common cost question renders approved amounts and preserves a natural qualifying question',async()=>{
  const h=harness(output(draft({text:'{{price:single}}. Are you looking at one or both?',priceKey:null,priceFacts:null})));
  const r=await h.adapter.generate(input());
  assert.match(r.draft.text,/Google or Facebook Ads is \$597\/month plus \$750 setup/);
  assert.match(r.draft.text,/\$1,347 upfront/);assert.match(r.draft.text,/Ad spend is separate/);assert.match(r.draft.text,/USD\. Are you looking at one or both\?$/);
  assert.equal(r.draft.priceKey,'google');assert.equal(r.draft.priceQuotes.length,1);assert.equal(h.calls.length,1);
});

test('comparison uses independently verified application prices, not the single-package amount allowlist',async()=>{
  const h=harness(output(draft({text:'{{price:single}}. {{price:both}}',priceKey:null,priceFacts:null})));
  const r=await h.adapter.generate(input());assert.equal(r.draft.priceQuotes.length,2);assert.match(r.draft.text,/\$997\/month plus \$1,199 setup \(\$2,196 upfront\)/);
  assert.equal(r.draft.answersMessageId,'in_1');
});

test('pricing placeholders never fix conflicting facts or unapproved amounts',async()=>{
  for(const changes of [
    {text:'{{price:google}}. Only $1 upfront',priceKey:null,priceFacts:null},
    {text:'{{price:both}}',priceKey:'google'},
    {text:'{{price:google}}',priceFacts:{...PRICING.google,monthly:1,currency:'USD',adSpendAdditional:true}},
    {text:'{{price:unknown}}',priceKey:null,priceFacts:null},
    {text:'{{price:google}}',intent:'answer',priceKey:null,priceFacts:null}
  ])await reason(harness(output(draft(changes))).adapter.generate(input()),'approved_price_facts_required');
});

test('configuration requires an explicit model and API key',()=>{
  for(const options of [{model:MODEL},{apiKey:KEY},{apiKey:KEY,model:'bad model'},{apiKey:KEY+'\n',model:MODEL}])assert.throws(()=>createOpenAIAdapter(options),e=>['openai_api_key_required','openai_model_required'].includes(e.reason));
});

