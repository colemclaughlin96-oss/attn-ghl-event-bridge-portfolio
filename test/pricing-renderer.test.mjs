import test from 'node:test';
import assert from 'node:assert/strict';
import {PRICING,validatePriceDraft} from '../src/sales-policy.mjs';
import {renderPricingDraft,validatePriceQuotes,priceBlock} from '../src/pricing-renderer.mjs';
const draft=text=>({text,intent:'pricing',answersMessageId:'pending_1',answersMessageIds:['pending_1'],policyReviewed:true,untrustedContentIgnored:true});
const rejects=fn=>assert.throws(fn,e=>e.reason==='approved_price_facts_required');
test('every approved package, shared single service and comparisons validate at final price boundary',()=>{
  for(const text of ['{{price:google}}','{{price:facebook}}','{{price:both}}','{{price:single}}','{{price:single}}. {{price:both}}','{{price:google}}. {{price:facebook}}. {{price:both}}']){
    const d=renderPricingDraft(draft(text),PRICING);assert.equal(validatePriceDraft(d),true);assert.equal(d.answersMessageId,'pending_1');assert.ok(!d.text.includes('{{'));
  }
});
test('facts remain bound to each exact displayed package block',()=>{
  const d=renderPricingDraft(draft('{{price:single}}. {{price:both}}'),PRICING);
  for(const broken of [
    {...d,text:d.text.replace('$997','$597')},
    {...d,text:d.text.replace('Google and Facebook together','Google Ads')},
    {...d,text:d.text+' Discount is available'},
    {...d,text:d.text+' CAD'},
    {...d,text:d.text+' Costs 5 USD'},
    {...d,text:d.text+' $5'},
    {...d,text:d.text.replace('Ad spend is separate','Ad spend is included')},
    {...d,priceFacts:{...d.priceFacts,monthly:1}},
    {...d,priceQuotes:[{key:'google',text:priceBlock('single',PRICING)}]},
    {...d,priceQuotes:[]}
  ])rejects(()=>validatePriceQuotes(broken,PRICING));
});
test('duplicate packages, unsupported currencies and raw extra prices require repair',()=>{
  for(const text of ['{{price:google}} {{price:google}}','{{price:single}} {{price:google}}','{{price:google}} $5','{{price:google}} in CAD','{{price:google}} free setup','{{price:wrong}}'])rejects(()=>renderPricingDraft(draft(text),PRICING));
});
test('plain non-price answers are untouched',()=>{
  const d={...draft('Sounds good'),intent:'acknowledge'};assert.equal(renderPricingDraft(d,PRICING),d);
});

test('both rendering and final price validation reject financial contradictions outside approved blocks',()=>{
  const contradictions=[
    'Ad spend is included.','Ad spend is covered.','Ad spend is free.',
    'You do not need to pay the setup upfront.','The monthly fee doubles after the first month.',
    'That is the Google-only price.','That is the price for both services.',
    'Setup is five dollars.','Setup is five bucks.','We bill in SEK.','We bill in NOK.',
    'That renews annually.','Setup is optional.','Setup is waived.',
    'Not all prices are USD.','Ad spend is not separate.','Ad spend is separate, but included for you.'
  ];
  for(const claim of contradictions){
    rejects(()=>renderPricingDraft(draft('{{price:google}}. '+claim),PRICING));
    const rendered=renderPricingDraft(draft('{{price:google}}'),PRICING);
    rejects(()=>validatePriceQuotes({...rendered,text:rendered.text+'. '+claim},PRICING));
  }
  rejects(()=>renderPricingDraft(draft('{{price:both}}. That is the Google-only price.'),PRICING));
});

test('financial remainder checks preserve useful service answers, free review details and casual questions',()=>{
  for(const tail of [
    'Are you looking at one or both?',
    'What service are you looking to grow?',
    'We build and manage campaigns around the work you want more of. What service are you looking to grow?',
    'The free 30-minute ads review covers your goals, service area, website and advertising.',
    'You can book a free ads review with Cole. No purchase is required.',
    'We set up your campaigns and tracking. Joining from a computer is helpful for screen sharing.',
    'What monthly ad budget do you have in mind?',
    'Does that pricing work for you?',
    'Management renews monthly until canceled. Setup and the first month are paid upfront.'
  ]){
    const text='{{price:single}}. '+tail,result=renderPricingDraft(draft(text),PRICING);
    assert.equal(validatePriceQuotes(result,PRICING),true);assert.ok(result.text.endsWith(tail));
  }
});

test('existing complete approved disclosures are preserved without duplicated facts or hiding qualified claims',()=>{
  const tail='Ad spend is separate. All prices are USD. Are you looking at one or both?';
  const result=renderPricingDraft(draft('{{price:single}}. '+tail),PRICING);
  assert.equal(result.text.split('Ad spend is separate').length,2);assert.equal(result.text.split('All prices are USD').length,2);
  assert.equal(validatePriceQuotes(result,PRICING),true);
  for(const claim of ['We cannot say all prices are USD.','Except for ad spend, which is included.','Management renews monthly until canceled, then it costs more.'])rejects(()=>renderPricingDraft(draft('{{price:google}}. '+claim),PRICING));
});

test('complete price placeholders normalize only harmless matching introductions',()=>{
  for(const [key,intro] of [
    ['single','For Google or Facebook Ads, it’s '],
    ['single',"For Google or Facebook Ads, it's "],
    ['google','Google Ads management is '],
    ['facebook','Facebook Ads costs '],
    ['both','For both, it is '],
    ['google','The price is '],
    ['both','Our pricing is ']
  ]){
    const bare=renderPricingDraft(draft(`{{price:${key}}}`),PRICING);
    assert.deepEqual(renderPricingDraft(draft(`${intro}{{price:${key}}}`),PRICING),bare,intro);
  }
  const before='We manage ads around the work you want. ',after=' What service are you looking to grow?';
  const actual=renderPricingDraft(draft(before+'Google Ads management is {{price:google}}.'+after),PRICING);
  const expected=renderPricingDraft(draft(before+'{{price:google}}.'+after),PRICING);
  assert.deepEqual(actual,expected);assert.ok(actual.text.startsWith(before));assert.ok(actual.text.endsWith(after));
  assert.deepEqual(
    renderPricingDraft(draft('Google Ads management is {{price:google}}. For both, it’s {{price:both}}.'),PRICING),
    renderPricingDraft(draft('{{price:google}}. {{price:both}}.'),PRICING)
  );
});

test('normalization preserves package mismatches and prior financial assertions for rejection',()=>{
  for(const text of [
    'Both is {{price:google}}',
    "For Google Ads, it's {{price:both}}",
    'Google Ads management is {{price:facebook}}',
    'The discounted price is {{price:google}}',
    'The monthly fee doubles; the price is {{price:google}}'
  ])rejects(()=>renderPricingDraft(draft(text),PRICING));
});

test('ordinary monthly revenue questions remain available after a price answer',()=>{
  for(const question of ["What's your monthly revenue?",'What’s your monthly revenue?','What is your monthly revenue?','Roughly what does your business bring in each month?']){
    const result=renderPricingDraft(draft('{{price:google}}. '+question),PRICING);
    assert.equal(validatePriceQuotes(result,PRICING),true);assert.ok(result.text.endsWith(question));
  }
  rejects(()=>renderPricingDraft(draft("{{price:google}}. What's your monthly revenue after paying only five dollars?"),PRICING));
});

