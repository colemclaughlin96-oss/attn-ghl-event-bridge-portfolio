import test from 'node:test';
import assert from 'node:assert/strict';
import {PRICING,priceAnswer,validatePriceDraft} from '../src/sales-policy.mjs';

const quote=(text,priceKey='google',changes={})=>({text,priceKey,priceFacts:{...PRICING[priceKey],currency:'USD',adSpendAdditional:true},...changes});
const reject=q=>assert.throws(()=>validatePriceDraft(q),e=>['approved_price_facts_required','unapproved_price_terms'].includes(e.reason));

test('single-package prices accept natural currency position, setup order and light punctuation',()=>{
  for(const text of [
    'Google Ads is $597 USD/month plus $750 setup. Ad spend is separate.',
    'Google Ads is $597/month and setup is $750. Ad spend is separate, all prices USD.',
    'Google Ads costs USD $597 per month, with setup at USD $750. Advertising spend is additional.',
    'Google Ads is $597.00 a month plus a $750.00 one-time setup fee. Ad spend is extra. All prices USD.',
    'google ads monthly is $597 setup is $750 upfront is $1,347 ad spend separate USD',
    'google ads monthly $597 setup $750 upfront $1347 USD ad spend extra',
    'Google or Facebook is $597/month plus $750 setup. Ad spend is separate, all prices USD. Are you looking at one or both?',
    'Facebook is $597 monthly with a setup fee of $750. Ad spend is billed separately. USD'
  ])assert.equal(validatePriceDraft(quote(text,text.startsWith('Facebook')?'facebook':'google')),true,text);
});

test('initial payment wording can precede or follow the exact total without changing monthly or setup facts',()=>{
  for(const text of [
    'Google Ads is $597/month plus $750 setup ($1,347 upfront). Ad spend is separate. USD',
    'Google Ads upfront payment is $1,347. Monthly management is $597 and setup costs $750. Ad spend is additional. USD',
    'Google Ads initial payment: $1,347, then $597/month. Setup is $750. Ad spend is separate. USD',
    'Google Ads is $750 setup plus $597 for the first month ($1,347 upfront), then $597/month. Ad spend is separate. USD',
    'Google Ads is $597/month, setup $750, and $1,347 due today. Ad spend is not included. USD',
    'Both is $997 USD/month with setup at $1,199 and an initial payment of $2,196. Additional ad spend. USD',
    'Google and Facebook is $997/month plus $1,199 setup. Total upfront is $2,196. Ad spend is extra. USD'
  ])assert.equal(validatePriceDraft(quote(text,/^Both|^Google and Facebook/.test(text)?'both':'google')),true,text);
});

test('separate advertising spend disclosure accepts natural equivalents but not inclusion or silence',()=>{
  const base='Google Ads is $597/month plus $750 setup. All prices USD. ';
  for(const disclosure of ['Ad spend is separate','Advertising spend is additional','Ad spend extra','Plus ad spend','Excluding advertising spend','Ad spend is not included','Ad spend is paid separately','Separate ad spend'])assert.equal(validatePriceDraft(quote(base+disclosure)),true,disclosure);
  for(const disclosure of ['', 'Ad spend is included','Ad spend is free','Ad spend is included. Ad spend is separate'])reject(quote(base+disclosure));
});

test('every amount must match its stated price role, even when all amounts are approved somewhere',()=>{
  for(const text of [
    'Google Ads is $750/month plus $597 setup. Ad spend is separate. USD',
    'Google Ads monthly is $750 and setup is $597. Ad spend is separate. USD',
    'Google Ads is $597/month plus $750 setup, or $750/month plus $597 setup. Ad spend is separate. USD',
    'Google Ads is $597/month plus $750 setup. Upfront is $750. Ad spend is separate. USD',
    'Google Ads is $1,347/month plus $750 setup. Ad spend is separate. USD',
    'Google Ads is $597/month plus $1,347 setup. Ad spend is separate. USD',
    'Google Ads is $598/month plus $750 setup. Ad spend is separate. USD',
    'Google Ads is $597/month plus $751 setup. Ad spend is separate. USD',
    'Google Ads is $597/month plus $750 setup, upfront $1,346. Ad spend is separate. USD',
    'Google Ads is $597/month plus $750 setup and $50 extra. Ad spend is separate. USD',
    'Google Ads is $597/month plus $750 setup and another $597. Ad spend is separate. USD',
    'Google Ads monthly is $597k and setup is $750. Ad spend is separate. USD',
    'Google Ads is $597.50/month plus $750 setup. Ad spend is separate. USD'
  ])reject(quote(text));
});

test('single-package validation cannot quote the combined plan at a single-service price or merge packages',()=>{
  reject(quote('Google and Facebook is $597/month plus $750 setup. Ad spend is separate. USD'));
  reject(quote('Google offers both. Both is $597/month plus $750 setup. Ad spend is separate. USD'));
  reject(quote('Both is $997/month plus $1,199 setup. Google Ads is $997/month. Ad spend is separate. USD','both'));
  reject(quote('Google or Facebook is $997/month plus $1,199 setup. Ad spend is separate. USD','both'));
  const all='Google or Facebook is $597/month plus $750 setup. Both is $997/month plus $1,199 setup. Ad spend is separate. USD';
  for(const key of Object.keys(PRICING))reject(quote(all,key));
});

test('exact structured facts, explicit USD and foreign currency rejection remain required',()=>{
  const text='Google Ads is $597 USD/month plus $750 setup. Ad spend is separate.';
  for(const change of [{monthly:598},{setup:749},{initial:1346},{currency:'CAD'},{adSpendAdditional:false}])reject(quote(text,'google',{priceFacts:{...PRICING.google,currency:'USD',adSpendAdditional:true,...change}}));
  reject(quote(priceAnswer('google'),'google',{priceFacts:{...PRICING.google,monthly:598,currency:'USD',adSpendAdditional:true}}));
  reject(quote(text,'google',{priceFacts:null}));reject(quote(text.replace('USD','')));
  for(const currency of ['CAD','AUD','EUR','GBP','Canadian dollars','C$','A$','€','£','¥','₹'])reject(quote(text+' '+currency));
});

test('unapproved terms stay rejected and approved canonical legacy quotes remain compatible',()=>{
  const text='Google Ads is $597/month plus $750 setup. Ad spend is separate. USD';
  for(const tail of [' A discount applies.',' Guaranteed jobs.',' You get a refund.',' Free ads included.'])reject(quote(text+tail));
  for(const priceKey of Object.keys(PRICING))assert.equal(validatePriceDraft({priceKey,text:priceAnswer(priceKey)}),true);
});

