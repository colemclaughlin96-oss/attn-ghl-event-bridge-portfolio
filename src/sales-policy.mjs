// Owner-approved facts. This deterministic helper is not a language-model evaluator.
import {validatePriceQuotes} from './pricing-renderer.mjs';
export const LOCATION = 'portfolio_fixture_01';
export const PROVIDER = 'portfolio_fixture_02';
export const CALENDAR = 'portfolio_fixture_03';
export const BOOKING_LINK = 'https://example.com/book';
export const CHECKOUT_LINK='https://example.com/checkout/';
export const PRICING = Object.freeze({google:{monthly:597,setup:750,initial:1347},facebook:{monthly:597,setup:750,initial:1347},both:{monthly:997,setup:1199,initial:2196}});
export const NEEDS_COLE = "That needs Cole's input so you get the right answer.";
export const HANDED_OFF = "I've passed that to Cole so you get the right answer.";
export const validId = x => typeof x === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(x);
export function blocked(reason) { const e = new Error(reason); e.reason = reason; throw e; }
export function utc(x) {
  if(typeof x !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(x) || !Number.isFinite(Date.parse(x)) || new Date(Date.parse(x)).toISOString()!==x) blocked('utc_milliseconds_required');
  return Date.parse(x);
}
export function localHour(now, zone) {
  if(typeof zone !== 'string' || !(zone==='UTC'||zone.includes('/'))) blocked('reliable_timezone_required');
  try { return Number(new Intl.DateTimeFormat('en-US',{timeZone:zone,hour:'numeric',hourCycle:'h23'}).format(now)); }
  catch { blocked('invalid_timezone'); }
}
export function inHours(now, zone) { const h=localHour(now,zone); return h>=8 && h<19; }
export function explicitOptOut(body) {
  if(typeof body!=='string')return false;
  const text=body.normalize('NFKC').replace(/[\u2018\u2019]/g,"'").trim().toLowerCase();
  // Direct requests only. A word inside a question, quotation or advertising
  // problem ("How do I stop wasting money?") is not an opt-out instruction.
  // Context-dependent intent still comes from the scoped message assessment.
  // A reported multiline quotation needs contextual assessment; commands inside
  // another person's message are not this contact's consent instruction.
  if(/^[^\n.!?;]*\b(?:lead|customer|client)\b[^\n.!?;]*\b(?:wrote|said|replied|texted)\b[^\n.!?;]*:\s*\n/.test(text))return false;
  const standalone=s=>/^(?:(?:please|pls)\s*,?\s*)?(?:stop|stopall|unsubscribe|stop texting|stop messaging)(?:\s*,?\s*(?:please|pls|now|thanks|thank you))?$/.test(s);
  const command=text.replace(/[.!?]+$/,'').trim().replace(/\s+/g,' ');
  if(standalone(command))return true;
  return text.split(/[.!?;\n]+/).some((part,index,parts)=>{
    const s=part.trim().replace(/\s+/g,' ');
    // A direct STOP can lead or follow only a brief thanks. Do not promote a
    // later quoted/reported bare word to a command.
    if(standalone(s)&&(index===0||parts.slice(0,index).every(p=>/^(?:thanks|thank you)$/i.test(p.trim()))))return true;
    const request=s.replace(/^stop,\s*/,'').replace(/^(?:(?:please|pls|kindly)\s+)?(?:(?:can|could|would) you\s+(?:please\s+)?)?/,'');
    if(/^(?:stop (?:texting|messaging|contacting|calling|emailing) (?:me|us|this number)|(?:do not|don't|dont) (?:text|contact|message|call|email) (?:me|us|this number))\b/.test(request))return true;
    if(/^stop sending (?:(?:me|us) (?:any |these |your |more )?(?:texts|messages|emails|ads)|(?:texts|messages|emails) to (?:me|us|this number))\b/.test(request))return true;
    if(/^(?:unsubscribe (?:me|us)|opt (?:me|us) out|leave (?:me|us) alone)\b/.test(request))return true;
    if(/^(?:i|we) want to unsubscribe(?:,? (?:please|thanks|thank you))?$/.test(request))return true;
    if(/^(?:remove|take) (?:me|us|this number) (?:off|from) (?:your |the |all )?(?:list|lists|mailing list|contact list|text list)\b/.test(request)||/^remove (?:me|us)(?:,? (?:please|thanks|thank you))?$/.test(request))return true;
    if(/^(?:remove|delete) (?:my|our|this) (?:phone )?number(?: (?:off|from) (?:your |the )?(?:list|lists|contact list|text list))?(?:,? (?:please|thanks|thank you))?$/.test(request))return true;
    if(/^(?:sorry[, ]*)?(?:(?:you have|you've got|this is|you're texting|you are texting) (?:the |a )?)?wrong number(?:,? (?:thanks|thank you))?$/.test(s))return true;
    if(/^(?:(?:no|nah)(?:,? (?:thanks|thank you))?,? )?(?:(?:i'm|i am|we're|we are) )?not interested(?: (?:anymore|at this time|in (?:this|your (?:services?|offer)|the (?:service|offer))))?(?:,? (?:thanks|thank you))?$/.test(s)||/^no,? (?:thanks|thank you)$/.test(s))return true;
    return /^(?:stop,? )?(?:i|we) (?:don't|do not|dont) want (?:any more |any |your |these )?(?:texts|messages|calls|emails|services)(?: (?:anymore|at all))?(?:,? (?:please|thanks|thank you))?$/.test(s);
  });
}
export function consentEvidence(contact) {
  const object=x=>x&&typeof x==='object'&&!Array.isArray(x),settings=contact?.dndSettings;
  const channels=object(settings)?Object.values(settings):[];
  if(contact?.dnd===true||channels.some(x=>object(x)&&x.status==='active'))return {status:'blocked',reason:'confirmed_dnd'};
  if(contact?.dnd!==false||settings!==undefined&&(!object(settings)||channels.some(x=>!object(x)||x.status!=='inactive')))return {status:'unknown',reason:'consent_evidence_unknown'};
  return {status:'allowed',reason:null};
}
export function priceAnswer(packageName) {
  const p=PRICING[packageName]; if(!p) blocked('unknown_package');
  const name=packageName==='both'?'Both':packageName==='google'?'Google Ads':'Facebook Ads';
  return `${name} is $${p.monthly.toLocaleString('en-US')}/month plus $${p.setup.toLocaleString('en-US')} setup ($${p.initial.toLocaleString('en-US')} upfront). Ad spend is separate; management renews monthly until canceled. All prices are USD.`;
}
export function inspectDraft(text) {
  if(typeof text!=='string'||!text.trim()||text.length>1500) blocked('invalid_draft');
  if((text.match(/\?/g)||[]).length>1) blocked('one_question_only');
  if(/real talk|game changer|guarantee.{0,30}\d|I'm Cole[.! ]|I am Cole[.! ]/i.test(text)) blocked('unsupported_voice_or_claim');
  return {words:text.trim().split(/\s+/).length,long:text.trim().split(/\s+/).length>35};
}
// Pricing may be part of a multi-question answer. Validate facts and the displayed
// amounts without requiring the entire message to equal one fixed sentence.
export function validatePriceDraft(draft) {
  if(draft&&Object.hasOwn(draft,'priceQuotes'))return validatePriceQuotes(draft,PRICING);
  const p=PRICING[draft?.priceKey],t=draft?.text;
  if(!p||typeof t!=='string')blocked('approved_price_facts_required');
  const f=draft.priceFacts;
  // Historical callers may supply the exact immutable canonical quote alone;
  // supplied structured facts must still be correct even for canonical text.
  if(t===priceAnswer(draft.priceKey)&&f===undefined)return true;
  if(!f||f.monthly!==p.monthly||f.setup!==p.setup||f.initial!==p.initial||f.currency!=='USD'||f.adSpendAdditional!==true)blocked('approved_price_facts_required');
  const foreignCode=/\b(?:CAD|AUD|NZD|EUR|GBP|JPY|CNY|RMB|CHF|INR|ZAR|MXN|BRL|SGD|HKD|SEK|NOK|DKK|KRW|AED|SAR)\b/i;
  const foreignName=/\b(?:(?:Canadian|Australian|New Zealand|Singapore|Hong Kong|Mexican) dollars?|euros?|pounds? sterling|British pounds?|yen|yuan|rupees?)\b/i;
  if(!/\bUSD\b/.test(t)||foreignCode.test(t)||foreignName.test(t)||/\b(?:C|CA|A|AU|NZ|S|SG|HK|MX|R)\$/.test(t)||[...t].some(c=>c!=='$'&&/\p{Sc}/u.test(c)))blocked('approved_price_facts_required');
  const combined=/\bboth\b|\bgoogle(?: ads)?\s*(?:and|&|\+)\s*facebook\b/i;
  const label=draft.priceKey==='both'?combined:draft.priceKey==='google'?/\bgoogle\b/i:/\bfacebook\b/i;
  if(!label.test(t))blocked('approved_price_facts_required');
  // A single-service quote can mention either Google or Facebook, which share
  // the same approved price, but must not label that price as the combined plan.
  const pricedCombined=/\bboth(?:\s+(?:platforms|channels|services))?\s*(?:is|are|costs?|runs?|:|=)?\s*(?:USD\s*)?\$/i;
  const pricedSingle=/\b(?:google|facebook)(?: ads)?\s*(?:is|costs?|runs?|:|=)\s*(?:USD\s*)?\$/i;
  const individualLabels=t.replace(/\bgoogle(?: ads)?\s*(?:and|&|\+)\s*facebook(?: ads)?\b/gi,'both');
  if(draft.priceKey!=='both'&&(pricedCombined.test(t)||/\bgoogle(?: ads)?\s*(?:and|&|\+)\s*facebook(?: ads)?\s*(?:is|are|costs?|:|=)?\s*(?:USD\s*)?\$/i.test(t))||draft.priceKey==='both'&&pricedSingle.test(individualLabels))blocked('approved_price_facts_required');
  const spend='(?:ad|advertising)\\s+(?:spend|budget|costs?)';
  const separate=new RegExp('\\b'+spend+'\\s*(?:(?:is|are)\\s+)?(?:separate|additional|extra|not included|(?:billed|paid) separately)\\b|\\b(?:separate|additional|extra)\\s+'+spend+'\\b|\\b(?:plus|excluding|excludes|not including)\\s+'+spend+'\\b','i');
  const included=new RegExp('\\b'+spend+'\\s*(?:(?:is|are)\\s+)?(?:included|covered|free)\\b','i');
  if(!separate.test(t)||included.test(t))blocked('approved_price_facts_required');

  // Check the role of every displayed dollar amount. Merely finding the three
  // approved numbers somewhere would also accept swapped monthly/setup prices.
  const money=[...t.matchAll(/\$\s*(\d+(?:,\d+)*(?:\.\d+)?)/g)];
  if(!money.length||(t.match(/\$/g)||[]).length!==money.length)blocked('approved_price_facts_required');
  const currency='(?:\\s*(?:USD|US dollars?))?\\s*';
  const roles={
    monthly:{
      before:/(?:monthly(?:\s+(?:management|fee|cost|payment|charge|retainer)){0,2}|(?:management|service)\s+(?:fee|cost)\s+per\s+month|(?:the\s+)?first\s+month(?:'s)?(?:\s+(?:fee|payment))?)\s*(?:(?:is|costs?|at|of)\s*|[:=]\s*)?(?:USD\s*)?$/i,
      after:new RegExp('^'+currency+'(?:/\\s*(?:mo|month)\\b|(?:per|a|each)\\s+month\\b|monthly\\b|(?:for\\s+)?(?:the\\s+)?first\\s+month\\b)','i')
    },
    setup:{
      before:/set[ -]?up(?:\s+(?:fee|cost|charge))?\s*(?:(?:is|costs?|at|of)\s*|[:=]\s*)?(?:USD\s*)?$/i,
      after:new RegExp('^'+currency+'(?:for\\s+(?:the\\s+)?)?(?:one[ -]time\\s+)?set[ -]?up(?:\\s+(?:fee|cost|charge))?\\b(?!(?:\\s+(?:fee|cost|charge))?\\s*(?:is\\b|costs?\\b|[:=]))','i')
    },
    initial:{
      before:/(?:(?:total\s+)?up[ -]?front(?:\s+(?:payment|total|cost))?|(?:initial|first)\s+(?:payment|total|cost)|(?:total\s+)?(?:due|paid)\s+(?:up[ -]?front|today)|total\s+to\s+start)\s*(?:(?:is|costs?|at|of)\s*|[:=]\s*)?(?:USD\s*)?$/i,
      after:new RegExp('^'+currency+'(?:up[ -]?front|(?:initial|first)\\s+(?:payment|total)|(?:due|paid)\\s+(?:up[ -]?front|today)|to\\s+start|today)\\b(?!\\s*(?:is\\b|[:=]))','i')
    }
  };
  const present=new Set();
  for(let i=0;i<money.length;i++){
    const m=money[i],end=m.index+m[0].length;
    if(!/^(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.00)?$/.test(m[1])||/^[\dA-Za-z_]|^[.,]\d/.test(t.slice(end)))blocked('approved_price_facts_required');
    const value=Number(m[1].replace(/,/g,'')),before=t.slice(i?money[i-1].index+money[i-1][0].length:0,m.index),after=t.slice(end,i+1<money.length?money[i+1].index:t.length);
    const matched=Object.entries(roles).filter(([role,r])=>{
      let prefix=r.before.test(before),suffix=r.after.test(after);
      // In sparse text, a bare label between amounts can belong to either side:
      // "monthly $597 setup $750". Give that label to its matching adjacent
      // amount; every amount must still retain its own verified role.
      if(prefix&&i&&r.after.test(before)&&Number(money[i-1][1].replace(/,/g,''))===p[role]&&value!==p[role])prefix=false;
      if(suffix&&i+1<money.length&&r.before.test(after)&&Number(money[i+1][1].replace(/,/g,''))===p[role]&&value!==p[role])suffix=false;
      return prefix||suffix;
    }).map(([role])=>role);
    if(!matched.length||matched.some(role=>p[role]!==value))blocked('approved_price_facts_required');
    for(const role of matched)present.add(role);
  }
  if(!present.has('monthly')||!present.has('setup'))blocked('approved_price_facts_required');
  if(/discount|guaranteed|refund|free ads/i.test(t))blocked('unapproved_price_terms');
  return true;
}

// Model completion metadata is required; punctuation cannot prove completeness.
// Called before acquiring a send permit, so a bounded repair cannot duplicate sends.
export function validateGeneration(result) {
  if(!result||!['stop','completed'].includes(result.finishReason)||result.complete!==true)blocked('generation_incomplete');
  if(result.handoff) {
    if(typeof result.handoff.reason!=='string'||typeof result.handoff.decision_needed!=='string')blocked('generation_schema_invalid');
    return result;
  }
  inspectDraft(result.draft?.text);
  if(result.draft?.policyReviewed!==true||result.draft?.untrustedContentIgnored!==true)blocked('draft_review_required');
  return result;
}
// Pure planning guard. No booking mutation permit is implemented in this version.
export function schedulingDecision({intent,contactId,appointments,timezoneConfirmed=false,timezone,selectedSlot,explicitSelection=false,slots,availabilityReadAt,now,existingId}) {
  if(!validId(contactId)||!Array.isArray(appointments)) return {action:'hold',reason:'appointment_state_unknown'};
  if(appointments.some(a=>a.locationId!==LOCATION||a.contactId!==contactId||a.calendarId!==CALENDAR||!validId(a.id)||!['new','confirmed','active','cancelled','showed','noshow','invalid','completed'].includes(a.appointmentStatus))) return {action:'handoff',reason:'appointment_conflict'};
  const active=appointments.filter(a=>['new','confirmed','active'].includes(a.appointmentStatus));
  if(active.length>1) return {action:'handoff',reason:'multiple_bookings'};
  if(!['book','reschedule','cancel','join'].includes(intent)) return {action:'hold',reason:'unknown_intent'};
  if(intent==='join') return {action:'needs_existing_link',appointment_id:active[0]?.id??null};
  if(intent==='book'&&active.length) return {action:'help_existing',appointment_id:active[0].id};
  if(['reschedule','cancel'].includes(intent)&&(!validId(existingId)||active.length!==1||active[0].id!==existingId)) return {action:'handoff',reason:'existing_appointment_required'};
  if(intent==='cancel') return {action:'needs_cole',reason:'booking_writes_not_enabled',appointment_id:existingId};
  if(!timezoneConfirmed) return {action:'ask_timezone'};
  localHour(now,timezone);
  if(!explicitSelection||typeof selectedSlot!=='string'||!/[+-]\d\d:\d\d$|Z$/.test(selectedSlot)||!Number.isFinite(Date.parse(selectedSlot))) return {action:'needs_exact_slot'};
  if(!Array.isArray(slots)||!slots.includes(selectedSlot)||utc(availabilityReadAt)>now||now-utc(availabilityReadAt)>15000||Date.parse(selectedSlot)<=now) return {action:'hold',reason:'fresh_available_exact_slot_required'};
  return intent==='reschedule'?{action:'needs_cole',reason:'booking_writes_not_enabled',preserve_appointment_id:existingId}:{action:'booking_link',url:BOOKING_LINK,reason:'booking_writes_not_enabled'};
}


