// Model chooses packages; approved application facts supply the actual prices.
// No model amounts are silently corrected, and no outbound action occurs here.
const fail=()=>{const e=new Error('approved_price_facts_required');e.reason='approved_price_facts_required';throw e;};
const own=(o,k)=>o&&Object.hasOwn(o,k);
const KEYS=['google','facebook','both','single'];
const factKey=k=>k==='single'?'google':k;
const name=k=>({google:'Google Ads',facebook:'Facebook Ads',both:'Google and Facebook together',single:'Google or Facebook Ads'})[k];
const dollars=n=>'$'+n.toLocaleString('en-US');
export function priceBlock(key,pricing){
  if(!KEYS.includes(key)||!pricing?.[factKey(key)])fail();
  const p=pricing[factKey(key)];
  return `${name(key)} is ${dollars(p.monthly)}/month plus ${dollars(p.setup)} setup (${dollars(p.initial)} upfront)`;
}
const facts=(key,pricing)=>({...pricing[factKey(key)],currency:'USD',adSpendAdditional:true});
const sameFacts=(a,b)=>a&&Object.keys(b).every(k=>a[k]===b[k]);
function normalizePriceIntroductions(text){
  const labels={google:'Google(?:[- ]only)?(?: Ads)?(?: management)?',facebook:'Facebook(?:[- ]only)?(?: Ads)?(?: management)?',single:'(?:either )?Google(?: Ads)? or Facebook(?: Ads)?(?: management)?',both:'(?:both(?: services| platforms| channels)?|Google(?: Ads)? (?:and|&|\\+) Facebook(?: Ads)?(?: together)?)(?: management)?'};
  let result='',cursor=0;
  for(const token of text.matchAll(/\{\{price:(google|facebook|both|single)\}\}/g)){
    let before=text.slice(cursor,token.index);
    // Match only the final complete clause's introduction, so earlier answers
    // and questions cannot be discarded when the model repeats a package name.
    const tail=before.match(/(^|[.!?;\n])([^.!?;\n]*)$/);
    if(tail){
      const intro=tail[2].trim().replace(/[\u2018\u2019]/g,"'");
      const generic=/^(?:the |our )?(?:price|pricing|cost|fee)(?: is|:)$/.test(intro.toLowerCase());
      const matched=Object.entries(labels).find(([,label])=>new RegExp('^(?:'+label+' (?:is|costs?|runs?)|for '+label+',? (?:it is|it\\\'s|the price is))$','i').test(intro));
      if(matched&&matched[0]!==token[1])fail();
      if(generic||matched)before=before.slice(0,tail.index)+tail[1]+(tail[1]&&tail[2].startsWith(' ')?' ':'');
    }
    result+=before+token[0];cursor=token.index+token[0].length;
  }
  return result+text.slice(cursor);
}
const foreign=t=>/\b(?:CAD|AUD|NZD|EUR|GBP|JPY|CNY|RMB|CHF|INR|ZAR|MXN|BRL|SGD|HKD|SEK|NOK|DKK|KRW|AED|SAR)\b|\b(?:Canadian|Australian|New Zealand|Singapore|Hong Kong|Mexican) dollars?|euros?|pounds? sterling|British pounds?|yen|yuan|rupees?|\b(?:C|CA|A|AU|NZ|S|SG|HK|MX|R)\$/i.test(t)||[...t].some(c=>c!=='$'&&/\p{Sc}/u.test(c));
const moneyOutside=t=>/\p{Sc}|\b(?:USD|dollars?|bucks)\b|\b\d[\d,.]*\s*(?:per month|a month|monthly|\/\s*mo(?:nth)?)\b/iu.test(t);
const spend=t=>/ad(?:vertising)? spend (?:is )?(?:separate|additional|extra)/i.test(t);
const terms=t=>/\b(?:discount|guaranteed|refund)\b|free ads(?!\s+review\b)|free setup|setup (?:is )?free|no setup fee|no charge/i.test(t);
// Only complete approved disclosure clauses may disappear from the remainder.
// A negated/qualified statement such as "not all prices are USD" stays visible.
const disclosure=/(^|[.!?;\n])\s*(?:ad(?:vertising)? spend (?:is )?(?:separate|additional|extra)|all prices are USD|management renews monthly until canceled|setup and (?:the )?first month are paid upfront|no purchase is required)\s*(?=$|[.!?;\n])/gi;
const financialTopics=/\b(?:prices?|pricing|costs?|fees?|charges?|charged|charging|pay|paid|payments?|bills?|billed|billing|up[ -]?front|deposits?|installments?|financing|financed|refunds?|discounts?|renew(?:s|al|als|ing)?|cancel(?:led|ed|lation)?|contracts?|annual(?:ly)?|yearly)\b|\b(?:ad|advertising)\s+(?:spend|budget)\b|\b(?:monthly|first month)\b|\bset[ -]?up\s+(?:is\s+)?(?:optional|waived|included|not required)\b/i;
function financialRemainder(text){
  const remainder=text.replace(disclosure,'$1');
  if(moneyOutside(remainder)||foreign(remainder)||terms(remainder))fail();
  // Nonfinancial service/booking prose and ordinary next questions remain free
  // text. Financial assertions belong in approved blocks/disclosures only.
  for(const clause of remainder.match(/[^.!?;\n]+(?:[.!?;\n]+|$)/g)??[]){
    if(!financialTopics.test(clause))continue;
    const q=clause.trim().replace(/[\u2018\u2019]/g,"'");
    const budgetQuestion=/^(?:what(?: monthly)?(?: ad| advertising)? budget (?:do you have in mind|are you considering|are you comfortable with)|how much(?: ad| advertising)? budget are you considering|do you have (?:an? )?(?:monthly )?(?:ad |advertising )?budget in mind|does that (?:price|pricing|cost) work for you|does that fit (?:your|the) budget|is that within (?:your|the) budget|what(?:'s| is) your monthly revenue)\?$/i;
    if(!budgetQuestion.test(q))fail();
  }
}

export function validatePriceQuotes(draft,pricing){
  const qs=draft?.priceQuotes,t=draft?.text;
  if(!Array.isArray(qs)||qs.length<1||qs.length>3||typeof t!=='string'||foreign(t))fail();
  let remainder=t;const seen=new Set();
  for(const q of qs){
    if(!q||Object.keys(q).length!==2||!own(q,'key')||!own(q,'text')||!KEYS.includes(q.key)||seen.has(factKey(q.key)))fail();
    seen.add(factKey(q.key));
    const expected=priceBlock(q.key,pricing);
    if(q.text!==expected||remainder.split(expected).length!==2)fail();
    remainder=remainder.replace(expected,'');
  }
  if(draft.priceKey!==factKey(qs[0].key)||!sameFacts(draft.priceFacts,facts(qs[0].key,pricing))||!spend(t)||!/\bUSD\b/.test(t)||/\{\{price:/i.test(t))fail();
  financialRemainder(remainder);
  return true;
}

export function renderPricingDraft(draft,pricing){
  if(typeof draft?.text!=='string'||!draft.text.includes('{{price:'))return draft;
  if(draft.intent!=='pricing'||own(draft,'priceQuotes'))fail();
  const normalized=normalizePriceIntroductions(draft.text);
  const tokens=[...normalized.matchAll(/\{\{price:(google|facebook|both|single)\}\}/g)];
  if(!tokens.length||tokens.length>3)fail();
  const remainder=normalized.replace(/\{\{price:(google|facebook|both|single)\}\}/g,'');
  if(/\{\{|\}\}/.test(remainder))fail();
  financialRemainder(remainder);
  const primary=factKey(tokens[0][1]);
  if(draft.priceKey!=null&&draft.priceKey!==primary||draft.priceFacts!=null&&!sameFacts(draft.priceFacts,facts(primary,pricing)))fail();
  const priceQuotes=tokens.map(m=>({key:m[1],text:priceBlock(m[1],pricing)}));
  let index=0;
  const text=normalized.replace(/\{\{price:(google|facebook|both|single)\}\}/g,(_m,k)=>{
    let block=priceBlock(k,pricing);
    if(++index===tokens.length){
      if(!spend(normalized))block+='. Ad spend is separate';
      if(!/\bUSD\b/.test(normalized))block+='. All prices are USD';
    }
    return block;
  });
  const result={...draft,text,priceKey:primary,priceFacts:facts(primary,pricing),priceQuotes};
  validatePriceQuotes(result,pricing);
  return result;
}

