// Local application spending guard, not an OpenAI account/vendor billing cap.
// Stores counters and opaque reservation IDs only; never prompts, keys or PII.
import {randomUUID} from 'node:crypto';

const problem=reason=>{const e=new Error(reason);e.reason=reason;return e;};
const need=(condition,reason)=>{if(!condition)throw problem(reason);};
const integer=x=>Number.isSafeInteger(x)&&x>=0;
const identifier=x=>typeof x==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(x);
const MICRO=1000000;
const validRate=x=>typeof x==='number'&&Number.isFinite(x)&&x>0&&x<=1000000;
// A USD-per-million rate multiplied by a token count is micro-USD.
const cost=(input,output,inputRate,outputRate,multiplier=1)=>{
  const value=Math.ceil((input*inputRate+output*outputRate)*multiplier);
  need(integer(value),'model_budget_amount_invalid');return value;
};

export class ModelBudget {
  constructor({db,model,rates,monthlyLimitUsd=10,margin=1.10,overheadTokens=1024,now=Date.now}={}) {
    need(db&&typeof db.exec==='function'&&typeof db.prepare==='function','model_budget_database_required');
    need(typeof model==='string'&&/^[A-Za-z0-9._:-]{1,100}$/.test(model),'model_budget_model_required');
    need(rates&&validRate(rates.inputUsdPerMillion)&&validRate(rates.outputUsdPerMillion),'model_budget_explicit_rates_required');
    need(typeof monthlyLimitUsd==='number'&&Number.isFinite(monthlyLimitUsd)&&monthlyLimitUsd>=0&&monthlyLimitUsd<=1000000,'model_budget_limit_invalid');
    need(typeof margin==='number'&&Number.isFinite(margin)&&margin>=1&&margin<=2&&integer(overheadTokens)&&overheadTokens>=128&&overheadTokens<=10000&&typeof now==='function','model_budget_configuration_invalid');
    this.db=db;this.model=model;this.rates={inputUsdPerMillion:rates.inputUsdPerMillion,outputUsdPerMillion:rates.outputUsdPerMillion};
    this.limit=Math.floor(monthlyLimitUsd*MICRO);this.margin=margin;this.overheadTokens=overheadTokens;this.now=now;
    this.db.exec(`CREATE TABLE IF NOT EXISTS model_budget_reservations (
      reservation_id TEXT PRIMARY KEY, month TEXT NOT NULL, model TEXT NOT NULL,
      purpose TEXT NOT NULL, reserved_micro_usd INTEGER NOT NULL,
      accounted_micro_usd INTEGER, input_rate REAL NOT NULL, output_rate REAL NOT NULL,
      input_tokens INTEGER, output_tokens INTEGER, state TEXT NOT NULL,
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    ); CREATE INDEX IF NOT EXISTS model_budget_month ON model_budget_reservations(month);`);
    this.allowRequest=this.allowRequest.bind(this);this.recordUsage=this.recordUsage.bind(this);
  }
  month(at=this.now()) {
    need(Number.isFinite(at),'model_budget_clock_invalid');
    const date=new Date(at);need(Number.isFinite(date.getTime()),'model_budget_clock_invalid');return date.toISOString().slice(0,7);
  }
  tx(fn) {
    // SQLite's write transaction serializes reservations across connections and
    // processes, so simultaneous requests cannot both spend the same allowance.
    let active=false;
    try{this.db.exec('BEGIN IMMEDIATE');active=true;const value=fn();this.db.exec('COMMIT');active=false;return value;}
    catch(e){if(active){try{this.db.exec('ROLLBACK');}catch{}}if(e?.reason)throw e;throw problem('model_budget_store_failed');}
  }
  sum(month) {
    const r=this.db.prepare('SELECT COALESCE(SUM(COALESCE(accounted_micro_usd,reserved_micro_usd)),0) AS used FROM model_budget_reservations WHERE month=?').get(month);
    need(integer(r.used),'model_budget_store_invalid');return r.used;
  }
  allowRequest({purpose,model,maxOutputTokens,inputBytes,signal}={}) {
    need(['assess','generate'].includes(purpose)&&model===this.model&&integer(inputBytes)&&inputBytes>0&&inputBytes<=1000000&&integer(maxOutputTokens)&&maxOutputTokens>=128&&maxOutputTokens<=32768,'model_budget_request_invalid');
    if(signal?.aborted)return {allowed:false,reason:'model_request_cancelled'};
    // Adapter inputBytes is the entire serialized request, including the JSON
    // schema. One input token per UTF-8 byte plus framing and a margin deliberately
    // over-reserves ordinary text; no unverified cache discount is assumed.
    const reserved=cost(inputBytes+this.overheadTokens,maxOutputTokens,this.rates.inputUsdPerMillion,this.rates.outputUsdPerMillion,this.margin);
    const at=this.now(),month=this.month(at);
    return this.tx(()=>{
      const used=this.sum(month);
      if(reserved>this.limit-used)return {allowed:false,reason:'model_budget_exhausted',month};
      const id='model_'+randomUUID();
      this.db.prepare('INSERT INTO model_budget_reservations(reservation_id,month,model,purpose,reserved_micro_usd,input_rate,output_rate,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)').run(id,month,model,purpose,reserved,this.rates.inputUsdPerMillion,this.rates.outputUsdPerMillion,'reserved',at,at);
      return {allowed:true,reservationId:id};
    });
  }
  recordUsage({reservationId,model,purpose,usage}={}) {
    need(identifier(reservationId)&&model===this.model&&['assess','generate'].includes(purpose),'model_budget_receipt_invalid');
    return this.tx(()=>{
      const r=this.db.prepare('SELECT * FROM model_budget_reservations WHERE reservation_id=?').get(reservationId);
      need(r&&r.model===model&&r.purpose===purpose,'model_budget_reservation_missing');
      const known=usage&&integer(usage.input_tokens)&&integer(usage.output_tokens)&&integer(usage.total_tokens)&&usage.total_tokens===usage.input_tokens+usage.output_tokens;
      if(!known){
        // Timeout/refusal/incomplete/HTTP errors without reliable usage retain
        // the full reservation, even across restart. Unknown is never free.
        if(r.state!=='settled')this.db.prepare("UPDATE model_budget_reservations SET state='unknown',updated_at=? WHERE reservation_id=?").run(this.now(),reservationId);
        return {recorded:true,usageKnown:r.state==='settled',reservationRetained:r.state!=='settled'};
      }
      const actual=cost(usage.input_tokens,usage.output_tokens,r.input_rate,r.output_rate);
      // API cached/reasoning tokens are already included in input/output totals.
      // Ignore cache discounts conservatively; do not double-charge reasoning.
      // Duplicate receipts do not add cost. A later lower receipt cannot reduce
      // an amount already settled; a higher correction only increases it.
      if(r.accounted_micro_usd!==null&&actual<=r.accounted_micro_usd)return {recorded:true,usageKnown:true,alreadyRecorded:true};
      this.db.prepare("UPDATE model_budget_reservations SET accounted_micro_usd=?,input_tokens=?,output_tokens=?,state='settled',updated_at=? WHERE reservation_id=?").run(actual,usage.input_tokens,usage.output_tokens,this.now(),reservationId);
      return {recorded:true,usageKnown:true,reservationRetained:false};
    });
  }
  status() {
    const month=this.month(),used=this.sum(month);
    const counts=this.db.prepare("SELECT COUNT(*) AS requests,SUM(CASE WHEN state!='settled' THEN 1 ELSE 0 END) AS unresolved FROM model_budget_reservations WHERE month=?").get(month);
    return {month,monthlyLimitUsd:this.limit/MICRO,accountedAndReservedUsd:used/MICRO,remainingUsd:Math.max(0,this.limit-used)/MICRO,requests:counts.requests,unresolvedUsage:counts.unresolved??0,applicationGuardOnly:true};
  }
}

