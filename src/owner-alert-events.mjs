// Owner-only notifications. This outbox never claims, retries or sends a customer job.
import crypto from 'node:crypto';
import {LOCATION,RpcError} from './bridge.mjs';
import {canonical,hash,equal,callbackURL,signingKey,signedHeaders} from './security.mjs';

export const OWNER_ALERT_EVENT='attn.owner.alert';
const KINDS=['handoff','technical_failure'],TTL=900000,LEASE=15000,MAX_ATTEMPTS=8,PIN='owner_alert_owner_pin';
const id=x=>typeof x==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(x);
const only=(x,keys)=>x&&typeof x==='object'&&!Array.isArray(x)&&Object.keys(x).every(k=>keys.includes(k));
const plain=x=>x!==null&&typeof x==='object'&&(Object.getPrototypeOf(x)===Object.prototype||Object.getPrototypeOf(x)===null);
const invalid=reason=>{throw new RpcError(-32602,'Invalid owner alert parameters',reason);};

export class OwnerAlertEvents {
  constructor({bridge,operatorOwner}={}) {
    if(!bridge?.db||!bridge.vault||typeof bridge.post!=='function'||typeof operatorOwner!=='string'||!operatorOwner||operatorOwner.length>256||operatorOwner==='attn_server_worker_v1'||bridge.operatorOwner&&bridge.operatorOwner!==operatorOwner)invalid('owner_alert_operator_required');
    this.b=bridge;this.owner=operatorOwner;this.busy=false;this.serialTail=Promise.resolve();
    this.b.db.exec(`CREATE TABLE IF NOT EXISTS owner_alert_subscriptions(id TEXT PRIMARY KEY,owner TEXT NOT NULL,state TEXT NOT NULL,expires INTEGER NOT NULL,active INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS owner_alert_receipts(alert_id TEXT PRIMARY KEY REFERENCES server_alerts(id),owner TEXT NOT NULL,kind TEXT NOT NULL,source_event_id TEXT NOT NULL,source_created INTEGER NOT NULL,created INTEGER NOT NULL,read_at INTEGER,acknowledged_at INTEGER,ack_id TEXT);
      CREATE TABLE IF NOT EXISTS owner_alert_deliveries(alert_id TEXT NOT NULL REFERENCES owner_alert_receipts(alert_id),sub_id TEXT NOT NULL REFERENCES owner_alert_subscriptions(id),status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,due INTEGER NOT NULL,lease_until INTEGER,lease_token_hash TEXT,accepted_at INTEGER,last_code INTEGER,PRIMARY KEY(alert_id,sub_id));
      CREATE INDEX IF NOT EXISTS owner_alert_due ON owner_alert_deliveries(status,due);`);
    this.tx(()=>{
      const pin=this.b.db.prepare('SELECT owner,state FROM owner_alert_subscriptions WHERE id=?').get(PIN);
      if(pin){if(pin.owner!==this.owner||this.b.vault.open(pin.state,PIN).owner!==this.owner)invalid('owner_alert_operator_immutable');}
      else this.b.db.prepare('INSERT INTO owner_alert_subscriptions(id,owner,state,expires,active) VALUES(?,?,?,?,0)').run(PIN,this.owner,this.b.vault.seal({owner:this.owner},PIN),0);
    });
  }
  authorize(owner){if(owner!==this.owner)invalid('owner_alert_operator_required');}
  tx(fn){this.b.db.exec('BEGIN IMMEDIATE');try{const value=fn();this.b.db.exec('COMMIT');return value;}catch(e){this.b.db.exec('ROLLBACK');throw e;}}
  async serial(fn){const next=this.serialTail.catch(()=>{}).then(fn);this.serialTail=next;return next;}
  catalog(){return [{name:OWNER_ALERT_EVENT,description:'An ATTN owner alert needs review. Identifiers only; fetch the private alert separately. Never executes or retries a customer message.',delivery:['webhook'],
    inputSchema:{type:'object',properties:{location_id:{type:'string',const:LOCATION},kinds:{type:'array',items:{type:'string',enum:KINDS},minItems:1,maxItems:2,uniqueItems:true}},required:['location_id'],additionalProperties:false},
    payloadSchema:{type:'object',properties:{alert_id:{type:'string'},location_id:{type:'string',const:LOCATION},kind:{type:'string',enum:KINDS}},required:['alert_id','location_id','kind'],additionalProperties:false}}];}
  identity(owner,params){
    this.authorize(owner);
    if(!only(params,['name','arguments','delivery','ttlMs','cursor','_meta'])||params.name!==OWNER_ALERT_EVENT)invalid('owner_alert_event_required');
    // Protocol metadata is not subscription identity, tenant scope or authority.
    if(Object.hasOwn(params,'_meta')&&!plain(params._meta))invalid('owner_alert_metadata_invalid');
    if(!only(params.arguments,['location_id','kinds'])||params.arguments.location_id!==LOCATION)invalid('owner_alert_location_required');
    const kinds=params.arguments.kinds??KINDS;
    if(!Array.isArray(kinds)||!kinds.length||kinds.length>2||new Set(kinds).size!==kinds.length||kinds.some(k=>!KINDS.includes(k)))invalid('owner_alert_kinds_invalid');
    if(!only(params.delivery,['mode','url','secret'])||params.delivery.mode!=='webhook')invalid('webhook_required');
    try{callbackURL(params.delivery.url);}catch{invalid('invalid_callback');}
    const args={location_id:LOCATION,kinds:[...kinds].sort()};
    return {id:'owner_sub_'+hash(canonical({owner,url:params.delivery.url,name:OWNER_ALERT_EVENT,arguments:args})),args};
  }
  async post(url,body,headers){
    let timer;try{return await Promise.race([this.b.post(url,body,headers),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('timeout')),10000);})]);}finally{clearTimeout(timer);}
  }
  async subscribe(owner,params){
    try{return await this.subscribeChecked(owner,params);}
    catch(e){
      const reason=typeof e?.reason==='string'&&/^[a-z][a-z0-9_]{0,100}$/.test(e.reason)?e.reason:'internal_error';
      this.b.audit('owner_alert_subscribe_rejected_'+reason,'owner_alert_subscription',Number.isSafeInteger(e?.code)?e.code:null);
      throw e;
    }
  }
  async subscribeChecked(owner,params){
    const {id:subscriptionId,args}=this.identity(owner,params);
    try{signingKey(params.delivery.secret);}catch{invalid('invalid_signing_secret');}
    if(params.cursor!==undefined&&params.cursor!==null)invalid('replay_not_supported');
    const requested=params.ttlMs??TTL;if(typeof requested!=='number'||!Number.isFinite(requested)||requested<=0)invalid('invalid_ttl');
    const ttl=Math.max(60000,Math.min(TTL,requested));
    return this.serial(async()=>{
      const row=this.b.db.prepare('SELECT * FROM owner_alert_subscriptions WHERE id=?').get(subscriptionId),old=row?this.b.vault.open(row.state,subscriptionId):null;
      const state={id:subscriptionId,owner,args,url:params.delivery.url,secret:params.delivery.secret,verifiedUntil:0};
      if(old&&old.secret===state.secret&&old.verifiedUntil>this.b.now()&&row.active&&row.expires>this.b.now())state.verifiedUntil=old.verifiedUntil;
      else{
        const challenge=crypto.randomBytes(32).toString('base64url'),body=JSON.stringify({type:'verification',challenge}),started=this.b.now();
        try{
          const response=await this.post(state.url,body,signedHeaders('verify_'+crypto.randomUUID(),body,state,this.b.now()));
          let echo;try{echo=JSON.parse(response.body).challenge;}catch{}
          if(response.status<200||response.status>=300||typeof echo!=='string'||!equal(echo,challenge)||this.b.now()-started>10000)throw new Error('challenge_failed');
        }catch(e){this.b.audit('owner_alert_callback_verification_failed',subscriptionId);throw new RpcError(-32015,'Owner callback verification failed',e.message==='timeout'?'timeout':'challenge_failed');}
        state.verifiedUntil=this.b.now()+300000;
      }
      if(old&&old.secret!==state.secret){state.oldSecret=old.secret;state.rotateUntil=this.b.now()+300000;}
      else if(old?.rotateUntil>this.b.now()){state.oldSecret=old.oldSecret;state.rotateUntil=old.rotateUntil;}
      const expires=this.b.now()+ttl;
      this.tx(()=>{
        // A newly verified callback replaces the prior destination for this owner.
        this.b.db.prepare('UPDATE owner_alert_subscriptions SET active=0 WHERE owner=? AND id<>?').run(owner,subscriptionId);
        this.b.db.prepare("UPDATE owner_alert_deliveries SET status='cancelled',lease_until=NULL,lease_token_hash=NULL WHERE sub_id<>? AND status IN ('pending','inflight')").run(subscriptionId);
        this.b.db.prepare('INSERT INTO owner_alert_subscriptions(id,owner,state,expires,active) VALUES(?,?,?,?,1) ON CONFLICT(id) DO UPDATE SET state=excluded.state,expires=excluded.expires,active=1').run(subscriptionId,owner,this.b.vault.seal(state,subscriptionId),expires);
        this.backfill(subscriptionId,args.kinds);
      });
      this.b.audit('owner_alert_subscribed',subscriptionId);
      return {id:subscriptionId,refreshBefore:new Date(expires).toISOString(),cursor:null,truncated:false};
    });
  }
  async unsubscribe(owner,params){
    const {id:subscriptionId}=this.identity(owner,params);
    return this.serial(()=>this.tx(()=>{
      this.b.db.prepare('UPDATE owner_alert_subscriptions SET active=0 WHERE id=? AND owner=?').run(subscriptionId,owner);
      this.b.db.prepare("UPDATE owner_alert_deliveries SET status='cancelled',lease_until=NULL,lease_token_hash=NULL WHERE sub_id=? AND status IN ('pending','inflight')").run(subscriptionId);
      this.b.audit('owner_alert_unsubscribed',subscriptionId);return {};
    }));
  }
  source(alertId,kind){
    if(!id(alertId)||!KINDS.includes(kind))invalid('owner_alert_reference_invalid');
    const source=this.b.db.prepare('SELECT * FROM server_alerts WHERE id=?').get(alertId);if(!source)invalid('server_alert_not_found');
    const alert=this.b.vault.open(source.payload,alertId),actual=alert.kind??'handoff';
    if(kind!==actual||!id(source.event_id)||alert.event_id!==source.event_id)invalid('owner_alert_reference_mismatch');
    const eventRow=this.b.db.prepare('SELECT payload FROM events WHERE id=?').get(source.event_id);
    if(!eventRow||this.b.vault.open(eventRow.payload,source.event_id).data?.location_id!==LOCATION)invalid('owner_alert_location_required');
    return source;
  }
  verifyReference(row){const source=this.source(row.alert_id,row.kind);if(row.owner!==this.owner||row.source_event_id!==source.event_id||row.source_created!==source.created)invalid('owner_alert_reference_immutable');}
  backfill(subscriptionId,kinds){
    const rows=this.b.db.prepare('SELECT alert_id,kind FROM owner_alert_receipts WHERE owner=? AND acknowledged_at IS NULL ORDER BY created,alert_id').all(this.owner);
    for(const row of rows){
      if(!kinds.includes(row.kind))continue;
      // Renewing an accepted/dead delivery does not create an unbounded replay.
      // A replacement callback gets the same stable event ID for consumer dedupe.
      this.b.db.prepare("INSERT INTO owner_alert_deliveries(alert_id,sub_id,status,due) VALUES(?,?,'pending',?) ON CONFLICT(alert_id,sub_id) DO UPDATE SET status='pending',due=excluded.due,lease_until=NULL,lease_token_hash=NULL WHERE owner_alert_deliveries.status IN ('expired','cancelled') AND owner_alert_deliveries.attempts<?").run(row.alert_id,subscriptionId,this.b.now(),MAX_ATTEMPTS);
    }
  }
  enqueue(alertId,kind){
    const source=this.source(alertId,kind);
    return this.tx(()=>{
      const prior=this.b.db.prepare('SELECT * FROM owner_alert_receipts WHERE alert_id=?').get(alertId);
      if(prior&&(prior.owner!==this.owner||prior.kind!==kind||prior.source_event_id!==source.event_id||prior.source_created!==source.created))invalid('owner_alert_reference_immutable');
      const created=this.b.now();
      const added=this.b.db.prepare('INSERT OR IGNORE INTO owner_alert_receipts(alert_id,owner,kind,source_event_id,source_created,created) VALUES(?,?,?,?,?,?)').run(alertId,this.owner,kind,source.event_id,source.created,created);
      const active=this.b.db.prepare('SELECT * FROM owner_alert_subscriptions WHERE owner=? AND active=1 AND expires>?').all(this.owner,this.b.now());
      for(const sub of active)this.backfill(sub.id,this.b.vault.open(sub.state,sub.id).args.kinds);
      this.b.audit('owner_alert_enqueued',alertId);
      return {stored:true,alert_id:alertId,duplicate:!added.changes,success:false,reason:'owner_alert_queued_not_acknowledged'};
    });
  }
  envelope(row){return {eventId:'owner_evt_'+hash(canonical({alert_id:row.alert_id,owner:row.owner,kind:row.kind,location_id:LOCATION})),name:OWNER_ALERT_EVENT,timestamp:new Date(row.created).toISOString(),data:{alert_id:row.alert_id,location_id:LOCATION,kind:row.kind},cursor:null};}
  reserve(alertId,subscriptionId){return this.tx(()=>{
    const at=this.b.now(),r=this.b.db.prepare('SELECT d.*,s.state,s.owner,s.active,s.expires,r.kind,r.created,r.acknowledged_at,r.source_event_id,r.source_created FROM owner_alert_deliveries d JOIN owner_alert_subscriptions s ON s.id=d.sub_id JOIN owner_alert_receipts r ON r.alert_id=d.alert_id WHERE d.alert_id=? AND d.sub_id=?').get(alertId,subscriptionId);
    if(!r||!['pending','inflight'].includes(r.status)||r.due>at||r.status==='inflight'&&r.lease_until>at)return null;
    const terminal=r.acknowledged_at!==null?'acknowledged':!r.active||r.expires<=at?'expired':r.attempts>=MAX_ATTEMPTS?'dead':null;
    if(terminal){this.b.db.prepare('UPDATE owner_alert_deliveries SET status=?,lease_until=NULL,lease_token_hash=NULL WHERE alert_id=? AND sub_id=?').run(terminal,alertId,subscriptionId);return null;}
    const state=this.b.vault.open(r.state,subscriptionId);
    if(state.owner!==this.owner||r.owner!==this.owner||!state.args.kinds.includes(r.kind))invalid('owner_alert_subscription_scope');
    this.verifyReference(r);
    const token=crypto.randomBytes(32).toString('base64url'),attempt=r.attempts+1,due=at+Math.min(120000,1000*2**(attempt-1));
    this.b.db.prepare("UPDATE owner_alert_deliveries SET status='inflight',attempts=?,due=?,lease_until=?,lease_token_hash=? WHERE alert_id=? AND sub_id=?").run(attempt,due,at+LEASE,hash(token),alertId,subscriptionId);
    return {row:r,state,token,attempt};
  });}
  async drain(){
    if(this.busy)return {busy:true};this.busy=true;let attempted=0;
    try{
      const rows=this.b.db.prepare("SELECT alert_id,sub_id FROM owner_alert_deliveries WHERE status='pending' AND due<=? OR status='inflight' AND lease_until<=? ORDER BY due LIMIT 50").all(this.b.now(),this.b.now());
      for(const row of rows){
        const reserved=this.reserve(row.alert_id,row.sub_id);if(!reserved)continue;attempted++;
        const event=this.envelope(reserved.row),body=JSON.stringify(event);let code=0;
        try{const response=await this.post(reserved.state.url,body,signedHeaders(event.eventId,body,reserved.state,this.b.now()));code=Number.isSafeInteger(response.status)?response.status:0;}catch{}
        const transient=code===0||code===408||code===429||code>=500,status=code>=200&&code<300?'accepted':transient&&reserved.attempt<MAX_ATTEMPTS?'pending':'dead';
        // A callback can read+ack synchronously. Never overwrite that receipt or
        // an unsubscribe/replacement with this earlier HTTP response.
        this.tx(()=>{
          this.b.db.prepare('UPDATE owner_alert_deliveries SET status=?,lease_until=NULL,lease_token_hash=NULL,last_code=?,accepted_at=CASE WHEN ?=\'accepted\' THEN ? ELSE accepted_at END WHERE alert_id=? AND sub_id=? AND status=\'inflight\' AND lease_token_hash=?').run(status,code,status,this.b.now(),row.alert_id,row.sub_id,hash(reserved.token));
          if(code===410)this.b.db.prepare('UPDATE owner_alert_subscriptions SET active=0 WHERE id=?').run(row.sub_id);
        });
        this.b.audit(status==='accepted'?'owner_alert_callback_accepted':status==='pending'?'owner_alert_callback_retry':'owner_alert_callback_dead',row.alert_id,code);
      }
      return {attempted};
    }finally{this.busy=false;}
  }
  metadata(row){
    const deliveries=this.b.db.prepare('SELECT status,attempts,accepted_at,last_code FROM owner_alert_deliveries WHERE alert_id=?').all(row.alert_id);
    return {alert_id:row.alert_id,kind:row.kind,location_id:LOCATION,created_at:new Date(row.created).toISOString(),consumer_read_at:row.read_at===null?null:new Date(row.read_at).toISOString(),consumer_acknowledged_at:row.acknowledged_at===null?null:new Date(row.acknowledged_at).toISOString(),notificationId:row.ack_id,callback_accepted:deliveries.some(d=>d.accepted_at!==null),deliveries,push_delivery_verified:false,owner_read_verified:false};
  }
  list(owner,{limit=20,before=null}={}){
    this.authorize(owner);if(!Number.isSafeInteger(limit)||limit<1||limit>100||before!==null&&(!Number.isSafeInteger(before)||before<0))invalid('invalid_alert_query');
    return {alerts:this.b.db.prepare('SELECT * FROM owner_alert_receipts WHERE owner=? AND created<? ORDER BY created DESC,alert_id DESC LIMIT ?').all(owner,before??Number.MAX_SAFE_INTEGER,limit).map(r=>this.metadata(r)),push_delivery_verified:false,owner_read_verified:false};
  }
  read(owner,alertId){
    this.authorize(owner);if(!id(alertId))invalid('owner_alert_reference_invalid');
    const row=this.b.db.prepare('SELECT * FROM owner_alert_receipts WHERE owner=? AND alert_id=?').get(owner,alertId);if(!row)invalid('owner_alert_not_enqueued');
    this.verifyReference(row);
    if(row.read_at===null){row.read_at=this.b.now();this.b.db.prepare('UPDATE owner_alert_receipts SET read_at=? WHERE alert_id=? AND read_at IS NULL').run(row.read_at,alertId);}
    return this.metadata(row);
  }
  ack(owner,alertId){
    this.authorize(owner);if(!id(alertId))invalid('owner_alert_reference_invalid');
    return this.tx(()=>{
      const r=this.b.db.prepare('SELECT * FROM owner_alert_receipts WHERE owner=? AND alert_id=?').get(owner,alertId);if(!r)invalid('owner_alert_not_enqueued');
      this.verifyReference(r);
      if(r.read_at===null)invalid('owner_alert_read_required');
      const notificationId=r.ack_id??'owner_ack_'+hash(canonical({owner,alert_id:alertId}));
      this.b.db.prepare('UPDATE owner_alert_receipts SET acknowledged_at=COALESCE(acknowledged_at,?),ack_id=COALESCE(ack_id,?) WHERE alert_id=?').run(this.b.now(),notificationId,alertId);
      this.b.db.prepare("UPDATE owner_alert_deliveries SET status='acknowledged',lease_until=NULL,lease_token_hash=NULL WHERE alert_id=? AND status IN ('pending','inflight','accepted')").run(alertId);
      this.b.audit('owner_alert_consumer_acknowledged',alertId);
      return {recorded:true,alert_id:alertId,consumer_acknowledged:true,notificationId,already_acknowledged:r.acknowledged_at!==null,push_delivery_verified:false,owner_read_verified:false};
    });
  }
  status(owner){
    this.authorize(owner);
    return {event:OWNER_ALERT_EVENT,active_subscriptions:this.b.db.prepare('SELECT COUNT(*) AS n FROM owner_alert_subscriptions WHERE owner=? AND active=1 AND expires>?').get(owner,this.b.now()).n,
      alerts:this.b.db.prepare('SELECT COUNT(*) AS n FROM owner_alert_receipts WHERE owner=?').get(owner).n,
      unacknowledged:this.b.db.prepare('SELECT COUNT(*) AS n FROM owner_alert_receipts WHERE owner=? AND acknowledged_at IS NULL').get(owner).n,
      deliveries:this.b.db.prepare('SELECT status,COUNT(*) AS count FROM owner_alert_deliveries GROUP BY status').all(),max_attempts:MAX_ATTEMPTS,max_subscription_ttl_ms:TTL,push_delivery_verified:false,owner_read_verified:false};
  }
}

