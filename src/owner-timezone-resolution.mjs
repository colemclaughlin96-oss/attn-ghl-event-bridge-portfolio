// Operator-only local CLI; deliberately not imported by server.mjs or any MCP tool.
// No HTTP endpoint, environment startup toggle, caller-chosen target or generic reset.
import {DatabaseSync} from 'node:sqlite';
import {pathToFileURL} from 'node:url';
import {Vault,canonical,hash} from './security.mjs';
import {LOCATION,PROVIDER,blocked} from './sales-policy.mjs';
export const TARGET=Object.freeze({
 contactId:'portfolio_fixture_06',conversationId:'portfolio_fixture_07',
 eventId:'evt_49b932ba12a8d4702bfd90b3773ca8453c0b7ac9e235aceb910a869c25160646',
 owner:'AIGTGL0dgkeEVqAHLY3xa3TrJlRaJhwZImSDkqA18vYkFCWxKtfC8B',
 reason:'reliable_timezone_unknown',approval:'cole_assume_pacific_20261020',
 resolution:'owner_timezone_7969_20261020',
 messages:[{id:'portfolio_fixture_15',date:'2026-10-20T22:51:03.610Z',body:'Test'},{id:'portfolio_fixture_16',date:'2026-10-20T22:51:27.258Z',body:'Hello?'}]
});
const key=(id='')=>hash(canonical({location:LOCATION,contact:TARGET.contactId,id}));
const read=(db,vault,table,id)=>{const r=db.prepare(`SELECT payload FROM ${table} WHERE id=?`).get(id);return r?vault.open(r.payload,id):null;};
const write=(db,vault,table,id,x)=>db.prepare(`INSERT INTO ${table}(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`).run(id,vault.seal(x,id));
function capture(db,vault){
 const events=db.prepare('SELECT id,payload FROM events ORDER BY id').all().map(r=>vault.open(r.payload,r.id)).filter(e=>e.data.contact_id===TARGET.contactId);
 const messages=db.prepare('SELECT id,payload FROM prospect_messages ORDER BY id').all().map(r=>({key:r.id,value:vault.open(r.payload,r.id)})).filter(r=>r.value.contactId===TARGET.contactId);
 const claims=events.map(e=>db.prepare('SELECT * FROM processing WHERE event_id=?').get(e.eventId)??null);
 const hasTable=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='prospect_owner_resolutions'").get();
 return {entry:read(db,vault,'prospect_entries',key()),state:read(db,vault,'prospect_contacts',key()),alert:read(db,vault,'prospect_alerts',key()),messages,events,claims,prior:hasTable?read(db,vault,'prospect_owner_resolutions',key()):null};
}
function validate(x,owner){
 if(owner!==TARGET.owner)blocked('owner_identity_required');
 if(x.prior)blocked('owner_resolution_already_consumed');
 if(!x.entry||x.entry.contactId!==TARGET.contactId||x.entry.conversationId!==TARGET.conversationId||x.entry.timezone!=null||x.entry.resumeAfter)blocked('exact_missing_timezone_entry_required');
 const s=x.state,a=x.alert;
 if(!s||s.contactId!==TARGET.contactId||s.enrollmentHash!==hash(canonical(x.entry)))blocked('exact_contact_state_required');
 if(s.suppressed||s.externalLock||s.manualUntil!==0)blocked('independent_contact_hold');
 if(!s.handoff||s.handoff.eventId!==TARGET.eventId||s.handoff.reason!==TARGET.reason||s.handoff.status!=='chat_alert_prepared'||Object.keys(s.handoff).some(k=>!['eventId','reason','at','status'].includes(k)))blocked('exact_timezone_handoff_required');
 if(!a||a.contact_id!==TARGET.contactId||a.event_id!==TARGET.eventId||a.reason!==TARGET.reason)blocked('exact_timezone_alert_required');
 if(x.events.length!==1||x.events[0].eventId!==TARGET.eventId||x.events[0].data.location_id!==LOCATION||x.events[0].data.source!=='loop_workflow')blocked('new_or_different_contact_event');
 if(x.claims.length!==1||!x.claims[0]||x.claims[0].owner!==owner||x.claims[0].outcome!=='escalated'||!x.claims[0].completed)blocked('completed_escalated_owner_claim_required');
 if(x.messages.length!==TARGET.messages.length)blocked('exact_historical_rows_required');
 for(const {key:k,value:r} of x.messages){
  const expected=TARGET.messages.find(m=>m.id===r.messageId);
  if(!expected||k!==key(r.messageId)||r.dateAdded!==expected.date||!['pending','prepared'].includes(r.phase)||Object.keys(r).some(k=>/^attempt/i.test(k)||k==='outboundId'))blocked('consumed_or_different_message');
  if(r.fingerprint!==hash(canonical({id:expected.id,date:expected.date,body:expected.body,attachments:null,provider:PROVIDER,type:'TYPE_CUSTOM_PROVIDER_SMS',direction:'inbound'})))blocked('historical_message_changed');
  if(!Array.isArray(r.receiptIds)||r.receiptIds.length!==1||r.receiptIds[0]!==TARGET.eventId||r.eventId!==undefined&&r.eventId!==TARGET.eventId)blocked('newer_message_claim');
 }
}
export function inspectResolution(db,vault,owner){
 const state=capture(db,vault);let reason=null;try{validate(state,owner);}catch(e){reason=e.reason??'inspection_failed';}
 return {resolution:TARGET.resolution,contact_id:TARGET.contactId,event_id:TARGET.eventId,eligible:reason===null,reason,expected_state_hash:hash(canonical(state)),message_phases:state.messages.map(r=>({id:r.value.messageId,phase:r.value.phase})),handoff_reason:state.state?.handoff?.reason??null,has_prior_resolution:!!state.prior,mutated:false};
}
export function applyResolution(db,vault,{owner,approval,expectedStateHash,now=Date.now}={}){
 if(approval!==TARGET.approval||typeof expectedStateHash!=='string'||!/^[a-f0-9]{64}$/.test(expectedStateHash))blocked('exact_owner_approval_and_inspection_required');
 db.exec('BEGIN IMMEDIATE');
 try{
  const before=capture(db,vault);validate(before,owner);
  if(hash(canonical(before))!==expectedStateHash)blocked('state_changed_since_inspection');
  const at=now();if(!Number.isSafeInteger(at)||at<=before.claims[0].completed||at<=Date.parse(before.entry.notBefore))blocked('resolution_clock_invalid');
  const resumeAfter=new Date(at).toISOString(),entry={...before.entry,resumeAfter,ownerResolutionRef:TARGET.resolution};
  const state={...before.state,handoff:null,enrollmentHash:hash(canonical(entry))};
  // Preserve entire original encrypted state/alert/rows and acknowledgement. No audit rewrite.
  const receipt={resolution:TARGET.resolution,approval,owner,resolved_at:resumeAfter,resume_after:resumeAfter,before,before_hash:expectedStateHash,releasedAlertHash:hash(canonical(before.alert)),no_send:true};
  db.exec('CREATE TABLE IF NOT EXISTS prospect_owner_resolutions(id TEXT PRIMARY KEY,payload TEXT NOT NULL)');
  write(db,vault,'prospect_owner_resolutions',key(),receipt);
  for(const r of before.messages)write(db,vault,'prospect_messages',r.key,{...r.value,phase:'skipped_owner_timezone_resolution',previousPhase:r.value.phase,ownerResolutionRef:TARGET.resolution,resolvedAt:at});
  write(db,vault,'prospect_entries',key(),entry);write(db,vault,'prospect_contacts',key(),state);
  db.prepare('INSERT INTO audit(at,action,ref,code) VALUES(?,?,?,NULL)').run(at,'owner_timezone_hold_resolved',hash(TARGET.eventId).slice(0,16));
  db.exec('COMMIT');return {resolved:true,resolution:TARGET.resolution,contact_id:TARGET.contactId,event_id:TARGET.eventId,resume_after:resumeAfter,historical_rows_skipped:before.messages.length,no_send:true};
 }catch(e){db.exec('ROLLBACK');throw e;}
}
export function main(args=process.argv.slice(2),env=process.env){
 const inspect=args.length===1&&args[0]==='--inspect';
 const apply=args.length===5&&args[0]==='--apply'&&args[1]==='--expected-state'&&args[3]==='--owner-approval';
 if(!inspect&&!apply)throw new Error('Use --inspect OR --apply --expected-state <inspection hash> --owner-approval cole_assume_pacific_20261020');
 const db=new DatabaseSync(env.DB_PATH||'/data/bridge.sqlite',{readOnly:inspect}),vault=new Vault(env.VAULT_KEY);
 db.exec('PRAGMA busy_timeout=5000');
 try{const result=inspect?inspectResolution(db,vault,env.SITE_OWNER_SUBJECT):applyResolution(db,vault,{owner:env.SITE_OWNER_SUBJECT,approval:args[4],expectedStateHash:args[2]});console.log(JSON.stringify(result));return result;}finally{db.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){try{main();}catch(e){console.error(JSON.stringify({resolved:false,reason:e.reason??'resolution_failed'}));process.exitCode=1;}}


