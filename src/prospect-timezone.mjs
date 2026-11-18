// Evidence, not a reply gate. Unknown/conflicting zones hold scheduled outreach
// and booking interpretation, while genuine active inbound answers remain eligible.
import {canonical,hash} from './security.mjs';
import {inHours,PROVIDER} from './sales-policy.mjs';
export const TZ_APPROVAL='cole_assume_pacific_20261020';
export const absentTimezone=v=>v===undefined||v===null||typeof v==='string'&&!v.trim();
export function canonicalZone(v){
 if(typeof v!=='string'||!(v==='UTC'||v.includes('/')))return null;
 try{return new Intl.DateTimeFormat('en-US',{timeZone:v}).resolvedOptions().timeZone;}catch{return null;}
}
export function sendingTimezone(contact,messages,contactRead,messageReads,completedAt){
 const missing=absentTimezone(contact.timezone),saved=missing?null:canonicalZone(contact.timezone);
 let invalid=!missing&&!saved;
 const names={pacific:'America/Los_Angeles',mountain:'America/Denver',central:'America/Chicago',eastern:'America/New_York'};
 const statements=[];
 for(const m of messages){
  if(m.direction!=='inbound'||m.conversationProviderId!==PROVIDER||m.messageType!=='TYPE_CUSTOM_PROVIDER_SMS'||typeof m.body!=='string')continue;
  const body=m.body.trim();
  const regional=body.match(/^(?:I am|I'm|We are|We're|My timezone is|My time zone is) (?:in )?(Pacific|Mountain|Central|Eastern)(?: time| timezone| time zone)?[.!]?$/i);
  const explicit=body.match(/^(?:My|Our) time ?zone is ([A-Za-z_]+\/[A-Za-z_+\/-]+|UTC)[.!]?$/i);
  if(!regional&&!explicit)continue;
  const value=regional?names[regional[1].toLowerCase()]:explicit[1],zone=canonicalZone(value);
  if(!zone){invalid=true;continue;}
  statements.push({value,zone,messageId:m.id,dateAdded:m.dateAdded});
 }
 const zones=new Set(statements.map(x=>x.zone));
 const conflict=zones.size>1||!!saved&&[...zones].some(z=>z!==saved);
 const base={locationId:contact.locationId,contactId:contact.id,observed_at:completedAt,reply_policy:'active_inbound_timezone_independent',evidenceRef:'mcp_'+hash(canonical({contactRead,messageReads}))};
 if(invalid||conflict)return {...base,value:null,source:'unresolved',status:conflict?'conflicting':'invalid',reason:conflict?'timezone_conflict':'invalid_timezone',provisional:false,bookingConfirmed:false};
 statements.sort((a,b)=>b.dateAdded.localeCompare(a.dateAdded)||b.messageId.localeCompare(a.messageId));
 if(saved){
  const statement=statements[0];
  return {...base,value:contact.timezone,source:'ghl_get_contact',status:'known',field:'contact.timezone',provisional:false,bookingConfirmed:!!statement,verified_at:completedAt,...(statement?{messageId:statement.messageId,statement_at:statement.dateAdded}:{} )};
 }
 if(statements.length){
  const s=statements[0];
  return {...base,value:s.value,source:'contact_statement',status:'known',provisional:false,bookingConfirmed:true,field:'message.body',messageId:s.messageId,statement_at:s.dateAdded,verified_at:completedAt};
 }
 return {...base,value:null,source:'unresolved',status:'missing',reason:'reliable_timezone_unknown',provisional:false,bookingConfirmed:false,field:'contact.timezone',observed_missing:true};
}
export function sameTimezone(a,b){return !!a&&!!b&&canonicalZone(a.value)===canonicalZone(b.value)&&a.source===b.source&&a.status===b.status&&!!a.provisional===!!b.provisional&&!!a.bookingConfirmed===!!b.bookingConfirmed&&(!a.bookingConfirmed||a.messageId===b.messageId&&a.statement_at===b.statement_at);}
export function scheduledOutboundEligibility(timezone,now){
 if(!timezone||timezone.provisional||timezone.status!=='known'||!canonicalZone(timezone.value))return {allowed:false,reason:timezone?.reason??'reliable_timezone_unknown'};
 if(!inHours(now,timezone.value)||!inHours(now+15000,timezone.value))return {allowed:false,reason:'outside_local_hours'};
 return {allowed:true,timezone:timezone.value};
}


