import test from 'node:test';
import assert from 'node:assert/strict';
import {explicitOptOut,consentEvidence} from '../src/sales-policy.mjs';

test('direct STOP variants, contact-stop requests, wrong numbers and clear refusals remain opt-outs',()=>{
  for(const text of ['STOP',' stop ','Stop!!!','Please stop.','STOP, please','STOPALL','unsubscribe','Unsubscribe me please','Could you please unsubscribe me?','Please stop texting me','Don’t contact me again','Do not message us','Stop sending me these messages','Remove me','Please take me off your mailing list','Leave me alone','Wrong number','Sorry, you have the wrong number','Not interested','I am not interested anymore','No thanks','No, thank you'])assert.equal(explicitOptOut(text),true,text);
});
test('advertising, spending, opt-out questions and quoted examples do not become deterministic opt-outs',()=>{
  for(const text of ['How do I stop wasting money on ads?','How can we stop spending so much on ads?','Can I stop Google Ads whenever I want?','I want to stop my old ads and use Google','My leads say not interested. How can we improve?','I am not interested in Facebook ads. What about Google?','What happens if someone gives a wrong number?','What does STOP mean?','A lead wrote "STOP" yesterday','How do I unsubscribe from my old agency?','Can I unsubscribe if I change my mind?','Remove me from the appointment and book next week','Can you stop my advertising spend?','No','I do not want your emails to go to spam'])assert.equal(explicitOptOut(text),false,text);
  assert.equal(explicitOptOut(null),false);
  assert.equal(explicitOptOut('A lead wrote this:\nSTOP'),false);
});
test('a leading direct STOP remains explicit when followed by an explanation',()=>{
  for(const text of ['STOP. Seriously.','STOP! I said stop.','Please stop. I already asked.','Stop, I do not want any more messages.','Stop, I don’t want your texts anymore.','Please remove my number','Could you please remove my phone number from your list?','STOP. A lead wrote this:\nPlease stop texting me'])assert.equal(explicitOptOut(text),true,text);
});
test('courtesy, a clear stop-contact clause and direct unsubscribe intent do not hide opt-outs',()=>{
  for(const text of ['Thanks. STOP',"STOP, don't text me again",'No thanks, not interested','I want to unsubscribe'])assert.equal(explicitOptOut(text),true,text);
  for(const text of ['Thanks. A lead wrote this:\nSTOP','A lead wrote this:\nThanks. STOP','I want to unsubscribe from my old agency','No thanks, not interested in Facebook ads. What about Google?'])assert.equal(explicitOptOut(text),false,text);
});
test('wrong-number advertising context and reported multiline requests require contextual assessment',()=>{
  for(const text of ['Wrong number on my landing page. Can you fix it?','Wrong number of leads this week','You have the wrong number on the ad','A lead wrote this:\nPlease stop texting me','My customer said:\nRemove me from your list','A client replied:\nWrong number','Remove my number from the appointment and book next week'])assert.equal(explicitOptOut(text),false,text);
  for(const text of ['Wrong number. This is not Steve.','Sorry, you have the wrong number','You are texting the wrong number','Wrong number, thanks'])assert.equal(explicitOptOut(text),true,text);
});
test('consent evidence distinguishes a confirmed block from unavailable or malformed state',()=>{
  for(const contact of [{dnd:true},{dnd:false,dndSettings:{SMS:{status:'active'}}},{dndSettings:{SMS:{status:'active'}}}])assert.deepEqual(consentEvidence(contact),{status:'blocked',reason:'confirmed_dnd'});
  for(const contact of [{},{dnd:null},{dnd:'false'},{dnd:false,dndSettings:null},{dnd:false,dndSettings:[]},{dnd:false,dndSettings:{SMS:null}},{dnd:false,dndSettings:{SMS:{}}}])assert.deepEqual(consentEvidence(contact),{status:'unknown',reason:'consent_evidence_unknown'});
  for(const contact of [{dnd:false},{dnd:false,dndSettings:{}},{dnd:false,dndSettings:{SMS:{status:'inactive'},Email:{status:'inactive'}}}])assert.deepEqual(consentEvidence(contact),{status:'allowed',reason:null});
});

