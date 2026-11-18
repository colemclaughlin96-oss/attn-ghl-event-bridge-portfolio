import test from 'node:test';
import assert from 'node:assert/strict';
import {fixture,C,owner,result} from './helpers/prospect-fixture.mjs';
import {LOCATION,CALENDAR,BOOKING_LINK} from '../src/sales-policy.mjs';
const appointment=(id='appt',extra={})=>({id,contactId:C,locationId:LOCATION,calendarId:CALENDAR,appointmentStatus:'confirmed',...extra});
function withAppointments(snapshot,appointments){snapshot.reads.find(r=>r.operationId==='get-appointments-for-contact').result.data.events=appointments;return snapshot;}
const state=f=>f.l.contactState(f.l.read('prospect_entries',f.l.key(C)));

test('a valid active appointment on another calendar permits a complete pricing answer',async t=>{
  const f=fixture(t),id=await f.event(),appointments=[appointment('elsewhere',{calendarId:'another_calendar'})],s=withAppointments(f.snap(),appointments),p=f.prepare(id,s);
  assert.equal(p.prepared,true);assert.deepEqual(p.booking_restrictions,['active_other_calendar']);assert.equal(state(f).handoff,null);
  const permit=f.begin(id,p,withAppointments(f.snap([f.m()],{detail:f.m()}),appointments));assert.equal(permit.permitted,true);
  const recorded=f.l.record(owner,id,result(f,permit));assert.equal(recorded.phase,'sent');assert.equal(state(f).handoff,null);
});
test('multiple valid active bookings permit an unrelated answer without creating a contact-wide handoff',async t=>{
  const f=fixture(t),id=await f.event(),m=f.m('in_1',{body:'How does your Google Ads service work?'}),appointments=[appointment('a'),appointment('b')],p=f.prepare(id,withAppointments(f.snap([m]),appointments));
  assert.deepEqual(p.booking_restrictions,['multiple_active_bookings']);assert.equal(state(f).handoff,null);f.restart();
  const draft={...f.draft,intent:'answer',text:'We build and manage Google Ads around the work you want more of.'};assert.equal(f.begin(id,p,withAppointments(f.snap([m],{detail:m}),appointments),draft).permitted,true);assert.equal(state(f).handoff,null);
});
test('conflicting booking links and appointment-specific drafts are denied before a send permit',async t=>{
  for(const draft of [{intent:'booking_link',text:'Choose a time here: '+BOOKING_LINK},{intent:'answer',text:'Here is a booking link: '+BOOKING_LINK},{intent:'existing_meeting',text:'Your meeting is tomorrow at noon.'}]){
    const f=fixture(t),id=await f.event(),appointments=[appointment('a'),appointment('b',{calendarId:'another_calendar'})],p=f.prepare(id,withAppointments(f.snap(),appointments));
    const denied=f.begin(id,p,withAppointments(f.snap([f.m()],{detail:f.m()}),appointments),{...f.draft,...draft});
    assert.deepEqual(denied,{permitted:false,reason:'booking_action_requires_review',retryable:true,booking_restrictions:['active_other_calendar','multiple_active_bookings']});assert.equal(f.l.rows(C)[0].attemptId,undefined);assert.equal(state(f).handoff,null);
  }
});
test('a new conflict discovered at preflight blocks booking but permits a revised ordinary answer',async t=>{
  const f=fixture(t),id=await f.event(),p=f.prepare(id),appointments=[appointment('a'),appointment('b')],fresh=withAppointments(f.snap([f.m()],{detail:f.m()}),appointments);
  assert.deepEqual(p.booking_restrictions,[]);const denied=f.begin(id,p,fresh,{...f.draft,intent:'booking_link',text:BOOKING_LINK});assert.equal(denied.reason,'booking_action_requires_review');assert.equal(f.l.rows(C)[0].attemptId,undefined);
  assert.equal(f.begin(id,p,fresh).permitted,true);assert.equal(state(f).handoff,null);
});
test('valid closed or historical appointments on other calendars do not restrict a new booking link',async t=>{
  const f=fixture(t),id=await f.event(),appointments=['cancelled','showed','noshow','invalid','completed'].map((appointmentStatus,i)=>appointment('old_'+i,{calendarId:'old_calendar',appointmentStatus})),p=f.prepare(id,withAppointments(f.snap(),appointments));
  assert.deepEqual(p.booking_restrictions,[]);assert.equal(f.begin(id,p,withAppointments(f.snap([f.m()],{detail:f.m()}),appointments),{...f.draft,intent:'booking_link',text:'You can choose a time here: '+BOOKING_LINK}).permitted,true);assert.equal(state(f).handoff,null);
});
test('cross-contact, wrong-location and invalid appointment identity/status evidence still fail closed',async t=>{
  for(const invalid of [appointment('a',{contactId:'another_contact'}),appointment('a',{locationId:'another_location'}),appointment(''),appointment('a',{calendarId:null}),appointment('a',{appointmentStatus:'unknown'}),null]){
    const f=fixture(t),id=await f.event();const p=f.prepare(id,withAppointments(f.snap(),[invalid]));assert.equal(p.prepared,false);assert.equal(p.reason,'appointment_conflict');assert.equal(f.l.rows(C).some(r=>r.attemptId),false);assert.ok(state(f).handoff);
  }
});
test('one verified current booking still prevents another booking link under any answer intent',async t=>{
  const f=fixture(t),id=await f.event(),appointments=[appointment()],p=f.prepare(id,withAppointments(f.snap(),appointments));assert.deepEqual(p.booking_restrictions,[]);
  assert.throws(()=>f.begin(id,p,withAppointments(f.snap([f.m()],{detail:f.m()}),appointments),{...f.draft,intent:'answer',text:BOOKING_LINK}),e=>e.reason==='already_booked');assert.equal(f.l.rows(C)[0].attemptId,undefined);
});

