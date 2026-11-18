import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {LOCATION,PROVIDER,CALENDAR,BOOKING_LINK,CHECKOUT_LINK,PRICING} from '../src/sales-policy.mjs';
import {EXCLUSIONS} from '../src/prospect-v1.mjs';
import {LIMITS} from '../src/inbound-runner.mjs';

const read = name => readFileSync(new URL('../docs/'+name, import.meta.url),'utf8');
const prompt = read('INBOUND-RELIABILITY-PROMPT.txt');
const original = read('PROSPECT-V1-NATIVE-TASK-PROPOSED.txt');
const block = prompt.match(/<business_facts>\s*([\s\S]*?)\s*<\/business_facts>/);
const facts = JSON.parse(block?.[1] ?? 'null');

// These are policy-artifact drift checks. They do not evaluate model reply quality.
test('effective prompt business facts match executable configuration exactly',()=>{
  assert.equal((prompt.match(/<business_facts>/g)||[]).length,1);
  assert.equal(facts.location_id,LOCATION);
  assert.equal(facts.provider_id,PROVIDER);
  assert.equal(facts.calendar_id,CALENDAR);
  assert.equal(facts.booking_link,BOOKING_LINK);
  assert.equal(facts.checkout_link,CHECKOUT_LINK);
  assert.deepEqual(facts.pricing,PRICING);
  for(const p of Object.values(facts.pricing)) assert.equal(p.initial,p.monthly+p.setup);
  assert.equal(facts.currency,'USD');
  assert.equal(facts.ad_spend_additional,true);
  assert.equal(facts.upfront,'setup plus first month');
  assert.equal(facts.renewal,'monthly until canceled');
  assert.equal(facts.review_minutes,30);
  const links = new Set(prompt.match(/https?:\/\/[^"\s]+/g));
  assert.deepEqual([...links].sort(),[BOOKING_LINK,CHECKOUT_LINK].sort());
});

test('effective scope cannot advertise unavailable booking, email or timer capabilities',()=>{
  assert.equal(facts.booking_writes_enabled,false);
  assert.equal(facts.autonomous_email_enabled,false);
  assert.equal(facts.new_followups_enabled,false);
  for(const excluded of EXCLUSIONS) assert.ok(prompt.includes(excluded.contactId));
  assert.match(prompt,/This version cannot create, reschedule or cancel appointments/);
  assert.match(prompt,/Existing workflows own confirmations and reminders/);
  assert.match(prompt,/No email replies, fallback channel, new subscription or cross-chat\/Jarvis notification/);
  assert.match(prompt,/Local development adapters simulate these boundaries/);
  assert.match(prompt,/It is not installed in the live native task/);
});

test('timezone rule separates inbound answers, booking confirmation and scheduled outreach',()=>{
  assert.match(prompt,/active inbound question may receive an otherwise eligible answer regardless of missing, invalid, conflicting or changed timezone, international phone number, or current local hour/);
  assert.match(prompt,/Timezone problems alone never create a human handoff or suppress that answer/);
  assert.match(prompt,/Do not ask timezone before answering an unrelated question/);
  assert.match(prompt,/confirm it before interpreting an ambiguous requested time or creating an appointment/);
  assert.match(prompt,/Unknown or conflicting timezone holds timezone-dependent scheduled outbound work/);
  assert.match(prompt,/8am inclusive–7pm exclusive/);
  // Ban old directives that would silently reintroduce blanket inbound suppression.
  assert.doesNotMatch(prompt,/Unknown reliable timezone means hold outbound sending|invalid\/conflicting timezone.*persist a handoff|No later polling when a GHL message is not visible|Changed thread needs a later genuine event|Approved prices must exactly match priceAnswer texts/);
});

test('complete answers and bounded recovery do not authorize a duplicate send',()=>{
  assert.match(prompt,/Coalesce the current eligible pending inbound messages/);
  assert.match(prompt,/Do not mark a question answered merely because it was read or drafted/);
  assert.match(prompt,/new inbound message changes context, regenerate with that context/);
  assert.match(prompt,/At most one bounded regeneration/);
  assert.match(prompt,/malformed or explicitly truncated output/);
  assert.match(prompt,/Do not use punctuation alone as proof of completion/);
  assert.match(prompt,/Once consumed, a tool error, timeout or approval interruption never permits an automatic second send/);
  assert.match(prompt,/Never require the customer to send another message merely/);
});

test('human and channel protection remain immediately before the send boundary',()=>{
  assert.match(prompt,/Confirmed DND, genuine STOP\/unsubscribe requests, wrong-number messages and clear refusals suppress sending immediately/);
  assert.match(prompt,/at least 24 hours after his latest reply, extended by later replies/);
  assert.match(prompt,/Unresolved human handoffs remain paused until explicitly resolved/);
  assert.match(prompt,/Immediately before consuming a send attempt, refresh live messages, consent\/DND, manual takeover, client\/payment status and appointments/);
  assert.match(prompt,/A new STOP or Cole reply must cancel the queued draft/);
  assert.match(prompt,/Only a successful notification receipt permits/);
  assert.match(prompt,/Failed or unavailable handoff must be visible internally/);
  assert.match(prompt,/"Sent" is not proof of phone delivery/);
});

test('compacted policy retains Cole voice and business scope without appending obsolete policy',()=>{
  assert.match(prompt,/usually 1–2 sentences and 10–35 words, with more when a complete answer needs it/);
  assert.match(prompt,/Ask at most one useful question/);
  assert.match(prompt,/Answer the actual question before steering toward a booking or sale/);
  assert.match(prompt,/without claiming to be Cole/);
  assert.match(prompt,/Do not make someone book to learn pricing/);
  assert.match(prompt,/Do not invent minimum budgets, guarantees, discounts, territory availability, refunds or cancellation notice periods/);
  assert.doesNotMatch(prompt,/OWNER POLICY - VERBATIM ORIGINAL BYTES FOLLOW|OWNER'S FULL INSTRUCTIONS FOLLOW/);
  assert.ok(Buffer.byteLength(prompt)<Buffer.byteLength(original)*0.6,'effective prompt must remain at least 40% smaller than the baseline native prompt');
});

test('documented development bounds and generation contract match the implemented runner',()=>{
  assert.deepEqual(LIMITS,{budgetMs:45000,readMs:8000,modelMs:15000,sendMs:8000,visibilityAttempts:3,contextRefreshes:2,generationAttempts:2});
  assert.match(prompt,/45 seconds per run, 8 seconds per evidence read, 15 seconds per generation and 8 seconds per send/);
  assert.match(prompt,/three visibility checks with 250ms then 500ms waits, two generation attempts per context, and two context refreshes/);
  assert.match(prompt,/finishReason:"stop"\|"completed",complete:true/);
  assert.match(prompt,/answersMessageId is the prepared primary ID and answersMessageIds are the exact prepared batch IDs/);
  assert.match(prompt,/Metadata must represent the actual generation result, not a forced success value/);
});

test('compacting effective policy leaves original owner bytes intact',()=>{
  const preservation=JSON.parse(read('policy/POLICY-PRESERVATION.json'));
  const check=(name,bytes,sha)=>{
    const buf=readFileSync(new URL('../docs/policy/'+name,import.meta.url));
    assert.equal(buf.length,bytes);
    assert.equal(createHash('sha256').update(buf).digest('hex'),sha);
  };
  check('COLE-OWNER-POLICY-2026-10-20.txt',preservation.owner_policy_bytes,preservation.owner_policy_sha256);
  check('COLE-SALES-INSTRUCTIONS-2026-10-20.txt',preservation.attachment_bytes,preservation.attachment_sha256);
});

