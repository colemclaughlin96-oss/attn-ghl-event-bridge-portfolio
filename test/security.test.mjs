import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {Vault,publicV4,resolveCallback,makeSecurePost,callbackURL,signingKey} from '../src/security.mjs';
import {proxyMcp} from '../sites-facade/proxy.mjs';

test('AES-GCM binds ciphertext to its record and detects tampering',()=>{
  const v=new Vault(Buffer.alloc(32,1).toString('base64')),sealed=v.seal({secret:'private'},'record1');
  assert.deepEqual(v.open(sealed,'record1'),{secret:'private'});assert.throws(()=>v.open(sealed,'record2'));
  const bytes=Buffer.from(sealed,'base64');bytes[30]^=1;assert.throws(()=>v.open(bytes.toString('base64'),'record1'));
});
test('callback URL and IP defenses reject private, local and alternate encodings',async()=>{
  for(const ip of ['0.0.0.0','10.1.2.3','127.0.0.1','169.254.169.254','172.16.0.1','192.168.1.1','100.64.0.1','198.18.1.1','192.0.2.1','203.0.113.1','224.1.1.1','255.255.255.255','::1','::ffff:127.0.0.1'])assert.equal(publicV4(ip),false,ip);
  for(const url of ['http://callback.example.com','https://127.0.0.1','https://2130706433','https://0x7f000001','https://[::1]','https://user:pass@callback.example.com','https://localhost','https://callback.example.com:444','https://callback.example.com/#fragment'])assert.throws(()=>callbackURL(url));
  await assert.rejects(resolveCallback('https://callback.example.com/',async()=>['8.8.8.8','10.0.0.1']));
  assert.equal((await resolveCallback('https://callback.example.com/',async()=>['8.8.8.8'])).address,'8.8.8.8');
});
test('connection pins validated IP while preserving hostname, rechecks DNS, and never follows redirects',async()=>{
  let resolves=0,requests=0;const seen=[];
  const post=makeSecurePost({resolver:async()=>{resolves++;return resolves===1?['8.8.8.8']:['127.0.0.1'];},request:(opts,callback)=>{
    requests++;seen.push(opts);const req=new EventEmitter();req.destroy=()=>req.emit('close');req.end=()=>{
      const res=new EventEmitter();res.statusCode=302;callback(res);queueMicrotask(()=>{res.emit('end');req.emit('close');});
    };return req;
  }});
  assert.equal((await post('https://callback.example.com/path?x=1','{}',{})).status,302);
  assert.equal(requests,1);assert.equal(seen[0].servername,'callback.example.com');assert.equal(seen[0].hostname,'callback.example.com');assert.equal(seen[0].agent,false);
  seen[0].lookup('callback.example.com',{},(err,address,family)=>{assert.equal(err,null);assert.equal(address,'8.8.8.8');assert.equal(family,4);});
  await assert.rejects(post('https://callback.example.com/path','{}',{}));assert.equal(requests,1);assert.equal(resolves,2);
});
test('signing secrets enforce protocol byte lengths',()=>{
  for(const n of [0,1,23,65])assert.throws(()=>signingKey('whsec_'+Buffer.alloc(n).toString('base64')));
  for(const n of [24,32,64])assert.equal(signingKey('whsec_'+Buffer.alloc(n).toString('base64')).length,n);
});
test('Sites proxy fails closed for missing/unapproved identities and preserves trusted principal only',async()=>{
  let seen;const config={SITE_OWNER_SUBJECT:'owner_fixture',BRIDGE_ORIGIN:'https://private-delivery.example.com',PROXY_TOKEN:'s'.repeat(40)};
  const make=owner=>new Request('https://new-private-site.example.com/mcp',{method:'POST',headers:owner?{'oai-authenticated-user-id':owner,'x-attn-principal':'attacker'}:{},body:'{"jsonrpc":"2.0","id":1,"method":"server/discover"}'});
  const fetcher=async(url,options)=>{seen={url:String(url),options};return Response.json({jsonrpc:'2.0',id:1,result:{}});};
  assert.equal((await proxyMcp(make(),config,fetcher)).status,403);
  assert.equal((await proxyMcp(make('other'),config,fetcher)).status,403);assert.equal(seen,undefined);
  assert.equal((await proxyMcp(make('owner_fixture'),config,fetcher)).status,200);
  assert.equal(seen.options.headers['x-attn-principal'],'owner_fixture');assert.equal(seen.options.redirect,'manual');
  assert.equal(seen.url,'https://private-delivery.example.com/mcp');
});


