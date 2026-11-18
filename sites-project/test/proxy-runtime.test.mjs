import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { test } from 'node:test';

// Exercise the real Cloudflare runtime, including Request/fetch option support.
// All identities and tokens below are fixtures. No external network is used.
const require = createRequire(import.meta.url);
const { Miniflare } = createRequire(require.resolve('wrangler/package.json'))('miniflare');
const proxySource = await readFile(new URL('../proxy.mjs', import.meta.url), 'utf8');
const fixtureToken = 'fixture-token-not-a-secret-000000000000000000';

test('Sites proxy preserves authentication and rejects redirects in workerd', async (t) => {
  const requests = [];
  const mf = new Miniflare({
    modules: true,
    compatibilityDate: '2026-05-15',
    script: proxySource + '\nexport default { fetch(request, env) { return proxyMcp(request, env); } };',
    bindings: {
      BRIDGE_ORIGIN: 'https://bridge.invalid',
      PROXY_TOKEN: fixtureToken,
      SITE_OWNER_SUBJECT: 'fixture-owner',
    },
    outboundService: async (request) => {
      const body = JSON.parse(await request.text());
      requests.push({
        url: request.url,
        authorization: request.headers.get('authorization'),
        principal: request.headers.get('x-attn-principal'),
        body,
      });
      if (body.redirect) {
        return new Response(null, { status: body.redirect, headers: { location: 'https://untrusted.invalid/' } });
      }
      return Response.json({ jsonrpc: '2.0', id: body.id, result: { tools: [] } });
    },
  });
  const call = (body, owner = 'fixture-owner') => mf.dispatchFetch('https://site.invalid/mcp', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(owner ? { 'oai-authenticated-user-id': owner } : {}) },
    body: JSON.stringify(body),
  });
  try {
    await t.test('authenticated tool discovery reaches only the configured backend', async () => {
      const response = await call({ jsonrpc: '2.0', id: 1, method: 'tools/list' });
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), { jsonrpc: '2.0', id: 1, result: { tools: [] } });
      assert.equal(requests.length, 1);
      assert.equal(requests[0].url, 'https://bridge.invalid/mcp');
      assert.equal(requests[0].authorization, 'Bearer ' + fixtureToken);
      assert.equal(requests[0].principal, 'fixture-owner');
    });
    await t.test('missing and wrong owner identities never reach the backend', async () => {
      const before = requests.length;
      for (const owner of ['', 'other-owner']) {
        const response = await call({ id: 2 }, owner);
        assert.equal(response.status, 403);
      }
      assert.equal(requests.length, before);
    });
    await t.test('redirect responses are rejected without forwarding credentials or location', async () => {
      for (const status of [301, 302, 303, 307, 308]) {
        const before = requests.length;
        const response = await call({ id: 3, redirect: status });
        assert.equal(response.status, 502);
        assert.deepEqual(await response.json(), { error: 'bridge_redirect_rejected' });
        assert.equal(response.headers.get('location'), null);
        assert.equal(requests.length, before + 1);
        assert.equal(requests.at(-1).url, 'https://bridge.invalid/mcp');
      }
    });
  } finally {
    await mf.dispose();
  }
});

