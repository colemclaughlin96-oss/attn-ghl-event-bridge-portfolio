import crypto from 'node:crypto';
import dns from 'node:dns/promises';
import https from 'node:https';
import net from 'node:net';

export const hash = value => crypto.createHash('sha256').update(value).digest('hex');
export const equal = (a, b) => {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && crypto.timingSafeEqual(x, y);
};
export function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical(value[k])).join(',') + '}';
  return JSON.stringify(value);
}
export function signingKey(secret) {
  if (typeof secret !== 'string' || !/^whsec_[A-Za-z0-9+/]+={0,2}$/.test(secret)) throw new Error('invalid_signing_secret');
  const value = secret.slice(6), bytes = Buffer.from(value, 'base64');
  if (bytes.length < 24 || bytes.length > 64 || bytes.toString('base64').replace(/=+$/, '') !== value.replace(/=+$/, '')) throw new Error('invalid_signing_secret');
  return bytes;
}
export function signedHeaders(id, body, subscription, now = Date.now()) {
  const timestamp = String(Math.floor(now / 1000));
  const keys = [subscription.secret];
  if (subscription.oldSecret && subscription.rotateUntil > now) keys.push(subscription.oldSecret);
  return {
    'content-type': 'application/json',
    'webhook-id': id,
    'webhook-timestamp': timestamp,
    'webhook-signature': keys.map(s => 'v1,' + crypto.createHmac('sha256', signingKey(s)).update(`${id}.${timestamp}.${body}`).digest('base64')).join(' '),
    'x-mcp-subscription-id': subscription.id,
  };
}
export class Vault {
  constructor(key) {
    this.key = Buffer.from(key, 'base64');
    if (this.key.length !== 32) throw new Error('VAULT_KEY must decode to 32 bytes');
  }
  seal(value, context) {
    const iv = crypto.randomBytes(12), cipher = crypto.createCipheriv('aes-256-gcm', this.key, iv);
    cipher.setAAD(Buffer.from(context));
    const encrypted = Buffer.concat([cipher.update(JSON.stringify(value), 'utf8'), cipher.final()]);
    return Buffer.concat([iv, cipher.getAuthTag(), encrypted]).toString('base64');
  }
  open(value, context) {
    const data = Buffer.from(value, 'base64'), decipher = crypto.createDecipheriv('aes-256-gcm', this.key, data.subarray(0,12));
    decipher.setAAD(Buffer.from(context));
    decipher.setAuthTag(data.subarray(12,28));
    return JSON.parse(Buffer.concat([decipher.update(data.subarray(28)), decipher.final()]).toString('utf8'));
  }
}

// Conservative IPv4-only transport. IPv6-only callbacks fail closed.
export function publicV4(address) {
  if (net.isIP(address) !== 4) return false;
  const [a,b,c] = address.split('.').map(Number);
  return !(a === 0 || a === 10 || a === 127 || a >= 224 ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && (b === 168 || b === 0 || (b === 88 && c === 99))) ||
    (a === 198 && (b === 18 || b === 19 || (b === 51 && c === 100))) ||
    (a === 203 && b === 0 && c === 113));
}
export function callbackURL(value) {
  if (typeof value !== 'string' || value.length > 2048) throw new Error('invalid_callback');
  const u = new URL(value);
  if (u.protocol !== 'https:' || u.username || u.password || u.hash || (u.port && u.port !== '443') ||
      net.isIP(u.hostname) || u.hostname.includes(':') || u.hostname.endsWith('.') ||
      !u.hostname.includes('.') || /\.(localhost|local|internal|test|invalid)$/.test(u.hostname)) throw new Error('invalid_callback');
  return u;
}
export async function resolveCallback(value, resolver = dns.resolve4) {
  const u = callbackURL(value);
  const addresses = await resolver(u.hostname);
  if (!addresses.length || addresses.some(a => !publicV4(a))) throw new Error('unsafe_callback_address');
  return {url: u, address: addresses[0]};
}
export function makeSecurePost({resolver = dns.resolve4, request = https.request} = {}) {
  return async (value, body, headers) => {
    // Resolve anew on EVERY verification and delivery, and pin this connection.
    const {url, address} = await resolveCallback(value, resolver);
    if (Buffer.byteLength(body) > 262144) throw new Error('payload_too_large');
    return new Promise((resolve, reject) => {
      const req = request({
        protocol: 'https:', hostname: url.hostname, port: 443,
        servername: url.hostname, path: url.pathname + url.search,
        method: 'POST', agent: false, family: 4, rejectUnauthorized: true,
        lookup: (_host, options, cb) => {
          if (typeof options === 'function') cb = options;
          if (options?.all) cb(null, [{address, family:4}]); else cb(null, address, 4);
        },
        headers: {...headers, 'content-length': Buffer.byteLength(body)},
      }, res => {
        // Node https never follows redirects; all 3xx are returned as failures.
        let size = 0; const chunks = [];
        res.on('data', chunk => {size += chunk.length; if(size > 8192) req.destroy(new Error('response_too_large')); else chunks.push(chunk);});
        res.on('end', () => resolve({status: res.statusCode, body: Buffer.concat(chunks).toString('utf8')}));
        res.on('error', () => reject(new Error('transport_error')));
      });
      const timer = setTimeout(() => req.destroy(new Error('timeout')), 10000);
      req.on('close', () => clearTimeout(timer));
      req.on('error', err => reject(new Error(err.message === 'timeout' ? 'timeout' : 'transport_error')));
      req.end(body);
    });
  };
}
export const securePost = makeSecurePost();

// Official HighLevel Ed25519 verification key, checked 2026-10-20.
export const GHL_PUBLIC_KEY = '-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAi2HR1srL4o18O8BRa7gVJY7G7bupbN3H9AwJrHCDiOg=\n-----END PUBLIC KEY-----';
export function verifyGhl(raw, signature, key = GHL_PUBLIC_KEY) {
  try {
    if (typeof signature !== 'string' || !/^[A-Za-z0-9+/]+={0,2}$/.test(signature)) return false;
    const bytes = Buffer.from(signature, 'base64');
    return bytes.length === 64 && crypto.verify(null, raw, key, bytes);
  } catch { return false; }
}


