import { env } from 'cloudflare:workers';
import { proxyMcp } from '../../proxy.mjs';
export const dynamic = 'force-dynamic';
export async function POST(request: Request) {
  return proxyMcp(request, env);
}

