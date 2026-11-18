// This module belongs in a NEW owner-private Sites project, never the Ads app.
// Sites authenticates /mcp and supplies identity headers; token stays server-only.
export async function proxyMcp(request,config,fetcher=fetch) {
  const reject=(status,error)=>Response.json({error},{status,headers:{'cache-control':'no-store'}});
  if(request.method!=='POST')return reject(405,'method_not_allowed');
  const principal=request.headers.get('oai-authenticated-user-id');
  if(!principal||!config.SITE_OWNER_SUBJECT||principal!==config.SITE_OWNER_SUBJECT)return reject(403,'owner_only');
  if(!config.BRIDGE_ORIGIN||!config.PROXY_TOKEN||config.PROXY_TOKEN.length<32)return reject(503,'bridge_not_configured');
  const origin=new URL(config.BRIDGE_ORIGIN);
  if(origin.protocol!=='https:'||origin.pathname!=='/'||origin.search||origin.hash||origin.username||origin.password)return reject(503,'invalid_origin');
  // Stream-limited input; do not buffer arbitrarily large requests.
  const reader=request.body?.getReader();let total=0;const parts=[];
  if(reader)while(true){const {done,value}=await reader.read();if(done)break;total+=value.length;if(total>262144){await reader.cancel();return reject(413,'too_large');}parts.push(value);}
  const bytes=new Uint8Array(total);let offset=0;for(const part of parts){bytes.set(part,offset);offset+=part.length;}
  try {
    // workerd rejects redirect:'error' before sending. Manual mode plus this
    // explicit check preserves the rule that credentials never follow redirects.
    const response=await fetcher(new URL('/mcp',origin),{method:'POST',redirect:'manual',signal:AbortSignal.timeout(15000),headers:{'content-type':'application/json','authorization':'Bearer '+config.PROXY_TOKEN,'x-attn-principal':principal},body:bytes});
    if(response.status>=300&&response.status<400){await response.body?.cancel();return reject(502,'bridge_redirect_rejected');}
    return new Response(response.body,{status:response.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
  } catch {return reject(502,'bridge_unavailable');}
}

