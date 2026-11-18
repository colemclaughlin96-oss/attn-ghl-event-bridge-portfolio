import { requireChatGPTUser } from './chatgpt-auth';
export const dynamic = 'force-dynamic';
export default async function Home() {
  const user = await requireChatGPTUser('/');
  return <main className="mx-auto max-w-2xl px-6 py-20 text-slate-900">
    <p className="mb-4 text-sm font-semibold uppercase tracking-widest text-slate-500">The Attention Company</p>
    <h1 className="mb-6 text-4xl font-semibold">Incoming messages</h1>
    <p className="mb-4 text-lg">This private connection carries message events to your Cloud Work chat.</p>
    <div className="my-8 rounded-xl border border-amber-200 bg-amber-50 p-6">
      <p className="font-semibold">Setup requires verification</p>
      <p className="mt-2">This page alone does not mean the listener is active. Check the listener status in the connected chat and complete a real message test.</p>
    </div>
    <p className="text-sm text-slate-600">Automatic replies to real conversations are disabled in this version.</p>
    <details className="mt-10 rounded-lg border p-4">
      <summary className="cursor-pointer text-sm">Connection setup identity</summary>
      <p className="mt-3 text-sm text-slate-500">Use this Site-specific identity to restrict the bridge to your account. It is not a password.</p>
      <code className="mt-2 block break-all text-sm">{user.userId}</code>
    </details>
  </main>;
}

