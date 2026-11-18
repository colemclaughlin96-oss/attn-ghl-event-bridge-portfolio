// Always-on, opt-in dispatcher using the existing durable bridge and reply ledger.
// Adapters own network I/O. No credentials or new hosting resources live here.
import {InboundRunner} from './inbound-runner.mjs';
import {SERVER_WORKER_OWNER} from './bridge.mjs';
const ID=/^[a-zA-Z0-9_-]{1,128}$/;
const reasonOf=e=>typeof e?.reason==='string'&&/^[a-z][a-z0-9_]{0,100}$/.test(e.reason)?e.reason:'worker_run_failed';

export class ServerWorker {
  constructor({bridge,adapters,concurrency=2,allowedContacts=null,pollIntervalMs=1000,runnerFactory=options=>new InboundRunner(options),limits={},now=()=>bridge.now()}){
    if(!bridge||bridge.processingMode!=='server')throw new Error('server_worker_mode_required');
    if(!Number.isSafeInteger(concurrency)||concurrency<1||concurrency>8)throw new Error('invalid_worker_concurrency');
    if(!Number.isSafeInteger(pollIntervalMs)||pollIntervalMs<100||pollIntervalMs>30000)throw new Error('invalid_worker_poll_interval');
    if(allowedContacts!==null&&(!Array.isArray(allowedContacts)||allowedContacts.length>100||allowedContacts.some(id=>typeof id!=='string'||!ID.test(id))))throw new Error('invalid_worker_allowlist');
    if(!adapters||typeof runnerFactory!=='function')throw new Error('worker_adapters_required');
    this.b=bridge;this.concurrency=concurrency;this.allowedContacts=allowedContacts===null?null:[...new Set(allowedContacts)];this.pollIntervalMs=pollIntervalMs;this.runnerFactory=runnerFactory;this.limits=limits;this.now=now;
    this.running=false;this.pumping=false;this.active=new Map();this.timer=null;this.lastError=null;
    this.adapters={...adapters,notify:async(alert,context)=>{
      const stored=this.b.persistServerAlert(SERVER_WORKER_OWNER,alert);
      // Saving an alert is useful and visible, but does not notify Cole by itself.
      if(typeof adapters.notify!=='function')return stored;
      try{
        const receipt=await adapters.notify(alert,{...context,storedAlertId:stored.alert_id});
        if(receipt?.queued!==true)this.b.recordServerAlertNotification(SERVER_WORKER_OWNER,stored.alert_id,receipt);
        return {...receipt,stored:true,alert_id:stored.alert_id};
      }catch(e){this.b.recordServerAlertNotification(SERVER_WORKER_OWNER,stored.alert_id,{success:false,reason:reasonOf(e)});throw e;}
    }};
  }
  start({poll=true}={}){
    if(this.running)return this;
    this.running=true;
    // Process the first available job immediately. The timer only recovers jobs
    // and services delayed retries; new webhook ingress calls wake directly.
    this.wake();
    if(poll){this.timer=setInterval(()=>this.wake(),this.pollIntervalMs);this.timer.unref?.();}
    return this;
  }
  wake(){
    if(!this.running||this.pumping||this.active.size>=this.concurrency)return;
    this.pumping=true;
    try{
      const jobs=this.b.reserveServerJobs({limit:this.concurrency-this.active.size,globalLimit:this.concurrency,allowedContacts:this.allowedContacts});
      for(const job of jobs)this.launch(job);
      this.lastError=null;
    }catch(e){this.lastError=reasonOf(e);this.b.audit('server_worker_dispatch_failed');}
    finally{this.pumping=false;}
  }
  launch(job){
    const task=Promise.resolve().then(async()=>{
      let result;
      try{
        const runner=this.runnerFactory({bridge:this.b,owner:SERVER_WORKER_OWNER,adapters:this.adapters,limits:this.limits,now:this.now},job);
        result=await runner.run(job.event_id,{recover:job.recover});
      }catch(e){result={category:'failed',reason:reasonOf(e)};this.lastError=result.reason;this.b.audit('server_worker_run_failed',job.event_id);}
      try{
        const finished=this.b.finishServerJob(job.event_id,job.job_token,result);
        if(['failed','needs_review'].includes(finished.state)&&result.category!=='handoff'){
          const stored=this.b.persistTechnicalServerAlert(SERVER_WORKER_OWNER,job.event_id);
          if(typeof this.adapters.notifyTechnical==='function')await this.adapters.notifyTechnical(stored.alert_id);
        }
      }
      catch(e){this.lastError=reasonOf(e);this.b.audit('server_worker_finish_failed',job.event_id);}
      return result;
    }).finally(()=>{
      this.active.delete(job.event_id);
      if(this.running)queueMicrotask(()=>this.wake());
    });
    this.active.set(job.event_id,task);
  }
  async idle(){
    while(this.active.size){await Promise.allSettled([...this.active.values()]);await Promise.resolve();}
  }
  async stop(){this.running=false;if(this.timer)clearInterval(this.timer);this.timer=null;await this.idle();}
  status(){return {running:this.running,active_jobs:this.active.size,concurrency:this.concurrency,canary_restricted:this.allowedContacts!==null,canary_contact_count:this.allowedContacts?.length??null,last_error:this.lastError,...this.b.serverWorkerStatus(SERVER_WORKER_OWNER)};}
}

