import {readFileSync,statSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
const cases=['aggregate','ordered','select-all','insert-rollback'];
const fields='case,elapsed_seconds,mode,pair_ordinal,sample,schema,trial';
const require=(ok,why)=>{if(!ok)throw Error(why)};
const med=x=>[...x].sort((a,b)=>a-b)[(x.length-1)/2];
const mad=(x,m)=>med(x.map(v=>Math.abs(v-m)));
const eq=(a,b)=>Number.isFinite(a)&&Number.isFinite(b)&&Math.abs(a-b)<=16*Number.EPSILON*Math.max(1,Math.abs(a),Math.abs(b));
const keys=(x)=>x&&typeof x==='object'&&!Array.isArray(x)?Object.keys(x).sort().join(','):'';
export function verify(data){
 require(data?.schema==='alopex.sql-four-case-export/v4'&&data.status==='prepared'&&data.matrix_status==='passed'&&data.performance_qualification===false,'not a completed measurement');
 const m=data.measurement;
 require(m?.schema==='alopex.sql-four-case-measurement/v2'&&m.units?.elapsed_seconds==='seconds','schema or unit');
 require(Array.isArray(m.timings)&&m.timings.length===504,'sample coverage');
 require(keys(m.paired)===[...cases].sort().join(','),'summary cases');
 let n=0;
 for(const c of cases){
  const d=[],u=[],off=[];
  for(let t=0;t<3;t++){
   const dif=[],offSamples=[];
   for(let s=0;s<21;s++){
    const order=(t+s)%2===0?['off','on']:['on','off'];
    const pair={};
    for(let k=0;k<2;k++){
     const r=m.timings[n++];
     require(keys(r)===fields&&r.schema==='four_case_timing_v1'&&r.case===c&&r.trial===t&&r.sample===s&&r.mode===order[k]&&r.pair_ordinal===2*s+k&&Number.isFinite(r.elapsed_seconds)&&r.elapsed_seconds>=0,'sample identity or value');
     pair[r.mode]=r.elapsed_seconds;
    }
    offSamples.push(pair.off); dif.push((pair.on-pair.off)*1000);
   }
   require(dif.every(Number.isFinite),'arithmetic');
   const x=med(dif); d.push(x);u.push(x+2*1.4826*mad(dif,x)/Math.sqrt(21));off.push(med(offSamples)*1000);
  }
  const D=med(d),MAD=mad(d,D),U=Math.max(D+2*1.4826*MAD,...u),bound=.05*med(off);
  const expected={delta_ms:d,trial_U_ms:u,D_ms:D,MAD_ms:MAD,U_ms:U,bound_ms:bound,small_overhead:Math.max(0,U)<=bound};
  const actual=m.paired[c];
  require(keys(actual)===keys(expected),'summary fields');
  for(const [k,v] of Object.entries(expected)){
   const a=actual[k];
   require(Array.isArray(v)?Array.isArray(a)&&a.length===3&&v.every((x,i)=>eq(x,a[i])):typeof v==='boolean'?a===v:eq(a,v),'summary mismatch '+c+' '+k);
  }
 }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 try{
  const [,,p,sha]=process.argv;
  require(process.argv.length===4&&/^[a-f0-9]{64}$/.test(sha??''),'usage: node verify-four-case-timing.mjs public.json EXPECTED_SHA256');
  const n=statSync(p).size; require(n>0&&n<=3000000,'file size');
  const b=readFileSync(p); require(b.length===n&&createHash('sha256').update(b).digest('hex')===sha,'file digest mismatch');
  verify(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(b)));
  console.log('Verified 504 public timing samples and four paired-overhead summaries. Not a benchmark rerun or release qualification.');
 }catch(e){console.error(e.message);process.exitCode=1}
}
