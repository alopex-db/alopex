import test from 'node:test';
import assert from 'node:assert/strict';
import {verify} from './verify-four-case-timing.mjs';
import {createHash} from 'node:crypto';
import {mkdtempSync,writeFileSync,unlinkSync,rmdirSync} from 'node:fs';
import {join} from 'node:path'; import {tmpdir} from 'node:os'; import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
function fixture(){
 const timings=[],paired={};
 for(const c of ['aggregate','ordered','select-all','insert-rollback']){
  for(let t=0;t<3;t++)for(let s=0;s<21;s++)
   ((t+s)%2===0?['off','on']:['on','off']).forEach((mode,k)=>timings.push({schema:'four_case_timing_v1',case:c,trial:t,sample:s,mode,pair_ordinal:2*s+k,elapsed_seconds:mode==='off'?1:1.01}));
  paired[c]={delta_ms:[10,10,10],trial_U_ms:[10,10,10],D_ms:10,MAD_ms:0,U_ms:10,bound_ms:50,small_overhead:true};
 }
 return {schema:'alopex.sql-four-case-export/v4',status:'prepared',matrix_status:'passed',performance_qualification:false,measurement:{schema:'alopex.sql-four-case-measurement/v2',units:{elapsed_seconds:'seconds'},timings,paired}};
}
test('known 10 ms delta and 50 ms bound',()=>verify(fixture()));
test('missing sample',()=>{const x=fixture();x.measurement.timings.pop();assert.throws(()=>verify(x),/coverage/)});
test('order corruption',()=>{const x=fixture();x.measurement.timings[0].pair_ordinal=1;assert.throws(()=>verify(x),/identity/)});
test('unexpected diagnostic field',()=>{const x=fixture();x.measurement.timings[0].internal='fixture';assert.throws(()=>verify(x),/identity/)});
test('altered decision',()=>{const x=fixture();x.measurement.paired.aggregate.small_overhead=false;assert.throws(()=>verify(x),/mismatch/)});
test('failed matrix',()=>{const x=fixture();x.matrix_status='failed';assert.throws(()=>verify(x),/completed/)});
test('CLI checks bytes',()=>{const dir=mkdtempSync(join(tmpdir(),'alopex-verify-'));const file=join(dir,'public.json');try{
 const body=JSON.stringify(fixture());writeFileSync(file,body);
 const cli=fileURLToPath(new URL('./verify-four-case-timing.mjs',import.meta.url));
 const sha=createHash('sha256').update(body).digest('hex');
 assert.equal(spawnSync(process.execPath,[cli,file,sha]).status,0);
 assert.equal(spawnSync(process.execPath,[cli,file,'0'.repeat(64)]).status,1);
}finally{unlinkSync(file);rmdirSync(dir)}});
