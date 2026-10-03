const fs = require('fs'), cp = require('child_process'), crypto = require('crypto');
const claims=JSON.parse(Buffer.from(process.env.ACTIONS_RUNTIME_TOKEN.split('.')[1], 'base64url'));
const scopes=JSON.parse(claims.ac).map(x=>({Scope:x.Scope,Permission:x.Permission}));
console.log('CACHE_AUTHORIZED_SCOPES '+JSON.stringify(scopes));
const paths=cp.execFileSync('git',['ls-files','-z'],{encoding:'utf8'}).split('\0').filter(p=>p==='Cargo.toml'||p==='Cargo.lock'||p==='Dockerfile'||p==='.dockerignore'||p.startsWith('crates/'));
const dirs=new Set(['crates']); for(const p of paths) { let d=require('path').dirname(p); while(d!=='.') {dirs.add(d);d=require('path').dirname(d);} }
const records=[...new Set([...paths,...dirs])].sort().map(p=>{const st=fs.lstatSync(p);return {path:p,mode:st.mode,uid:st.uid,gid:st.gid,size:st.size,mtime:st.mtimeMs,sha256:st.isFile()?crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'):null};});
console.log('EFFECTIVE_CONTEXT '+JSON.stringify({checkout:cp.execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),scope:process.env.DOCKER_CACHE_SCOPE,records}));

(async()=>{
for(const cacheScope of [process.env.DOCKER_CACHE_SCOPE, "nk-container-v1-Linux-X64-main"]){
for(const {Scope} of scopes){
 const key='index-'+cacheScope+'-1-'+crypto.createHash('sha256').update(Scope).digest('hex').slice(0,8);
 const version=crypto.createHash('sha256').update('|go-actionscache-1.0').digest('hex');
 const r=await fetch(process.env.ACTIONS_RESULTS_URL.replace(/\/$/,'')+'/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL',{method:'POST',headers:{Authorization:'Bearer '+process.env.ACTIONS_RUNTIME_TOKEN,'Content-Type':'application/json'},body:JSON.stringify({key,restore_keys:[key],version})});
 if(!r.ok) throw new Error('Cache lookup status '+r.status);
 const data=await r.json();
 const result={cacheScope,scope:Scope,key,ok:data.ok,matched_key:data.matched_key};
 if(data.ok){const response=await fetch(data.signed_download_url);if(!response.ok)throw new Error('Manifest download status '+response.status);const bytes=Buffer.from(await response.arrayBuffer());result.bytes=bytes.length;result.sha256=crypto.createHash('sha256').update(bytes).digest('hex');result.manifest=JSON.parse(bytes.toString());}
 console.log('CACHE_INDEX_LOOKUP '+JSON.stringify(result));
}
}
})().catch(error=>{console.error(error.message);process.exitCode=1;});
