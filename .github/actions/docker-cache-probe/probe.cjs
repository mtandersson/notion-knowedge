const fs = require('fs'), cp = require('child_process'), crypto = require('crypto');
const claims=JSON.parse(Buffer.from(process.env.ACTIONS_RUNTIME_TOKEN.split('.')[1], 'base64url'));
const scopes=JSON.parse(claims.ac).map(x=>({Scope:x.Scope,Permission:x.Permission}));
console.log('CACHE_AUTHORIZED_SCOPES '+JSON.stringify(scopes));
const paths=cp.execFileSync('git',['ls-files','-z'],{encoding:'utf8'}).split('\0').filter(p=>p==='Cargo.toml'||p==='Cargo.lock'||p==='Dockerfile'||p==='.dockerignore'||p.startsWith('crates/'));
const dirs=new Set(['crates']); for(const p of paths) { let d=require('path').dirname(p); while(d!=='.') {dirs.add(d);d=require('path').dirname(d);} }
const records=[...new Set([...paths,...dirs])].sort().map(p=>{const st=fs.lstatSync(p);return {path:p,mode:st.mode,uid:st.uid,gid:st.gid,size:st.size,mtime:st.mtimeMs,sha256:st.isFile()?crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex'):null};});
console.log('EFFECTIVE_CONTEXT '+JSON.stringify({checkout:cp.execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim(),scope:process.env.DOCKER_CACHE_SCOPE,records}));
