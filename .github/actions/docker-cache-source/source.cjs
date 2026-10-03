const crypto = require('node:crypto');
const fs = require('node:fs');

// Match BuildKit's GHA index key and go-actions-cache API version. These
// lookups are read-only; BuildKit still validates all input and platform keys.
const version = crypto.createHash('sha256').update('|go-actionscache-1.0').digest('hex');
function indexKey(scope, ref) {
  return `index-${scope}-1-${crypto.createHash('sha256').update(ref).digest('hex').slice(0, 8)}`;
}

async function lookup({scope, ref, allowedRefs, serviceURL, token, fetcher = fetch}) {
  const result = {scope, ref, available: false};
  if (!allowedRefs.includes(ref)) return result;
  try {
    const key = indexKey(scope, ref);
    const response = await fetcher(`${serviceURL.replace(/\/$/, '')}/twirp/github.actions.results.api.v1.CacheService/GetCacheEntryDownloadURL`, {
      method: 'POST', signal: AbortSignal.timeout(5000),
      headers: {Authorization: `Bearer ${token}`, 'Content-Type': 'application/json'},
      body: JSON.stringify({key, restore_keys: [key], version}),
    });
    if (!response.ok) return result;
    const entry = await response.json();
    if (!entry.ok || !entry.matched_key.startsWith(`${key}#`)) return result;
    const download = await fetcher(entry.signed_download_url, {signal: AbortSignal.timeout(5000)});
    if (!download.ok) return result;
    const bytes = Buffer.from(await download.arrayBuffer());
    const graph = JSON.parse(bytes.toString());
    if (!Array.isArray(graph.records) || !Array.isArray(graph.layers) || !graph.layers.length ||
        !graph.records.some(record => Array.isArray(record.layers) && record.layers.length)) return result;
    return {...result, available: true, key: entry.matched_key, bytes: bytes.length,
      sha256: crypto.createHash('sha256').update(bytes).digest('hex')};
  } catch {
    // Never log exceptions: download errors can contain signed resource URLs.
    return result;
  }
}

function choose(own, main) {
  const selected = own.available ? own : main.available ? main : null;
  return selected ? `type=gha,scope=${selected.scope}` : '';
}

async function run(env = process.env, fetcher = fetch, log = console.log) {
  let finalSource = '', builderSource = '';
  try {
    const claims = JSON.parse(Buffer.from(env.ACTIONS_RUNTIME_TOKEN.split('.')[1], 'base64url'));
    const allowedRefs = JSON.parse(claims.ac).map(scope => scope.Scope);
    const own = env.DOCKER_CACHE_SCOPE;
    const main = `nk-container-v1-${env.RUNNER_OS}-${env.RUNNER_ARCH}-main`;
    if (!own || !env.GITHUB_REF || !env.ACTIONS_RESULTS_URL) throw new Error();
    const args = {allowedRefs, serviceURL: env.ACTIONS_RESULTS_URL, token: env.ACTIONS_RUNTIME_TOKEN, fetcher};
    const [ownFinal, mainFinal, ownBuilder, mainBuilder] = await Promise.all([
      lookup({...args, scope: own, ref: env.GITHUB_REF}),
      lookup({...args, scope: main, ref: 'refs/heads/main'}),
      lookup({...args, scope: `${own}-builder`, ref: env.GITHUB_REF}),
      lookup({...args, scope: `${main}-builder`, ref: 'refs/heads/main'}),
    ]);
    for (const result of [ownFinal, mainFinal, ownBuilder, mainBuilder]) {
      log(`DOCKER_CACHE_ROOT ${JSON.stringify(result)}`);
    }
    finalSource = choose(ownFinal, mainFinal);
    builderSource = choose(ownBuilder, mainBuilder);
  } catch {
    log('Docker cache preflight unavailable; using the local builder cache.');
  }
  fs.appendFileSync(env.GITHUB_OUTPUT,
    `final-cache-from=${finalSource}\nbuilder-cache-from=${builderSource}\n`);
}

module.exports = {indexKey, lookup, choose, run};
if (require.main === module) run();
