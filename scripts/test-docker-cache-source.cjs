const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {indexKey, lookup, choose, run} = require('../.github/actions/docker-cache-source/source.cjs');

const scope = 'nk-container-v1-Linux-X64-187';
const ref = 'refs/pull/187/merge';
const args = {scope, ref, allowedRefs: [ref, 'refs/heads/main'],
  serviceURL: 'https://cache.invalid/', token: 'test-only-token'};
const graph = Buffer.from(JSON.stringify({layers: [{blob: 'sha256:fixture'}],
  records: [{layers: [{layer: 0}]}]}));
function responder(first, second = {ok: true, arrayBuffer: async () => graph}) {
  let calls = 0;
  return async (_url, options) => {
    if (++calls === 1) {
      assert.equal(JSON.parse(options.body).key, indexKey(scope, ref));
      return first;
    }
    assert.equal(options.headers, undefined, 'Runtime bearer must not follow the signed URL');
    return second;
  };
}
function entry(key = `${indexKey(scope, ref)}#1`) {
  return {ok: true, json: async () => ({ok: true, matched_key: key,
    signed_download_url: 'https://cache.invalid/download'})};
}

test('Own merge-ref index resolves before selecting exactly one logical source', async () => {
  assert.equal(indexKey(scope, ref), 'index-nk-container-v1-Linux-X64-187-1-94398b49');
  const own = await lookup({...args, fetcher: responder(entry())});
  assert.equal(own.available, true);
  assert.equal(own.bytes, graph.length);
  assert.equal(choose(own, {available: true, scope: 'trusted-main'}), `type=gha,scope=${scope}`);
  assert.equal(JSON.stringify(own).includes('signed_download_url'), false);
  assert.equal(JSON.stringify(own).includes(args.token), false);
});

test('Absent own cache preserves trusted-main warming; absent both uses local cache', () => {
  assert.equal(choose({available: false}, {available: true, scope: 'trusted-main'}), 'type=gha,scope=trusted-main');
  assert.equal(choose({available: false}, {available: false}), '');
});

test('Unauthorized refs and wrong matched ref are not used', async () => {
  const denied = await lookup({...args, allowedRefs: [], fetcher: () => assert.fail('Unauthorized ref queried')});
  assert.equal(denied.available, false);
  const wrong = await lookup({...args, fetcher: responder(entry(indexKey(scope, 'refs/heads/main') + '#1'))});
  assert.equal(wrong.available, false);
});

test('Lookup absence, service failures, unavailable blobs and malformed graphs fail safe', async () => {
  for (const fetcher of [
    responder({ok: true, json: async () => ({ok: false})}),
    responder({ok: false}),
    responder(entry(), {ok: false}),
    responder(entry(), {ok: true, arrayBuffer: async () => Buffer.from('not JSON')}),
    responder(entry(), {ok: true, arrayBuffer: async () => Buffer.from('{"records":[],"layers":[]}')}),
    async () => { throw new Error('Download error must remain private'); },
  ]) {
    assert.equal((await lookup({...args, fetcher})).available, false);
  }
});

test('Read-only fork authorization uses main without cache writes or credential logging', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'nk-cache-source-'));
  const output = path.join(directory, 'output');
  const main = 'nk-container-v1-Linux-X64-main';
  const claims = {ac: JSON.stringify([{Scope: 'refs/heads/main', Permission: 1}])};
  const token = `test-only.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.test-only`;
  const logs = [], requests = [];
  try {
    await run({DOCKER_CACHE_SCOPE: scope, GITHUB_REF: ref, RUNNER_OS: 'Linux', RUNNER_ARCH: 'X64',
      ACTIONS_RESULTS_URL: args.serviceURL, ACTIONS_RUNTIME_TOKEN: token, GITHUB_OUTPUT: output},
    async (url, options) => {
      requests.push(url);
      if (url === 'https://cache.invalid/download') {
        assert.equal(options.headers, undefined);
        return {ok: true, arrayBuffer: async () => graph};
      }
      assert.ok(url.endsWith('/GetCacheEntryDownloadURL'), 'Only read-only cache requests permitted');
      assert.equal(options.method, 'POST');
      const key = JSON.parse(options.body).key;
      if (key === indexKey(main, 'refs/heads/main')) return entry(key + '#1');
      assert.equal(key, indexKey(main + '-builder', 'refs/heads/main'));
      return {ok: true, json: async () => ({ok: false})};
    }, line => logs.push(line));
    assert.equal(fs.readFileSync(output, 'utf8'),
      `final-cache-from=type=gha,scope=${main}\nbuilder-cache-from=\n`);
    assert.equal(requests.length, 3, 'Unauthorized own refs must not be queried');
    assert.equal(logs.join('\n').includes(token), false);
    assert.equal(logs.join('\n').includes('https://cache.invalid/download'), false);
  } finally {
    fs.rmSync(directory, {recursive: true});
  }
});

test('A cache outage leaves both imports empty and keeps private errors out of logs', async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'nk-cache-source-'));
  const output = path.join(directory, 'output');
  const claims = {ac: JSON.stringify([{Scope: ref}, {Scope: 'refs/heads/main'}])};
  const token = `test-only.${Buffer.from(JSON.stringify(claims)).toString('base64url')}.test-only`;
  const privateURL = new URL('https://cache.invalid/download');
  privateURL.searchParams.set('sig', 'test-only-value');
  const logs = [];
  try {
    await run({DOCKER_CACHE_SCOPE: scope, GITHUB_REF: ref, RUNNER_OS: 'Linux', RUNNER_ARCH: 'X64',
      ACTIONS_RESULTS_URL: args.serviceURL, ACTIONS_RUNTIME_TOKEN: token, GITHUB_OUTPUT: output},
    async () => { throw new Error(privateURL.href + ' ' + token); }, line => logs.push(line));
    assert.equal(fs.readFileSync(output, 'utf8'), 'final-cache-from=\nbuilder-cache-from=\n');
    assert.equal(logs.join('\n').includes(privateURL.href), false);
    assert.equal(logs.join('\n').includes(token), false);
  } finally {
    fs.rmSync(directory, {recursive: true});
  }
});
