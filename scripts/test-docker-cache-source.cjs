const assert = require('node:assert/strict');
const test = require('node:test');
const {indexKey, lookup, choose} = require('../.github/actions/docker-cache-source/source.cjs');

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
