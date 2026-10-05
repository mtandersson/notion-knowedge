# ChatGPT connection experiment (#216)

The local read/index/search/refresh chain has been demonstrated in Steps 1–4.
**A real user-assisted ChatGPT conversation invoked this server, reflected the
updated source and declined an unsupported answer.** Local HTTP,
MCP Inspector, Codex tools, and Responses API transcripts cannot satisfy that
requirement. This document records the completed bounded demonstration and its reproduction
runbook.

## Connection gate

Official guidance checked on 2026-10-05:
[connect and test](https://developers.openai.com/plugins/deploy/connect-chatgpt),
[Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels),
and [authentication](https://developers.openai.com/plugins/build/auth).
Developer-mode availability depends on the account and workspace policy.
Check Settings → Security and login → Developer mode, then Plugins → plus →
Connection. Prefer **Tunnel** and an available tunnel associated with this
ChatGPT workspace. The user confirmed Developer mode and the Tunnel connection option. The supplied
private tunnel authenticated successfully; ChatGPT discovered and invoked
`knowledge_search` through that connection.
Existing unrelated tunnel-backed plugins do not prove access to a new tunnel.

A private tunnel requires its own tunnel ID and runtime control-plane credential,
plus Tunnels Read + Use; creating/configuring one requires Read + Manage.
Keep those credentials local. The upstream Notion integration token is unrelated
and must never be supplied to ChatGPT or the tunnel. Persisted-index search
requires no Notion requests or credentials. Do not assume custom static headers
are supported. Development anonymous access is limited to this explicitly
non-sensitive, read-only biography corpus; production OAuth remains #117/#109.
No public endpoint may be started before account capability is established.

## Local server and private tunnel

Start from the recorded code revision with the matched Nix compiler:

```sh
nix develop .#spike
cargo build --locked --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -j 2
exe=crates/retrieval/spikes/qwen-lance/target/debug/qwen-lance-spike
env -u NOTION_TOKEN -u OPENAI_API_KEY NK_HTTP_HOST=127.0.0.1 NK_HTTP_PORT=3216 \
  "$exe" serve-http /absolute/model-assets /absolute/verified-index
```

Provision the tunnel through Platform tunnel settings and associate it with the
intended ChatGPT workspace. Download the official `tunnel-client` from the link
in those settings or its latest official release; validate its local help first.
Supply `CONTROL_PLANE_API_KEY` through local secret configuration, then in a
second terminal:

```sh
tunnel-client init --sample sample_mcp_remote_no_auth --profile nk216 \
  --tunnel-id YOUR_TUNNEL_ID --mcp-server-url http://127.0.0.1:3216/mcp
tunnel-client doctor --profile nk216 --explain
tunnel-client run --profile nk216
```

The private HTTP/no-auth sample was executed with official tunnel-client v0.0.15.
The Linux amd64 archive was checked against the release SHA256 manifest. The
client reports build `0.0.15+a390c168ff1b2d14e73a95991c186c6aba3ff5a0`.
Local `doctor` completed with exit 0 and `/healthz` and `/readyz` returned 200.
The selected private tunnel authenticated and fetched its metadata. Only the
control-plane runtime credential was passed; the Notion token was excluded.
The supplied runtime key and tunnel ID reside in ignored local `.env`; Notion
credentials remain separate in `.env.local`. No secret value is documented.
The temporary profile is `/tmp/nk216-tunnel-profile/nk216.yaml`, containing an
environment credential reference, not the runtime key. An initial startup before
model readiness failed MCP initialization; restarting only the owned tunnel
client after the listener was ready resolved it. This is transport readiness,
not ChatGPT conversation evidence.
Confirm the installed client's HTTP configuration and doctor output. Keep the
server and tunnel client live while creating the connection. Review discovered
`knowledge_search`, its schema, and read-only annotations. Add the connection
from the tools menu of a new ChatGPT conversation. No custom UI is required.
For an HTTPS fallback, a trusted proxy must preserve MCP sessions/SSE and rewrite
Host to the loopback authority accepted by the existing listener. Do not weaken
Host/Origin validation globally. No fallback endpoint has been published.

## Baseline and refreshed generation in the same experiment

For the first actual ChatGPT call, start the owned server with the retained old
`/tmp/nk213-index`, using the same assets, port and executable. Its introduction
has the old role wording and lacks `NK215-B`, even though Notion has already been
edited. Do not restore or modify Notion to manufacture this baseline.

```sh
env -u NOTION_TOKEN -u OPENAI_API_KEY NK_HTTP_HOST=127.0.0.1 NK_HTTP_PORT=3216 \
  "$exe" serve-http /tmp/nk212-assets /tmp/nk213-index
# After the baseline conversation evidence, Ctrl-C this owned server, then:
env -u NOTION_TOKEN -u OPENAI_API_KEY NK_HTTP_HOST=127.0.0.1 NK_HTTP_PORT=3216 \
  "$exe" serve-http /tmp/nk212-assets /tmp/nk215-rebuild-index
```

Keep the private tunnel forwarding to that loopback port. Restarting the server
ends old MCP sessions; reconnect the ChatGPT connection before repeating the
same question. A new chat can avoid relying on a cached previous tool answer.
Require a fresh `knowledge_search` invocation, identical query string, and
returned passage inspection. Tell ChatGPT to report the role wording and whether
the returned introductory passage contains the refresh marker, and cite the
page. These instructions do not introduce a new retrieval question. The old
response must lack the marker; the new response must contain it. This compares
derived generations without new source writes or model runs.

## Required real conversation evidence

Reuse existing questions; the epic's five distinct questions are already spent.
Ask ChatGPT to use this connection and answer only from returned source evidence:

- `Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?`
- Repeat the same question against the refreshed generation and confirm the
  returned introduction contains `NK215-B` and the updated role wording.
- `How long should sourdough bread bake in a home oven?`

Record actual ChatGPT tool name, arguments, returned citations and final answer,
conversation date/model/workspace route, and the visible selected-page link.
The correct organization is U.S. Central Command, 1988–1991. The source URL must
identify page `3ef26b4f59908161b656e6a30784d8ca`. The bread question has no answer
in this biography: the current adapter returns irrelevant nearest neighbors,
so a valid outcome must explicitly report missing source support, without
inventing a source-backed baking answer. This is not a relevance-threshold pass.
Retain raw conversation/source evidence locally; commit only sanitized findings.

## Revisions and observed bounds

Preparation starts at main `57a3c24d3eb03a729a8a252f46e950f7c1734605` on host
`north`. Model: Qwen3-Embedding-0.6B revision
`97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3`, Float32/1024, pinned local assets.
LanceDB 0.39 uses the matched Rust/LLVM spike toolchain. The approved rebuilt
index has 11 unique canonical chunks, corpus SHA256
`bcc95d9a0ba00eb7c1a5bdb89f2aa0b68d24fa5043a9b1741934ab9af97e5f71`.
One real page plus the original synthetic fixture remain inside the three-page
bound. No new distinct question, broad crawl, write, upload, or secret transfer
is authorized by this connection experiment.

Existing debug-profile observations: roughly 35 seconds model load, 8–13 seconds
query handling, 11 minutes full 11-chunk embedding. Sampled memory around
2.5–2.8 GiB is not a peak-memory claim. These single-host experiments establish
neither production latency nor cold OS-cache behavior. The promotion-year query
failed relevance and the absent-answer query returns unrelated neighbors.

## Actual local preparation evidence (2026-10-05)

An explicit matched-toolchain build from the above main revision completed in
8.82 seconds, naming this worktree's dependency crates. The exact built executable
started privately at `http://127.0.0.1:3216/mcp`, with Notion and OpenAI API
credentials removed from its process environment. Actual initialization and tool
discovery returned HTTP 200, created an MCP session, and advertised
`knowledge_search` with a read-only annotation. The reused Swedish query returned
a successful tool result containing the edited marker, correct page ID and
correct Notion URL. Raw responses remain local. An initial local verification
parser rejected the SSE stream's empty keepalive `data:` frame; inspecting the
actual nonempty JSON result resolved this harness issue without changing the
server or rerunning the question. This is HTTP preflight, not ChatGPT evidence.
The owned preparation server stopped cleanly (exit 0); its loopback port was
confirmed closed. No public endpoint was created during preparation. The later authorized private
tunnel served both generations for the conversation test and was stopped after
the completed proof.

An independent read-only preparation review approved the runbook after the
old/new generation transition was made explicit. The final independent review examined the subsequent real conversation trace
and supplied answers; preparation approval alone did not permit issue closure.

## Actual isolated ChatGPT baseline (2026-10-05)

An earlier user-reported answer was ambiguous because the regular Notion
connection was also available; it does not count as attribution to this server.
The user then selected only this experiment's connection and repeated the exact
Swedish question in ChatGPT. A temporary loopback verification proxy between
the private tunnel client and server recorded `tools/call` to `knowledge_search`
at `2026-10-05T18:20:48.534921Z`, with the query SHA256
`1dc1b97d14a42323cf97bec10be957ebfe490a00ef1e73818227278209e549ab`
and exact expected-query match true. No local semantic calls were made through
that proxy. The narrow trace captures the exact query hash and tool name, not a
complete `mode`/`limit` argument transcript. Its response completed at `18:21:00.067863Z`, HTTP 200, with the
selected Notion URL present and `NK215-B` absent, consistent with the retained
old generation. The user reported receiving the isolated answer; the isolated run returned an answer. The earlier supplied answer said U.S. Central
Command and marker absent, but its mixed-connector attribution remains excluded.
The subsequent isolated old-response trace proves the source marker absent; the
fully captured updated final answer below proves the supported organization.

An earlier call at `18:20:44.932666Z` had HTTP 200 without the selected URL or
marker; the narrow trace did not record its result/error class. Do not infer a
specific failure cause or count that attempt as successful retrieval. The later
call supplies the actual baseline response evidence.

The proxy logs only UTC timestamp, tool name, query hash/exact-match boolean,
HTTP status and citation/marker booleans. It forwards sessions/SSE, rewrites Host
to the loopback authority, and excludes Authorization/Cookie headers. It is
local verification instrumentation, not a production proxy or authentication
implementation. Raw source content and credentials are not logged. The same isolated connection then called `knowledge_search` at
`2026-10-05T18:23:29.350054Z` with the identical Swedish query hash and exact-match
boolean. Its response completed at `18:23:40.929832Z`, HTTP 200, with both the
selected Notion URL and `NK215-B` present. The owned server had been stopped and
restarted against `/tmp/nk215-rebuild-index`; the private tunnel client was
reinitialized to avoid reusing a stopped server session. No Notion edit or
embedding run was needed for this switch. The user supplied the updated final answer: it states that Schwarzkopf commanded
U.S. Central Command (CENTCOM) during 1988–1991, reports the introduction contains
`NK215-B`, quotes the short marker phrase, and labels the Notion source
“H. Norman Schwarzkopf Jr.”. The user separately supplied the exact selected-page
URL. Final answer text is user-reported ChatGPT evidence, corroborated by actual
server receipt/result tracing; it was not generated by a local MCP client. An earlier
updated attempt at `18:23:24.764132Z` lacked both marker and citation in its
response; as with the initial baseline retry, its error class was not captured.

For the final absent-answer question, the expected query hash is
`0172fa1330b2d52b29d15aa5e1f000fe33173820c019d9845ed87707cf1f2ebe`.
The generic query hash trace can verify that exact existing bread question
without additional instrumentation or a sixth distinct question. The model's
actual final answer must be inspected for unsupported source claims; transport
success and a biography citation alone cannot prove correct abstention.
The actual bread-question call at `2026-10-05T18:26:10.973Z` used
`knowledge_search` and that exact hash. Its response completed at `18:26:18.906Z`,
HTTP 200, with the biography URL and marker present. These were irrelevant
nearest neighbors, not evidence about baking. The user supplied ChatGPT's final
answer: the tool sources lack the answer, only irrelevant Schwarzkopf results
were found, and no information supports a home-oven sourdough baking duration.
It gives no invented source-backed duration. An earlier citation-free HTTP 200
attempt remains unclassified. This demonstrates correct model abstention for
this one prompt, not adapter relevance filtering or general hallucination safety.

The user confirmed Developer mode/Tunnel availability and executed the actual
ChatGPT UI flow. Plan, model selector and workspace identifier were not captured;
no claim about those account attributes or broader plan availability is made.
The server logs and returned source booleans establish the experiment's actual
request path, while the user's supplied final answers establish model behavior.

## Preliminary decision and production reconciliation

**Proceed with hardening** based on the completed bounded ChatGPT proof.
Rust, local Qwen and embedded LanceDB have crossed the real source, persistence,
MCP and refresh boundaries. Change deployment/performance expectations: the
unoptimized prototype is expensive, exact scan is tiny-corpus only, and ranking
quality is not production-ready. The tiny read-only end-to-end experiment succeeded; production readiness is
unproven.

| Ticket | Reuse | Harden or replace before production |
| --- | --- | --- |
| #37 | Canonical Arrow/Lance columns and dimension checks | Queryable filter schema, stable-ID upsert and create/open specifications |
| #39 | Verified local model identity, pooling and normalization | Configurable batch/device, resource lifecycle and bilingual smoke |
| #40 | Persisted vectors and exact cosine reference | Configurable ANN construction, maintenance and safe rebuild |
| #42 | Separate disposable generations and corpus identity | Incremental skip/upsert/delete, stable reconciliation and metrics |
| #43 | Strict identity incompatibility checks | Operator fail/rebuild policy, versioning and migration runbook |
| #45 | Core search port and shared MCP composition | Production provider integration, bounded concurrency and fixed-corpus tests |
| #49 | Canonical citation mapping | Query-aware configurable snippets, timestamps/rank evidence and signed-URL checks |
| #109 | Supported connection runbook and actual real tool evidence | Interactive OAuth, workspace/user restriction, revocation and metadata refresh |
| #110 | Real selected-page read/index/search/manual refresh | Isolated safe writes/uploads and explicit cleanup; require separate authorization |
| #145 | Matched native toolchain and external asset/index layout | Real production indexing lifecycle, non-root image, mounts and container recovery smoke |

The smallest production foundations are #37 and #39, followed by their native
dependencies; #45 can reuse composition after production vector/provider support.
Retain the isolated spike as an explicit experiment until those implementations
replace it. Remove its fixed 32-row cap, hardcoded settings and development
launch path only when production equivalents have meaningful verification.
No production ticket closes from these development-only results.

## Teardown

Stop the tunnel client with Ctrl-C, then stop the owned local server with Ctrl-C.
If using the optional tracing proxy described below, stop it with Ctrl-C too.
Disconnect/remove the temporary ChatGPT connection and disable/delete only the
experiment's tunnel when it is disposable and not shared. Verify owned processes
have exited and the loopback port no longer accepts connections. Preserve model
assets, authoritative page and retained evidence/index generations. Do not
remove or rotate an unrelated Home Assistant tunnel. Temporary marker removal
is a user source edit and is not performed automatically.

Actual teardown: the owned server and tunnel client exited 0; the Python tracing
proxy exited 130 after SIGINT. Ports 3216, 3217 and 3218 were confirmed closed
after process termination. The existing Platform tunnel and ChatGPT connection
configuration were preserved; neither can forward to this stopped local client.
No public endpoint was created. Assets, indexes, ignored credentials and local
evidence are retained. No Notion content was changed by this experiment.

## Optional local tracing used for the captured evidence

The ordinary reproduction commands connect directly to port 3216 and can be
verified through ChatGPT's expanded tool activity. The recorded proof additionally
used a temporary loopback proxy on 3218 to avoid relying solely on user-reported
tool attribution. To reproduce that auxiliary tracing, save the following as a
local ignored `/tmp/nk216-trace-proxy.py`, run it with Python 3, and change the
tunnel profile's MCP URL to `http://127.0.0.1:3218/mcp`. Keep the actual server on
3216. The proxy is development verification only, with no production auth or
general request-limiting claim. Its log contains no raw query/source payload.
Stop all three owned processes after the test.

```python
import http.server,http.client,json,hashlib,datetime
EXPECTED='Vilken organisation var H. Norman Schwarzkopf Jr. chef för mellan 1988 och 1991?'
class Handler(http.server.BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def log_message(self,*args):pass
 def relay(self):
  body=self.rfile.read(int(self.headers.get('Content-Length','0')))
  event=None
  try:
   req=json.loads(body)
   if req.get('method')=='tools/call':
    p=req.get('params',{});q=p.get('arguments',{}).get('query','')
    event={'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'method':'tools/call','tool':p.get('name'),'query_sha256':hashlib.sha256(q.encode()).hexdigest(),'expected_swedish_query':q==EXPECTED}
    print(json.dumps(event),flush=True)
  except (ValueError,AttributeError,TypeError):pass
  h={k:v for k,v in self.headers.items() if k.lower() not in ('host','connection','transfer-encoding','authorization','cookie')};h['Host']='127.0.0.1:3216'
  c=http.client.HTTPConnection('127.0.0.1',3216,timeout=90)
  try:
   c.request(self.command,self.path,body,h);r=c.getresponse();self.send_response(r.status)
   for k,v in r.getheaders():
    if k.lower() not in ('transfer-encoding','content-length','connection'):self.send_header(k,v)
   self.send_header('Transfer-Encoding','chunked');self.end_headers();seen=b''
   while True:
    b=r.read1(16384)
    if not b:break
    if event:seen=(seen+b)[-65536:]
    self.wfile.write(('%x\r\n'%len(b)).encode()+b+b'\r\n');self.wfile.flush()
   self.wfile.write(b'0\r\n\r\n');self.wfile.flush()
   if event:print(json.dumps({'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'response_status':r.status,'updated_marker_present':b'NK215-B' in seen,'selected_url_present':b'app.notion.com/p/H-Norman-Schwarzkopf-Jr-3ef26b4f59908161b656e6a30784d8ca' in seen}),flush=True)
  except (OSError,TimeoutError):
   print(json.dumps({'event':'proxy_transport_failure','method':self.command}),flush=True);self.close_connection=True
  finally:c.close()
 do_POST=relay;do_GET=relay;do_DELETE=relay
print('private_trace_proxy=127.0.0.1:3218',flush=True)
http.server.ThreadingHTTPServer(('127.0.0.1',3218),Handler).serve_forever()
```
