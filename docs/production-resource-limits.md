# Production resource containment (#416 / #454)

This envelope is a starting point for operator review, not a production sizing
recommendation. #416 remains open until #455 records actual deployed peaks,
effective settings, disk alerts, overload and recovery. The local development
Compose file is deliberately unchanged. Coordinate the production image and
mounts with [container deployment](container.md).

Use one dedicated Linux host proxy process and a private loopback application
container. Never expose port 3000 remotely. OAuth reader and integration indexer
are separate roles; apply a measured envelope to each. Health, metrics, receipts,
recovery tools and indexing administration stay private. The proxy routes only
the reviewed OAuth/MCP paths and preserves the Host/Origin boundary from
[production OAuth setup](oauth-production-setup.md).

Copy `deploy/production/policy.example.json` to a private operator path. Every
numeric zero is intentionally invalid: select values only after measuring model
load, query concurrency, index growth and a full rebuild on the actual machine.
The production image must be pinned by digest. Record the image, hardware,
dataset size, peak RSS/CPU/PIDs/FDs, temporary and permanent storage, concurrency,
measurement interval and rebuild duration. Reserve explicit capacity for health
and durable receipt recovery; neither model RSS from another host nor a tiny
local fixture proves sufficient headroom.

The policy fields control:

- `cpus`, `memory_bytes`, `pids`, `nofile`: Docker CPU quota, hard memory plus
  equal memory/swap ceiling (no additional swap), PID and descriptor limits.
- `tmp_bytes`: writable `/tmp` tmpfs ceiling; the root filesystem stays read-only.
  Tmpfs memory counts against the cgroup. Additional writable mounts require
  separate filesystem quotas; this does not bound the total state/index disk.
- `log_bytes`, `log_files`: bounded Docker local log rotation. Proxy access logs
  are disabled to avoid recording OAuth codes and private requests.
- `proxy_connections`: one NGINX worker's total connection slots, including
  upstream sockets; `per_client_connections`, `requests_per_second` and
  `request_burst`: rejection-based per-IP limits, without an upstream waiting
  queue. Behind another proxy, configure trusted client identity separately;
  never trust arbitrary forwarded client IPs. NAT shares a quota.
- `header_bytes`, `body_bytes`: bounded header buffers and request bodies.
- `header_timeout_seconds`: header completion deadline. Body/write/upstream
  timeout fields are **inactivity** limits, not absolute request lifetimes.
  Slow clients that keep making progress can occupy bounded connection slots;
  application deadlines and token-bounded SSE remain necessary. Choose an
  upstream inactivity window consistent with SSE heartbeat behavior.

With a private service environment already prepared:

```sh
python3 scripts/production-resources.py /private/policy.json render \
  --environment /private/service.env --output-directory /private/generated
```

The output directory must be new. Its `compose.json` is a separate application
service. Before starting, add reviewed read-only model/secrets mounts and
role-appropriate private index/state mounts to this generated deployment; the
renderer deliberately does not invent credentials, volumes or worker identity.
Keep UID/GID 65532 ownership and private modes required by the server. For a
read-only OAuth reader, do not add integration writer settings. Validate with
`docker compose -f /private/generated/compose.json config --quiet`.

The complete `nginx.conf` targets a dedicated proxy. Replace its example public
hostname and certificate paths, validate `nginx -t -c /private/generated/nginx.conf`,
and have the operator review the exact config before installing/reloading it.
Do not run this template as an overlay inside another `http {}` block. Proxy
resource containment also needs host/cgroup memory, CPU, PID and FD limits chosen
for its own measured peak; this application Compose does not limit a host NGINX
service. Keep the proxy error log policy under operator review and private.

After an approved launch, record effective Docker settings without dumping the
secret-bearing environment:

```sh
python3 scripts/production-resources.py /private/policy.json inspect \
  --container <exact-container-id> > /private/container-limits.json
```

A running Docker configuration is evidence of assigned bounds, not proof that
all kernel controllers or operational deadlines behave correctly. Record the
actual cgroup v2 `cpu.max`, `memory.max`, `memory.swap.max`, `pids.max`, process
`/proc/<pid>/limits`, mount ceiling and throttling/OOM counters on the host. Save
sanitized `nginx -T` output or a reviewed configuration hash and effective service
limits. Keep all deployment evidence private; never dump service environments,
requests, provider credentials or OAuth callbacks.

Before accepting #455, exercise CPU/memory/PID/FD/tmp pressure, header/body
oversize, slow headers/body/readers, per-client and total connection saturation,
rate overload, upstream stall and provider failure with nonsecret fixtures on
the actual deployment. Demonstrate bounded rejection, private health/recovery
headroom, graceful shutdown, restart and durable receipt recovery. Recheck the
same effective limits after restart. Set private state/index filesystem quotas
and warning/critical alerts with tested delivery and sufficient rebuild reserve.
Local tests of the renderer or a disposable proxy cannot satisfy these gates.

Repository checks use credential-free fixtures:

```sh
python3 scripts/test-production-resources.py
```
