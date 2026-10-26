# Bounded shipping-model container measurements (#564)

This opt-in Linux x86_64 runner measures the **tiny synthetic** shipping-runtime
fixture. It does not size representative production, establish live Notion or
ChatGPT acceptance, or close [#455](https://github.com/mtandersson/notion-knowedge/issues/455)
and #416. No real Notion token, public listener, model download, host setting or
filesystem quota change is involved. Use the [production envelope](production-resource-limits.md)
for its separate deployment/monitoring/overload gates.

## Provision separately, then admit deliberately

Use a clean checkout with no tracked or untracked changes (including generated
bytecode); the runner refuses dirty inputs before any Docker command. Use
Python3.11+, local Unix-socket Docker/cgroupv2 and compiler/OpenSSL from the
reviewed Nix environment. Docker CLI and any temporary private user daemon must
come from Nix/Nix shell tooling; do not install distribution packages or change
the existing system daemon. Record the selected context/socket explicitly. A
private user daemon, if provisioned separately, must have its own resource bounds,
socket, data directory and ownership record.
Provision the shipping `webhook-worker` image **after #562's security-fixed merge
and exact-head CI**. Record that actual merge, checkout commit, image revision
label and immutable image ID. The supplied fixed merge must be an ancestor of
the exact checkout; the image's revision label must equal that checkout. This is
an operator attestation about the audited merge, not an online GitHub verifier.
Do not supply an older merge just to pass the ancestry check.

Image builds must be bounded separately through a reviewed builder, or use a
previously provisioned reviewed image. Runtime CPU/memory flags do not constrain
Docker daemon builds. The runner accepts prebuilt images only and forbids
build/pull commands. Provision the existing pinned Python helper image from
`scripts/smoke-retrieval-container.py` beforehand too; all run commands use
`--pull never`. It never changes Docker daemon configuration or mounts the host
Docker socket inside a workload.

Externally provision the pinned Qwen assets outside the checkout. Existing
production checksum verification runs with downloads disabled. Provide the
[explicit resource policy](../deploy/production/policy.example.json), with all
fields filled: the main workload's CPU/memory/PID/FD/tmp/log bounds are enforced;
proxy fields are validated but no proxy is launched in this internal stdio run.
A local image tag is accepted only after inspection matches the supplied
immutable image ID and exact source revision. Evidence stays labelled synthetic;
no local image ID is presented as a production repository digest.

For the currently reviewed local exploratory envelope, the model process uses
CPU2,6GiB memory with no additional swap,256PIDs,1024FDs,256MiB tmpfs and8MiB x2
logs. Provider/helpers separately use CPU0.25,128MiB,no additional swap,32PIDs and
256FDs; they retain bounded logging and run with read-only filesystems. Those
numbers are an exploration budget, not a safe production default. Account for
helper memory, exec sampler overhead and other host workloads when choosing
explicit host start/stop gates. Stop admission on memory pressure rather than
raising limits on a busy shared host.

Compile only the read-only sampler from the exact reviewed source; it does not perform the #454 exhaustion
probe's fault operations:

```sh
cc -nostdlib -static -ffreestanding -fno-builtin -fno-stack-protector -O2 \
  deploy/production/measurement-probe.c -o /absolute/private/measurement-probe
```

Record SHA-256 of both the reviewed `measurement-probe.c` and the executable.
`--attest-probe-built-from-source` declares that the operator compiled that exact
source into that artifact. These supplied hashes are a trusted operator
attestation, **not** cryptographic proof of arbitrary compiler provenance. The
artifact must be operator-owned, executable, at most1MiB and not group/world
writable. The runner checks both hashes, snapshots exact bytes under private
evidence mode0500 and binds only that snapshot. It rechecks protected snapshot
identity before container launch and each sampler exec; replacing the original
artifact cannot change an admitted run. No implicit compilation is introduced.

Create a mode0700 operator-owned evidence parent; choose a new child name. Supply
absolute non-symlinked assets/probe paths and separate assets/evidence locations.
The command below has deliberate image/revision placeholders and performs real
local fixture work once all gates pass:

```sh
python3 scripts/measure-model-container.py \
  --policy /absolute/private/policy.json \
  --assets /absolute/pinned-model-assets \
  --probe /absolute/private/measurement-probe \
  --probe-sha256 ATTESTED_EXECUTABLE_SHA256 \
  --probe-source-sha256 REVIEWED_SOURCE_SHA256 \
  --attest-probe-built-from-source \
  --evidence /absolute/private/evidence/run-1 \
  --source-revision EXACT_CHECKOUT_COMMIT \
  --fixed-merge ACTUAL_SECURITY_FIXED_MERGE \
  --image-id sha256:ACTUAL_INSPECTED_IMAGE_ID \
  --interval 1 --duration 1800 --evidence-bytes 16777216 \
  --start-available-bytes 10737418240 --stop-available-bytes 6442450944 \
  --start-psi 0.5 --stop-psi 2
```

The actual shipping reader launch has its own capped `Popen` hook, synchronous
headroom/abort/deadline check and watchdog that stops its owned container and
terminates the CLI on expiry. Both launch paths enforce the same declared container limits.

The host gates inspect MemAvailable and memory PSI full avg60. Start admission
requires the stricter gate; every later container admission and sampler tick
rechecks stop gates. The run has a finite duration (maximum30minutes), sample
interval0.5–30seconds and evidence budget64KiB–64MiB. A stop condition cancels
further admission and stops only the explicitly owned model containers, preserving
state/receipts. It is not permission to kill another service or change host limits.

## What it actually exercises

The runner reuses the existing shipping-container smoke's Stack and TLS provider
fixture, not a second retrieval implementation. Its uniquely named internal Docker
network aliases the nonsecret local provider as `api.notion.com`; no port is
published. Inherited NK settings and upstream/API credentials are removed.
Only fixed synthetic fixture tokens enter the internal workloads.

A fresh volume pair is initialized by the actual production worker and signed
hint. The runner waits for committed canonical content, private permissions and
completed work. It measures serial worker/model-load+commit, index ensure,
first/warm semantic/lexical/hybrid queries plus fresh get, reader replacement,
retained-state index rebuild and a recovery query. The existing fixture compares
source/chunk identity and snapshots; nonempty operational ledgers must survive
rebuild unchanged. Replacing a reader and rebuilding derived ANN state do not
prove crash recovery for interrupted writes or arbitrary overload; those fault
and production acceptance gates remain #455.

Each running model container is inspected for exact image, non-root isolation,
no published ports and effective CPU/memory/swap/PID/FD/tmp/log settings. The
read-only static probe independently checks kernel CPU/memory/swap/PID and
process FD ceilings. It reads cgroup current/peak RAM, CPU/throttling and OOM
counters, process RSS/high-water/threads/FD count, and allocated bytes for tmp,
index and state. It traverses at most100000entries and depth32, follows no
symlinks and emits bounded output; each exec has a3second deadline. Resource
sampling itself adds one transient exec process to the measured cgroup.

`result.json` and `samples.jsonl` are private mode0600. They record phase times,
sample counts, image/source/probe identities, policy, fixture/hint-event/checkpoint/chunk
identities, ledger digest/counts and retained volume names, without source text,
raw stderr, credentials or process environment. Sampled RSS/FD/disk values can
miss brief peaks; cgroup memory.peak spans each model container lifetime. A short
phase may have zero samples and must not be reported as a measured peak. Disk
values count allocated filesystem blocks, not a filesystem quota or complete
ZFS pool reservation. Production monitoring/disk alerts are separate.

## Retention, checks and limits

Failure diagnostics are bounded static classes, not raw child-process errors.
No automatic retry, volume deletion or Docker prune occurs. The runner removes
only its own containers/network, retains all named state/index volumes on both
success and failure and records their names for deliberate operator follow-up.
Private TLS fixture material remains under the evidence directory. Inspect it
locally. Retention is temporary for investigation, not permission to leave the
experiment deployed indefinitely.

After a successful run, or an explicit decision to end an investigated failure,
export the bounded evidence to the operator's private retained location before
final cleanup. Using the exact recorded identities, remove only this run's
remaining containers, network and retained state/index volumes. Remove image or
build-cache objects only when this run created them exclusively and no existing
consumer uses them; the runner itself creates no builds or image pulls. If a
private user daemon was provisioned for this experiment, stop only that owned
daemon/slice and remove its owned sockets/data directory after export. Verify
those resources and listeners are gone. Preserve pre-existing volumes, images,
services, system Docker daemon and unrelated sockets/slices. Never use global
`docker system prune`, `docker volume prune` or wildcard resource deletion.

Ordinary CI needs no model, Docker, network or credentials:

```sh
python3 scripts/test-measure-model-container.py
python3 scripts/test-production-resources.py
```

These contracts establish wrapper admission, limits, sampling parsing and safe
failure retention. Actual model/container execution is a separate opt-in gate;
no such run is claimed by those tests. Representative workload/page/chunk counts,
concurrency/SLOs, rebuild reserve, measured state/index disk headroom and actual
private disk alerts still need #455 evidence before a release decision. Verify
local warning/critical alert delivery and recovery against the actual monitored
paths; a private local observer is valid without an external notification
service. Bounded temporary storage remains required. A hard state/index disk
quota or external alert destination is not an additional completion gate.
