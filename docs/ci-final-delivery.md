# Final #186 delivery evidence

The audited report merged through PR190 at `23087aabcc920b1189038b3f8f844710804047f8`.
Actual PR37110206661, full main37110338867 and only-after-main manual37110479728
all passed eleven jobs. Wall/summed-step seconds:96/363,101/362,93/377.
The canonical security and final-image smoke logs are retained, with zero compiler
events across the final/builder/final-export solves and actual compiler RUN and
executable COPY reuse. All tested checkout trees equal the independently reviewed
`882ee53cbbe1d719d4fe5e16f0b6bf6fde23b397`.

This task-owned evidence ref archives the final delivery; it changes no production
source and needs no second merge. Its archival commit is not claimed to have passed
production CI. This record does not close or certify the separate original parent
audits; #176/#155/#151 remain open. Original negative Rust timings and historical
causal limitations remain in the merged complete acceptance report.

Recollect and recalculate the retained immutable data:

```sh
python3 scripts/collect-docker-cache-evidence.py --output /tmp/delivery-186 37110206661 37110338867 37110479728
python3 scripts/calculate-docker-cache-evidence.py --input docs/ci-final-delivery-raw --output /tmp/delivery-186-solves.json
```

The machine ledger additionally retains complete job API identities, checkout
Git/tree/production-input hashes, actual security/smoke excerpts and archive hashes.
The cache snapshot contains only entries attached to task-owned PR190; no cache
was deleted. Collection sanitizes signed URL queries and rejects visible bearer/JWT
values; original and sanitized hashes remain inspectable in the archive manifest.
