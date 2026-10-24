#!/usr/bin/env python3
"""List and optionally delete obsolete, identifiable BuildKit GHA cache entries.

Docker CI moved to GHCR in PR #510. This script runs once on protected main.
GitHub's Actions cache API is used to enumerate exact cache IDs before deletion.
No generic gh cache delete --all: Cargo targets/downloads must be preserved.
"""
import argparse
import json
import os
import re
import subprocess
import sys

REPOSITORY = "mtandersson/notion-knowedge"
ONE_SHOT_TITLE = "chore(ci): purge obsolete BuildKit GHA caches once"
INDEX = re.compile(r"index-nk-container-v1-Linux-X64-[A-Za-z0-9_.-]+-1-[0-9a-f]{8}(?:-sig)?(?:#\d+)?\Z")
BLOB = re.compile(r"buildkit-blob-1-sha256:[0-9a-f]{64}\Z")
LIMIT = 5000
PAGE_SIZE = 100


def gh_api(endpoint, *, delete=False):
    command = ["gh", "api"]
    if delete:
        command += ["-X", "DELETE"]
    command.append(endpoint)
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return None if delete else json.loads(result.stdout)


def collect_caches(api=None):
    """Snapshot *all* pages before deletion; deleting during pagination skips IDs."""
    if api is None:
        api = gh_api
    found = {}
    for page in range(1, LIMIT // PAGE_SIZE + 2):
        body = api(f"/repos/{REPOSITORY}/actions/caches?per_page={PAGE_SIZE}&page={page}")
        items = body["actions_caches"]
        if body.get("total_count", 0) > LIMIT or len(items) > PAGE_SIZE:
            raise RuntimeError("Too many entries; refusing automated deletion")
        for item in items:
            cache_id = item["id"]
            if not isinstance(cache_id, int) or cache_id <= 0 or cache_id in found:
                raise RuntimeError("Malformed or duplicate Actions cache ID")
            found[cache_id] = item
            if len(found) > LIMIT:
                raise RuntimeError("Cache inventory larger than safe limit")
        if len(items) < PAGE_SIZE:
            return list(found.values())
    raise RuntimeError("Cache inventory pagination did not terminate")


def select_obsolete(caches):
    """Only known old container indexes, plus blob keys if no other index exists."""
    indexes = []
    blobs = []
    foreign_indexes = []
    for cache in caches:
        key = cache["key"]
        if INDEX.fullmatch(key):
            indexes.append(cache)
        elif BLOB.fullmatch(key):
            blobs.append(cache)
        elif key.startswith("index-"):
            foreign_indexes.append(cache)
    # BuildKit's "buildkit-blob-1-" is repository-wide, not namespaced by
    # builder scope. Preserve shared layers if any other index may use them.
    selected = indexes + ([] if foreign_indexes else blobs)
    if len({entry["id"] for entry in selected}) != len(selected):
        raise RuntimeError("Duplicate deletion candidate")
    return selected, foreign_indexes


def summarize(caches, selected, foreign_indexes):
    def size(items):
        return sum(int(item["size_in_bytes"]) for item in items)
    print(f"Repository: {REPOSITORY}")
    print(f"Total Actions caches: {len(caches)}, {size(caches):,} bytes")
    print(f"Old BuildKit deletion candidates: {len(selected)}, {size(selected):,} bytes")
    print(f"Unrelated BuildKit-style indexes: {len(foreign_indexes)}")
    if foreign_indexes:
        print("Safety guard: preserving all shared buildkit-blob-1-* blobs.")
    for item in selected:
        print(f"candidate id={item['id']} ref={item['ref']} size={item['size_in_bytes']} key={item['key']}")


def authorize_one_shot():
    if (os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("GITHUB_EVENT_NAME") != "push"):
        raise RuntimeError("Mutation only allowed from protected main push")
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as stream:
        event = json.load(stream)
    if not event.get("head_commit", {}).get("message", "").startswith(ONE_SHOT_TITLE):
        raise RuntimeError("Refusing non-one-shot commit; cleanup must be explicitly reviewed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Delete only inventoried obsolete entries")
    args = parser.parse_args()
    if args.apply:
        authorize_one_shot()
    caches = collect_caches()
    selected, foreign_indexes = select_obsolete(caches)
    summarize(caches, selected, foreign_indexes)
    if not args.apply:
        print("DRY RUN: no cache entry was deleted.")
        return 0
    for cache in selected:
        print(f"Deleting obsolete cache ID {cache['id']} ({cache['size_in_bytes']} bytes)")
        gh_api(f"/repos/{REPOSITORY}/actions/caches/{cache['id']}", delete=True)
    remaining = collect_caches()
    leftovers = {item["id"] for item in remaining} & {item["id"] for item in selected}
    if leftovers:
        raise RuntimeError(f"Deletion unverified for cache IDs: {sorted(leftovers)}")
    print(f"Verified deletion of {len(selected)} obsolete cache entries.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
