#!/usr/bin/env python3
"""Trusted #102 adapter for an existing installed hybrid index (no indexing).

Operator supplies the server command, normal private runtime environment, and a
reviewed manifest mapping evaluation chunk IDs to canonical indexed sources.
Protocol input is trusted local evaluation input, never an MCP request option.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shlex
import subprocess
import sys
import time


def require(condition):
    if not condition:
        raise ValueError("invalid evaluation contract")


def signature():
    """Detect index/state replacement or mutation during/between paired runs."""
    index = Path(os.environ["NK_WEBHOOK_INDEX_DIR"])
    state = Path(os.environ["NK_WEBHOOK_STATE_FILE"])
    paths = [index, state]
    paths += sorted(index.rglob("*"))
    paths += [p for p in (Path(str(state) + "-wal"),) if p.exists()]
    records = []
    for path in paths:
        require(not path.is_symlink())
        st = path.stat()
        records.append((str(path), st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns))
    return hashlib.sha256(json.dumps(records).encode()).hexdigest()


class Stdio:
    def __init__(self, command, env):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b""

    def call(self, request):
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()
        if "id" not in request:
            return None
        deadline = time.monotonic() + 120
        while True:
            if b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                response = json.loads(line)
                if response.get("id") == request["id"]:
                    require("error" not in response)
                    return response["result"]
                continue
            require(deadline > time.monotonic())
            require(bool(self.selector.select(deadline - time.monotonic())))
            part = os.read(self.process.stdout.fileno(), 65536)
            require(bool(part))
            self.buffer += part
            require(len(self.buffer) <= 1024 * 1024)

    def close(self):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.process.stdout.close()
        self.selector.close()


def run(request, manifest, command):
    require(request["schema_version"] == 1 and request["mode"] == "hybrid")
    require(manifest["schema_version"] == 1 and manifest["dataset_id"] == request["dataset_id"])
    config = request["graph_boost"]
    require(set(config) == {"enabled", "max_hops", "weight", "candidate_limit"})
    require(type(config["enabled"]) is bool and type(config["max_hops"]) is int
            and 1 <= config["max_hops"] <= 2 and type(config["candidate_limit"]) is int
            and 1 <= config["candidate_limit"] <= 100 and type(config["weight"]) in (int, float)
            and 0 < config["weight"] <= .5 and request["top_k"] == config["candidate_limit"])
    corpus = {c["id"]: c["text"] for p in request["pages"] for c in p["chunks"]}
    require(len(corpus) == sum(len(p["chunks"]) for p in request["pages"]))
    sources = manifest["sources"]
    require(set(sources) == set(corpus))
    mapped = {}
    for source_id, entry in sources.items():
        require(entry["text"] == corpus[source_id] and len(entry["text"]) <= 2000)
        key = (entry["page_id"], entry["chunk_id"])
        require(key not in mapped)
        mapped[key] = (source_id, entry["text"])
    page_map = {}
    for page in request["pages"]:
        canonical = {sources[c["id"]]["page_id"] for c in page["chunks"]}
        require(len(canonical) == 1)
        canonical = canonical.pop()
        require(canonical not in page_map)
        page_map[canonical] = page["id"]
    env = dict(os.environ)
    env.update(NK_RETRIEVAL="true", NK_GRAPH="true",
               NK_GRAPH_BOOST=str(config["enabled"]).lower(),
               NK_GRAPH_BOOST_MAX_HOPS=str(config["max_hops"]),
               NK_GRAPH_BOOST_WEIGHT=str(config["weight"]),
               NK_GRAPH_BOOST_CANDIDATE_LIMIT=str(config["candidate_limit"]))
    before = signature()
    server = Stdio(command, env)
    results = []
    try:
        server.call({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "trusted-graph-evaluation", "version": "1"}}})
        server.call({"jsonrpc": "2.0", "method": "notifications/initialized"})
        # Confirm the graph being evaluated matches the declared corpus too;
        # unrelated local edges must not masquerade as dataset relevance gains.
        for i, page in enumerate(request["pages"], 2):
            canonical = sources[page["chunks"][0]["id"]]["page_id"]
            output = server.call({"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": {
                "name": "knowledge_related", "arguments": {"page_id": canonical, "depth": 1, "limit": 100}}})
            require(not output.get("isError", False))
            related = output["structuredContent"]
            require(not related["limit_reached"])
            outgoing = {page_map[e["target_page_id"]] for e in related["relations"]
                        if e["source_page_id"] == canonical}
            require(outgoing == set(page["links"]))
        for i, query in enumerate(request["queries"], len(request["pages"]) + 2):
            output = server.call({"jsonrpc": "2.0", "id": i, "method": "tools/call", "params": {
                "name": "knowledge_search", "arguments": {"query": query["text"],
                    "mode": "hybrid", "limit": request["top_k"]}}})
            require(not output.get("isError", False))
            hits = output["structuredContent"]["results"]
            ids, scores = [], []
            for hit in hits:
                source_id, expected_text = mapped[(hit["source"]["page_id"], hit["source"]["chunk_id"])]
                require(hit["text"] == expected_text)
                ids.append(source_id)
                scores.append({"source_id": source_id, "score": hit["score"],
                               "matched_paths": hit["matched_paths"]})
            require(len(ids) == len(set(ids)))
            results.append({"query_id": query["id"], "ranked_source_ids": ids, "baseline_scores": scores})
    finally:
        server.close()
    require(before == signature())
    return {"schema_version": 1, "dataset_id": request["dataset_id"], "mode": "hybrid",
            "graph_boost": config, "index_signature": before, "results": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--server", required=True, help="Trusted compiled server command, no shell")
    args = parser.parse_args()
    try:
        raw = sys.stdin.buffer.read(16 * 1024 * 1024 + 1)
        require(len(raw) <= 16 * 1024 * 1024)
        result = run(json.loads(raw), json.loads(args.manifest.read_text()), shlex.split(args.server))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        # Never echo queries, source text, private paths, tokens or server output.
        print("installed-graph-eval: invalid or unavailable evaluation state", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
