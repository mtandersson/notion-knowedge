#!/usr/bin/env python3
"""Audit and carefully prune redundant GitHub issue-dependency edges (#266).

Read-only by default. Requires an authenticated `gh` CLI for API access.
Never modifies issue #257 or its adjacent indexing dependency graph.

Examples:
    python3 scripts/prune-issue-dependencies.py --batch A
    python3 scripts/prune-issue-dependencies.py --batch A --apply
    python3 scripts/prune-issue-dependencies.py --batch B
    python3 scripts/prune-issue-dependencies.py --batch B --apply

Batch A removes child-level *duplicate phase gates*; Batch B removes
the explicit parent-level roadmap-order links. Apply A and verify it
before applying B. This deliberately never closes issues or PRs.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Mapping

REPOSITORY = "mtandersson/notion-knowedge"
API_ROOT = f"/repos/{REPOSITORY}/issues"

# Parent-level phase constraints are retained by batch A. None of these
# child-level edges are unique technical prerequisites.
A_EDGES = (
    *((issue, 7) for issue in range(66, 76)),
    *((issue, 9) for issue in (*range(60, 66), *range(76, 94), *range(118, 130))),
    *((issue, blocker) for issue in range(94, 104) for blocker in (8, 10, 11, 129)),
    *((issue, 12) for issue in (*range(104, 116), 145)),
)

# Roadmap order should be represented by priorities/release criteria,
# not hard engineering prerequisites on every implementation issue.
B_EDGES = (
    (9, 7),
    *((issue, 9) for issue in (8, 10, 11, 117)),
    *((12, blocker) for blocker in (8, 10, 11, 129)),
    (13, 12),
    (130, 12),
)

# These functional/security dependencies must not be weakened. Ensure they
# exist both before and after every apply operation. #257 is GET-only here.
PROTECTED = {
    122: {121},
    123: {122},
    124: {123},
    125: {122},
    126: {122},
    127: {120, 125, 126},
    128: {124, 127},
    129: {128},
    254: {257},
    255: {254},
    257: {256, 241, 251},
}

assert len(A_EDGES) == 99
assert len(B_EDGES) == 11
assert len(set(A_EDGES)) == len(A_EDGES)
assert len(set(B_EDGES)) == len(B_EDGES)
assert not set(A_EDGES) & set(B_EDGES)
assert all(issue != 257 and blocker != 257 for issue, blocker in (*A_EDGES, *B_EDGES))


class ApiError(RuntimeError):
    """Sanitized GitHub API failure; never include response body or credentials."""


def gh(method: str, path: str) -> object:
    if method not in {"GET", "DELETE"} or not path.startswith(API_ROOT + "/"):
        raise ApiError("Refusing unsupported GitHub operation or repository")
    # subprocess never uses a shell. Authentication stays inside gh's
    # existing credential store; this program does not read or print it.
    args = ["gh", "api", "--method", method, "-H", "Accept: application/vnd.github+json", path]
    result = subprocess.run(args, text=True, capture_output=True, check=False)
    if result.returncode:
        raise ApiError(f"GitHub {method} request failed; exit code {result.returncode} (response redacted)")
    if method == "DELETE":
        return None
    try:
        return json.loads(result.stdout)
    except ValueError as exc:
        raise ApiError("GitHub returned invalid JSON (response redacted)") from exc


def blockers(issue: int) -> dict[int, int]:
    """Issue number -> GitHub database ID for the actual, current blockers."""
    response = gh("GET", f"{API_ROOT}/{issue}/dependencies/blocked_by?per_page=100")
    if not isinstance(response, list) or len(response) >= 100:
        raise ApiError(f"Unexpected/paginated dependency list for issue #{issue}")
    result: dict[int, int] = {}
    for item in response:
        if not isinstance(item, dict):
            raise ApiError(f"Malformed dependency for issue #{issue}")
        number, issue_id = item.get("number"), item.get("id")
        if not isinstance(number, int) or not isinstance(issue_id, int):
            raise ApiError(f"Missing dependency ID for issue #{issue}")
        if number in result:
            raise ApiError(f"Duplicate dependency for issue #{issue}")
        result[number] = issue_id
    return result


def group_edges(edges: tuple[tuple[int, int], ...]) -> dict[int, set[int]]:
    grouped: dict[int, set[int]] = defaultdict(set)
    for issue, predecessor in edges:
        grouped[issue].add(predecessor)
    return dict(sorted(grouped.items()))


def assert_protected() -> None:
    for issue, required in PROTECTED.items():
        observed = set(blockers(issue))
        if not required <= observed:
            raise ApiError(f"Critical dependency changed for issue #{issue}; refusing mutations")


def plan_removals(
    planned: Mapping[int, set[int]],
    live: Mapping[int, dict[int, int]],
) -> list[tuple[int, int, int]]:
    result: list[tuple[int, int, int]] = []
    for issue, expected in sorted(planned.items()):
        for predecessor in sorted(expected):
            issue_id = live[issue].get(predecessor)
            if issue_id is not None:
                result.append((issue, predecessor, issue_id))
    return result


def ensure_a_cleaned() -> None:
    """Never remove epic phase gates while child duplicates are present."""
    expected = group_edges(A_EDGES)
    for issue, redundant in expected.items():
        if redundant & set(blockers(issue)):
            raise ApiError("Batch A still has live links; complete and verify A before B")


def run(batch: str, apply: bool, delay: float) -> None:
    edges = A_EDGES if batch == "A" else B_EDGES
    grouped = group_edges(edges)
    print(f"Repository: {REPOSITORY} | batch {batch} | mode: {'APPLY' if apply else 'DRY RUN'}")
    assert_protected()
    observed = {issue: blockers(issue) for issue in grouped}
    removals = plan_removals(grouped, observed)
    print(f"Configured edges: {len(edges)} | live: {len(removals)} | already absent: {len(edges) - len(removals)}")
    for issue, predecessor, _ in removals:
        print(f"  #{issue} blocked_by #{predecessor}")

    if not apply:
        print("No mutations performed; rerun with --apply when authorized.")
        return
    if batch == "B":
        ensure_a_cleaned()

    # Stop on the first API conflict. The script is idempotent on rerun.
    # Deleting uses the database ID read from the target issue's live graph.
    for issue, predecessor, issue_id in removals:
        latest = blockers(issue)
        if latest.get(predecessor) != issue_id:
            raise ApiError(f"Dependency changed during migration on issue #{issue}")
        gh("DELETE", f"{API_ROOT}/{issue}/dependencies/blocked_by/{issue_id}")
        if predecessor in blockers(issue):
            raise ApiError(f"Dependency still present after deletion for issue #{issue}")
        print(f"  Removed #{issue} blocked_by #{predecessor}")
        time.sleep(delay)

    assert_protected()
    print(f"Verified {len(removals)} removals and all critical dependency invariants.")
    print("The #257 indexing dependency graph was read but never modified.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True, choices=("A", "B"))
    parser.add_argument("--apply", action="store_true", help="Delete GitHub links; default is read-only")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay after each deletion (default: 1s)")
    args = parser.parse_args(argv)
    if not 0.5 <= args.delay <= 30:
        parser.error("--delay must be between 0.5 and 30 seconds")
    try:
        run(args.batch, args.apply, args.delay)
    except (ApiError, OSError) as exc:
        print(f"ABORTED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
