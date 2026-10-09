#!/usr/bin/env python3
"""Fail-closed release identity and changelog disclosure check (no credentials)."""
import argparse
from datetime import date
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
HEADING = re.compile(r"(?m)^## \[([^\]]+)\] - ([^\n]+)$")
REQUIRED = ("MCP tool-schema changes", "Index/state migrations")


def check(version: str, changelog: str, tag: str | None = None) -> list[str]:
    errors = []
    if not SEMVER.fullmatch(version):
        errors.append("Cargo workspace version must be MAJOR.MINOR.PATCH")
    if "## [Unreleased]" not in changelog:
        errors.append("Changelog needs an [Unreleased] section")
    matches = list(HEADING.finditer(changelog))
    releases = [i for i, m in enumerate(matches) if m.group(1) == version]
    if len(releases) != 1:
        errors.append(f"Changelog needs exactly one [{version}] release section")
    else:
        i = releases[0]
        match = matches[i]
        release_date = match.group(2).strip()
        body = changelog[match.end():matches[i + 1].start() if i + 1 < len(matches) else len(changelog)]
        for title in REQUIRED:
            heading = re.search(r"(?m)^### " + re.escape(title) + r"[ \t]*$", body)
            if not heading:
                errors.append(f"[{version}] missing section: {title}")
                continue
            next_heading = re.search(r"(?m)^###? ", body[heading.end():])
            end = heading.end() + next_heading.start() if next_heading else len(body)
            section = body[heading.end():end]
            if not re.search(r"(?m)^- \S", section):
                errors.append(f"[{version}] needs a bullet under {title}")
        if tag is not None:
            try:
                if release_date == "TBD":
                    raise ValueError("release date is TBD")
                if date.fromisoformat(release_date).isoformat() != release_date:
                    raise ValueError("noncanonical date")
            except ValueError:
                errors.append(f"[{version}] needs a valid dated release, not TBD")
    if tag is not None and tag != f"v{version}":
        errors.append(f"Tag {tag!r} does not match Cargo version v{version}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="Validate a publication tag, e.g. v0.1.0")
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        with (args.root / "Cargo.toml").open("rb") as manifest:
            version = tomllib.load(manifest)["workspace"]["package"]["version"]
        changelog = (args.root / "CHANGELOG.md").read_text(encoding="utf-8")
    except (OSError, KeyError, tomllib.TOMLDecodeError) as error:
        print(f"Release metadata unavailable: {type(error).__name__}", file=sys.stderr)
        return 1
    errors = check(version, changelog, args.tag)
    for error in errors:
        print(f"Release metadata invalid: {error}", file=sys.stderr)
    if not errors:
        print(f"Release metadata valid for {version}" + (f" ({args.tag})" if args.tag else " (draft)"))
    return bool(errors)


if __name__ == "__main__":
    sys.exit(main())
