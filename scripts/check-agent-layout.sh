#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'agent layout check failed: %s\n' "$*" >&2
  exit 1
}

[[ -f AGENTS.md ]] || fail "AGENTS.md is missing"
[[ ! -L AGENTS.md ]] || fail "AGENTS.md must be the canonical regular file"

[[ -L CLAUDE.md ]] || fail "CLAUDE.md must be a compatibility symlink"
[[ "$(readlink CLAUDE.md)" == "AGENTS.md" ]] || fail "CLAUDE.md must point to AGENTS.md"
[[ "$(realpath CLAUDE.md)" == "$(realpath AGENTS.md)" ]] || fail "CLAUDE.md does not resolve to AGENTS.md"

[[ -d .agents/skills ]] || fail ".agents/skills is missing"
[[ ! -L .agents/skills ]] || fail ".agents/skills must be the canonical directory"

for adapter in .claude/skills .codex/skills; do
  [[ -L "$adapter" ]] || fail "$adapter must be a compatibility symlink"
  [[ "$(readlink "$adapter")" == "../.agents/skills" ]] || fail "$adapter must point to ../.agents/skills"
  [[ "$(realpath "$adapter")" == "$(realpath .agents/skills)" ]] || fail "$adapter does not resolve to .agents/skills"
done

skills=(
  adversarial-review
  grill-me
  issue-to-merge
  pick-tickets
  writing-tests
)

for skill in "${skills[@]}"; do
  canonical=".agents/skills/$skill/SKILL.md"
  [[ -f "$canonical" ]] || fail "missing canonical skill: $canonical"

  for entrypoint in .claude/skills .codex/skills; do
    candidate="$entrypoint/$skill/SKILL.md"
    [[ -f "$candidate" ]] || fail "skill not discoverable through $entrypoint: $skill"
    cmp -s "$canonical" "$candidate" || fail "$candidate differs from canonical skill"
  done
done

metadata=".agents/skills/pick-tickets/agents/openai.yaml"
[[ -f "$metadata" ]] || fail "missing pick-tickets OpenAI metadata"

for entrypoint in .claude/skills .codex/skills; do
  candidate="$entrypoint/pick-tickets/agents/openai.yaml"
  [[ -f "$candidate" ]] || fail "OpenAI metadata not discoverable through $entrypoint"
  cmp -s "$metadata" "$candidate" || fail "$candidate differs from canonical metadata"
done

printf 'agent layout check passed: %d shared skills, Claude/Codex adapters resolved\n' "${#skills[@]}"
