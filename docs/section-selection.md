# Byte-exact section selection (#311)

`notion_knowledge_core::sections::plan_section` builds a **pure, non-mutating**
candidate for the targeted section update required by #79. It does not grant
write access, call Notion, or enable `knowledge_update_section`.

## Exact anchor contract

The caller identifies the heading by its **ATX level 1–6 plus exact, literal,
single-line heading text**. For example, level `2` and text `Agenda` means
an actual top-level line `## Agenda`. No substring/fuzzy/first-hit matching
or arbitrary client-controlled offsets. Inline Markdown formatting, trailing
attributes and nested/list/quoted headings are not accepted as selector keys.
The parser recognizes fenced code, so a heading-looking line in a code fence
never matches. If the same eligible heading appears twice the planner returns
`Ambiguous`; if absent, `NotFound`.

The selected section **body** starts immediately *after* the target heading
line (the heading is immutable) and ends immediately *before* the next
same-level or higher-level heading. Child headings of lower levels belong to
the selected body. The input is the complete, fresh authoritative Notion
Markdown, **not** index snippets or normalized semantic content.

Only that body is replaceable. The plan retains exact original bytes before
and after, including surrounding headings, Unicode, whitespace, and CRLF,
and computes SHA-256 of both original and candidate full Markdown. Its
`verify_readback` accepts only an exactly matching candidate including both
untouched neighbors. There is no silent newline insertion, no heuristic to
recover missing anchors, and no automatic retry. Empty body or empty
replacement are intentional; an unchanged body yields `NoChange`.

## Unsupported and conservative cases

- Content over 1 MiB or a replacement over 200,000 bytes is rejected.
- NUL, empty/invalid/multiline anchors and malformed boundary positions are
  rejected before any plan is returned.
- If a following sibling heading exists, nonempty replacement content must
  end with a newline so it cannot absorb the next heading.
- New headings at the same or higher level than the selected anchor are denied
  so an inserted sibling/parent cannot escape the intended section boundary.
- Enhanced Notion `<unknown`/`<page`/`<database`/`<data-source` markers
  inside the **selected** range prevent planning. Such constructs elsewhere
  are left untouched *when the parser can establish the exact heading bounds*.
  A raw HTML block that obscures the heading (without a blank-line boundary)
  causes `NotFound`, not a guessed edit. This intentionally favors a denied
  edit over losing unsupported blocks.

### Not yet supported / security boundary

A plan's `preview()` is **for local inspection and test comparisons only**.
Never pass that full page string to `replace_content`: that would produce
a blind full-page overwrite, which #79 explicitly forbids.

Follow-on [#312](https://github.com/mtandersson/notion-knowedge/issues/312)
must verify the Notion range-edit API and implement **one scoped edit**
alongside server-trusted physical root authorization (#87), fresh page
revision, an already-provisioned durable idempotency store (#78),
confirmation policy and authoritative readback. Even with a matching source
hash the preflight is not atomic Notion compare-and-swap.

## Verification

```sh
cargo test -p notion-knowledge-core --test sections --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```
