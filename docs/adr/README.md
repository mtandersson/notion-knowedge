# Architecture Decision Records

Architecture Decision Records (ADRs) capture decisions that are expensive,
cross-cutting, or important enough that future contributors need to understand
both **what** was chosen and **why**.

## When to write an ADR

Write an ADR when a change materially affects one or more of:

- runtime or language
- component or trust boundaries
- persistence or retrieval technology
- public/internal protocols
- authentication or authorization architecture
- deployment topology
- a dependency that would be costly to replace
- a deliberate architectural constraint that future work must preserve

Routine implementation details do not need ADRs.

## Location and numbering

ADRs live in this directory and use monotonically increasing four-digit
numbers:

```text
0001-runtime-and-component-boundaries.md
0002-example-future-decision.md
```

Use a short kebab-case title. Never reuse a number.

`0000-template.md` is the starting template and is not itself a decision.

## Status

Use one of:

- **Proposed** — under active review
- **Accepted** — current decision
- **Deprecated** — retained for history but no longer recommended
- **Superseded by ADR NNNN** — replaced by a newer decision

An accepted ADR is historical evidence. Do not rewrite its reasoning to make it
look as though a later decision was known at the time. Small typo/link fixes are
fine. Material changes get a new ADR that supersedes the old one.

## Process

1. Open or identify the GitHub issue that motivates the architectural choice.
2. Gather enough evidence to make the tradeoff explicit.
3. Add an ADR using the next number and `0000-template.md`.
4. Link the ADR to the issue and relevant implementation tickets.
5. For trust-boundary changes, update the [threat model](../threat-model.md)
   in the same PR and obtain its required independent security review.
   Review the ADR in the same PR that establishes the decision in code, or in a
   focused documentation PR before implementation when sequencing requires it.
6. Mark the ADR **Accepted** when the decision is approved for implementation.
7. If the decision later changes, add a new ADR and mark the old one
   `Superseded by ADR NNNN`.

For an issue that already captured an accepted decision before this directory
existed, copy the decision into an ADR without changing its substance and link
back to the original issue. ADR 0001 follows that migration path from #14.

## Review checklist

A useful ADR answers:

- What problem/constraint forced a decision?
- What was selected?
- What are the component/dependency boundaries?
- What alternatives were seriously considered?
- Why were they not selected?
- What positive and negative consequences follow?
- Which future tickets depend on the choice?
- What evidence would justify revisiting it?
