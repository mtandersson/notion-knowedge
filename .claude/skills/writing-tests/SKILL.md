---
name: writing-tests
description: Write tests as lasting specifications of desired behavior. Use when adding or editing tests, including tests for a bug fix or a new feature.
---

# Writing Tests

Name and describe tests for the contract the code should satisfy. Assert the
observable outcome, including relevant boundaries and failure cases. A test
should still read accurately after the fix is complete, without knowledge of
the defect that prompted it.

Prefer a representative range of inputs or states over one accidental example.
For configuration or behavior that crosses packages, test through the highest
production boundary that owns the wiring. Use the repository's existing test
framework and conventions. If no test framework exists, choose one appropriate to the implementation and document
how to run it. Inspect the browser when visual acceptance criteria require it.

Keep tests proportional to the change. Documentation-only or other reversible,
low-impact edits do not need tests that mirror the implementation.
