# Destructive-operation confirmation policy (#82)

This is a fail-closed policy and schema-level safety gate for the Phase 5 semantic write surface, **not an activated Notion delete/archive endpoint**.

## Operation classification and deployment gate

- Non-destructive: `knowledge_create_page`, `knowledge_append`.
- Destructive: `knowledge_update_section`, `knowledge_archive_page`, and future whole-page replacement or deletion operations.
- Unknown names do not inherit permission from a broad writable mode.

The shared MCP handler filters destructive tool discovery and dispatch behind the additional operator flag `NK_DESTRUCTIVE_WRITES` (default `false`). The setting can be `true` only with `NK_READ_ONLY=false`; malformed values abort startup. Both stdio and HTTP apply the same gate.

Even with both toggles enabled, write tools remain **design previews**. No live mutation backend is exposed until separately reviewed authorization, verification and write workflows are integrated.

## Trusted interactive confirmation

`DestructivePolicy::default()` denies every destructive operation. An enabled policy requires a `ConfirmationProvider` injected by the trusted server, *not* a client-supplied boolean or `confirmation_id`. The provider must check the exact independently authorized actor/workspace, target, operation, complete intended payload/revision fingerprint, and idempotency key, and must itself reject forged or replayed approvals.

Missing provider, UI-unavailable, declined or timeout cases fail closed. The gate makes a **single** confirmation attempt, never automatically re-prompts or retries. An approved `ConfirmationPermit` is not cloneable or serializable and can be consumed once, for the exact same request.

The preview schemas request `confirmation_id` for both section replacement and page archive, but that field **never authorizes a write by itself**.

## Security limits and integration requirements

This policy is not Notion root authorization, OAuth, a fresh revision precondition, durable idempotency, or a replacement for authoritative readback. Future live implementations must integrate those independent controls from #77–#81/#83 before any mutation. A changed operation, target, revision, payload, or scope needs a fresh approval.

No cross-process durable confirmation store or revocation of in-flight writes is claimed; those belong to the later server-owned confirmation adapter. Never log raw confirmation tokens, private body content, or credentials.

Verification: `cargo test -p notion-knowledge-core --test destructive` and the complete MCP/server CI.
