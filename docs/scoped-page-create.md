# Scoped Notion page creation (#77)

The internal `NotionClient::create_scoped_page` workflow takes an explicit Notion parent page ID, title, Markdown content, optional emoji icon and trusted server-owned `LifecycleScope`. Unknown arbitrary property maps are never accepted. It checks the canonical parent UUID, bounded title/body/icon and control/bidi spoofing, then authorizes the live **physical** Notion ancestry under the configured root and revalidates the scope immediately before a single page creation POST.

A creation receipt must include a canonical returned page UUID, HTTPS Notion URL and the correct physical parent. Then up to three **read-only GET /pages/{id}** attempts confirm matching page, parent, URL, title and active status before `ScopedPageCreated { page_id, parent_page_id, url, verified:true }` may be returned. Incorrect or missing readback fails as a typed `Conflict`. A successful but unverified/ambiguous POST is never retried, preventing duplicate pages. Callers must reconcile the returned/known receipt before manually retrying.

This is an internal adapter, not a public MCP write tool. It does not expand authorization, accept untrusted properties, write outside the allowed subtree or introduce full-page overwrite. Tests use credential-free mock Notion API with live root ancestry and verification outcomes, denied roots and exhausted readback.
