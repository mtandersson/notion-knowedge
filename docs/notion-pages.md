# Exact page metadata

`NotionClient::fetch_page(&str)` retrieves fresh authoritative core `Page`
metadata with `GET /v1/pages/{page_id}`. It accepts hyphenated or compact UUIDs
(case insensitive), normal `https://www.notion.so/Title-<id>` links and
`https://<workspace>.notion.site/<id>` links. Query strings and block fragments
do not change the page identity. Arbitrary hosts, credentials in URLs and
non-HTTPS links are rejected before network access; only the extracted UUID is
sent to the fixed API origin. Returned IDs use lowercase hyphenated UUIDs.
The returned canonical URL is Notion's page URL, not the caller's link.

The result preserves the RFC 3339 edit timestamp and archived flag, extracts
the title and normalizes properties under their stable property IDs (not display
names). Supported values include text, numbers, checkboxes, select/status,
multi-select, dates, people, relations and scalar formula/rollup results.
Property maps are sorted by stable ID; display names and object order do not
affect local metadata. Multi-select names and relation/person IDs are sorted
lexicographically, retaining duplicates. Person and relation UUIDs use lowercase
hyphenated form. Rich-text segment order remains meaningful and is preserved.
Dates preserve their start/end strings without conversion.
Unsupported property types and potentially truncated lists return
`UnsupportedContent`, rather than presenting incomplete metadata as complete.
A separate property-item pagination operation is not implemented here. File
properties and aggregate array rollups therefore require a follow-up adapter
capability. This endpoint does not fetch page content; the full `NotionRead`
implementation waits for #25 instead of returning fabricated content.

Missing/unshared pages map to `NotFound` (Notion deliberately does not
reveal whether a 404 is absence or lack of access), explicit 403 to
`PermissionDenied`, 401 to `Unauthenticated`, and 429 to `RateLimited`.
Transport failures are `Unavailable`; malformed or inconsistent page responses
are `Internal`. Errors contain neither credentials nor raw upstream bodies.
The adapter disables redirects, uses its existing 15-second timeout and bounds
metadata bodies at 2 MiB. Requests use the
[shared limiter and bounded retry policy](notion-rate-limits.md).

This operation uses `Notion-Version: 2026-03-11`. Official references checked
2026-10-03:

- [Retrieve a page](https://developers.notion.com/reference/retrieve-a-page)
- [Page properties](https://developers.notion.com/reference/page-property-values)
- [Retrieve a page property item](https://developers.notion.com/reference/retrieve-a-page-property)

Run credential-free HTTP boundary tests with
`cargo test -p notion-knowledge-notion --locked`. Tests cover URL/ID identity,
authentication/version headers, normalized metadata, untrusted inputs,
missing/inaccessible pages, malformed data, partial properties and size limits.
