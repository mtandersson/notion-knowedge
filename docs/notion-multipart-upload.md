# Opt-in Notion multi-part file upload (#70)

The internal `NotionClient::upload_multipart` implements Notion's `multi_part` file upload lifecycle: create a pending upload with `number_of_parts`, stream parts sequentially using `POST /file_uploads/{id}/send` with 1-based `part_number`, complete using `POST /file_uploads/{id}/complete`, and verify the authoritative final `GET /file_uploads/{id}` status is `uploaded`. Unsuccessful/ambiguous part send or completion **never** results in a success receipt or an automatic replay. No Notion page is mutated.

`MultipartPolicy::new(maximum_bytes, threshold_bytes, part_bytes)` is **explicit operator configuration**. Free-plan (5 MiB) deployments should not enable this path; for a plan that supports larger uploads, the threshold must be at least 20 MiB, a part must be 5–20 MiB, the configured maximum must be larger than the threshold, and the adapter caps total size at 100 MiB and count at 1000. There is no implicit upgrading of workspace plan/limits. This is a conservative safety cap, not a claim that Notion permits the selected size on every plan.

Only PDF, PNG and JPEG are eligible for multipart at this stage. Their filenames/extensions and header signatures are checked using the existing validated file policy before any upstream effect, without buffering the full input. Text/CSV/Markdown require a separate whole-file streaming UTF-8 validation path and therefore fail closed. The source remains a request-owned random tempfile with independent stream cursors; it is never a signed download URL passed to Notion.

Mock HTTP integration tests cover ordered part transfer, confirmed final status, failure on a part with no subsequent completion/replay, plus invalid thresholds, plan limits, count and mismatched receipts. No real documents or external credentials are used.

Documentation: [Notion File Upload](https://developers.notion.com/reference/file-upload), [Create File Upload](https://developers.notion.com/reference/create-a-file-upload), [Send](https://developers.notion.com/reference/upload-file), [Complete](https://developers.notion.com/reference/complete-a-file-upload).
