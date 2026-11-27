# MCP tool size measurements

Sizes are compact JSON bytes measured in the installed HTTP transport with
synthetic data. Guard tests: `installed_write_catalog_stays_within_its_size_budget`
(`crates/server/src/http/row_write_tests.rs`) and
`ten_row_view_response_stays_compact` (`crates/server/src/http/view_tests.rs`).
They measure MCP-level size, not ChatGPT token use.

| Measurement | Baseline (`b285cf2`) | After #810 and #812 |
| --- | ---: | ---: |
| Catalog, nine tools | 28 518 B | 27 862 B |
| Catalog after #818 (seven listed tools) | | 24 315 B |
| Ten-row view, three columns | 7 800 B | 2 080 B |
| Ten-row view with row revisions | 7 800 B | 2 900 B |

Per tool after #812: `knowledge_search` 7 152, `knowledge_create_row` 3 747,
`knowledge_update_row` 3 501, `knowledge_sync_status` 3 466,
`knowledge_get` 3 064, `knowledge_append` 2 180, `knowledge_create_page`
1 944, `knowledge_query_view` 1 470 and `knowledge_upload_file` 1 338.

After #818 `knowledge_create_page` is 3 910 B and `knowledge_update_page` 3 915 B; the row, append and old create tools are no longer listed.

After #819 `knowledge_get` with its saved-view mode is 4 377 B, replacing
`knowledge_get` (3 064 B) and `knowledge_query_view` (1 470 B). The
`data_source_id` listing mode (#839) adds about 330 B, to about 4 700 B.

The catalog barely shrank from compaction alone. The large remaining cost is
the number of tools and their typed-property schemas, which #813 addresses by
merging the page and row tools.

## Verification with a real ChatGPT connection

Not automated. After the image runs on Astra, refresh the connector's tool
metadata and check:

1. `tools/list` shows `fields` and `include_row_revision` on the saved-view
   mode of `knowledge_get`.
2. A read of selected columns, a follow-up page through `continuation`, and a
   read followed by `knowledge_update_page` with `patch_properties` all succeed.
3. Whether ChatGPT exposes `structuredContent`, the JSON text copy in
   `content`, or both to the model. Drop the duplicate only if the model sees
   the structured form.
