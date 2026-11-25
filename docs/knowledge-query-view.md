# Read a saved Notion view

`knowledge_query_view` is available on trusted stdio and loopback
bearer-authenticated MCP with the Notion integration and configured scope.
The read tool also works in read-only mode and uses the integration's connection
permissions. The view's containing database and the original data source must
both be in scope. A linked view does not grant access to an outside source.

```json
{
  "view_id": "view://aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "page_size": 10
}
```

Use the view UUID, with or without the `view://` prefix. Pass the returned
`continuation` object unchanged alongside the same view ID to fetch the next
page. Page size can be 1–25 and defaults to 10. Notion expires the query cache
after 15 minutes. If it expires or the view changes, start a new query.

The result includes the view and data-source IDs, saved filter and sorts, and
rows in Notion's result order. Each row has a stable page ID, its current
revision, and typed Notion property values read at the time of the call.
Notion establishes membership and order when the first page is requested.
Property values can subsequently change as the source is edited.

- `empty` means the first page has no matches.
- `more` means pass the continuation to read the next page.
- `complete` means the last page was returned without a reported truncation.
- `incomplete` means Notion truncated the query results. Continue while a
  continuation is present. Use a more selective saved view for exhaustive runs.

The tool reports inaccessible views or rows, unsupported view types, upstream
errors and responses too large for the tool limit explicitly. Table, list,
board, calendar, timeline and gallery views use Notion's
[view query endpoint](https://developers.notion.com/reference/create-view-query).
It never substitutes a search or an unfiltered data-source query.
