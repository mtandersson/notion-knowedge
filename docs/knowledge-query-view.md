# Read a saved Notion view

`knowledge_query_view` is available on trusted stdio and loopback
bearer-authenticated MCP with the Notion integration and configured scope.
The read tool also works in read-only mode and uses the integration's connection
permissions. The view's containing database and the original data source must
both be in scope. A linked view does not grant access to an outside source.

```json
{
  "view_id": "view://aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "page_size": 10,
  "fields": ["Status", "Due"],
  "include_row_revision": false
}
```

`fields` lists column names or property IDs. The title column is always
included. Without `fields` every readable column is returned. An unknown
field is `invalid_input`. Set `include_row_revision` only when the rows will
be updated.

Use the view UUID, with or without the `view://` prefix. Pass the returned
`continuation` object unchanged alongside the same view ID to fetch the next
page. Page size can be 1–25 and defaults to 10. Notion expires the query cache
after 15 minutes. If it expires or the view changes, start a new query.

The result includes the view and data-source IDs, saved filter, quick filters and sorts, and
rows in Notion's result order. The response describes the columns once as `columns` (`id`, `name`, `type`).
Each row has a stable `page_id` and `values`, an array in the same order as
`columns`, read at the time of the call. With `include_row_revision` each row
also has an opaque `row_revision` to pass to
[update the row](knowledge-update-row.md). It is null when the upstream
response truncates an editable property.
Notion establishes membership and order when the first page is requested.
Property values can subsequently change as the source is edited. Title and rich text are returned as plain strings when they contain only text
runs. Dates are `{start, end?, time_zone?}`. Status and select values are
`{id, name}`, so their option IDs can be used in writes. Checkboxes, numbers, URLs, email,
phone numbers and timestamps are also supported. `omitted_columns` lists
the names of columns that are never projected, including files, relations,
rollups and formulas. A row's `omitted` lists column IDs whose value could not
be safely projected in that row, such as rich text with mentions; its value is
null. Neither ever includes the value.

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
