# Create a Notion database row

`knowledge_create_row` is installed with the guarded integration write service
on authenticated HTTP. Select a configured `root_page_id` and an original
Notion `data_source_id` under that root. A database may contain several data
sources. Use the identifier of the one that owns the intended row.

```json
{
  "root_page_id": "22222222-2222-2222-2222-222222222222",
  "data_source_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
  "title": "Weekly delivery",
  "markdown": "Delivered the weekly report.",
  "idempotency_key": "example-example-example",
  "properties": {
    "due-id": {"type": "date", "value": {"start": "2026-12-02"}},
    "status-id": {"type": "status", "value": "existing-option-id"},
    "delivered-id": {"type": "checkbox", "value": true}
  }
}
```

Use stable Notion property IDs for up to 24 properties. The title is mapped
to the source's current title field. Status and select values use existing
option IDs. Dates accept ISO dates or timestamps with offsets and an optional
end of the same kind. Also supported are number, checkbox, HTTP(S) URL,
plain rich text and relation values. Relations accept up to ten unique row
IDs across the request, respect the relation field’s cardinality, and require access to the related rows and their
original source within the selected root. Use an empty relation array to
supply no relation targets.

The title, properties and Markdown travel in one Notion create request.
The response distinguishes the verified write, audit and local refresh.
A retry with the same key and inputs can replay a verified result without
another create. If the response says `outcome_unknown`, check the original
source before making another attempt with a different key.

[Notion's data-source row create guide](https://developers.notion.com/guides/data-apis/create-pages-in-a-data-source)
documents connection capabilities and schema requirements.
