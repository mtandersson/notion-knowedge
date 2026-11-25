# Update properties on a Notion database row

`knowledge_update_row` is installed on authenticated HTTP when guarded
integration writes are enabled. Call `knowledge_query_view` for the latest
`page_id` and `row_revision` of the target row. Select the configured root
that contains the row's actual data source.

```json
{
  "root_page_id": "22222222-2222-2222-2222-222222222222",
  "page_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "expected_row_revision": "0000000000000000000000000000000000000000000000000000000000000000",
  "idempotency_key": "example-example-example",
  "properties": {
    "status-id": {"type": "status", "value": "existing-option-id"},
    "note-id": {"type": "rich_text", "value": "Delivered to the team"}
  }
}
```

Replace the example revision with the exact value from the latest view
response. Supply one to 24 property IDs and typed values. Status and select
use existing option IDs. Also supported are date, plain rich text, HTTP(S)
URL, number and checkbox. Use an empty string to clear rich text, and
`null` to clear date, select, URL or number. The Notion request only
addresses the supplied fields. The page body and unrelated fields are
outside the patch.

The connector checks live scope and schema and compares a freshly read row
revision before the PATCH. The revision includes the source timestamp and
stable editable properties, so a changed property can be detected even when
the timestamp is unchanged. If Notion truncates one of those properties, the
view returns a null revision and the guarded update reports unavailable.
The connector serializes active requests to edit the same page. Notion
documents an unconditional
[Update page](https://developers.notion.com/reference/patch-page.md) endpoint,
so an edit from elsewhere can still occur between the last read and the
PATCH. The connector verifies the requested values after the PATCH.

- On `revision_conflict`, reread the row and decide whether to apply the
  change against its new revision.
- Keep the same key and inputs to replay the verified result after a retry.
- On `outcome_unknown`, inspect the row in Notion before attempting another
  write. A timed-out or cancelled PATCH can finish at Notion after the
  connector stops waiting. The connector records the unknown outcome
  durably and does not repeat the PATCH for that key.

The result reports verification, audit, and local refresh independently.
