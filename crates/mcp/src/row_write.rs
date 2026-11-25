use notion_knowledge_core::row_write::{MAX_PROPERTIES, MAX_RELATIONS};
use rmcp::model::{Tool, ToolAnnotations};
use serde_json::{Value, json};

pub const CREATE_TOOL: &str = "knowledge_create_row";
const UUID: &str = "^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$";

fn input_type(kind: &str, value: Value) -> Value {
    json!({"type":"object","additionalProperties":false,"required":["type","value"],
        "properties":{"type":{"const":kind},"value":value}})
}

pub(super) fn create_tool() -> Tool {
    let properties = json!({
        "type":"object","maxProperties":MAX_PROPERTIES,
        "description":"Typed properties keyed by stable Notion property ID. Supply the row title separately.",
        "propertyNames":{"minLength":1,"maxLength":128},
        "additionalProperties":{"oneOf":[
            input_type("date",json!({"type":["object","null"],"additionalProperties":false,"required":["start"],
                "properties":{"start":{"type":"string","maxLength":40},"end":{"type":["string","null"],"maxLength":40}}})),
            input_type("status",json!({"type":"string","minLength":1,"maxLength":128})),
            input_type("select",json!({"type":["string","null"],"minLength":1,"maxLength":128})),
            input_type("number",json!({"type":["number","null"]})),
            input_type("checkbox",json!({"type":"boolean"})),
            input_type("url",json!({"type":["string","null"],"maxLength":2000})),
            input_type("rich_text",json!({"type":"string","maxLength":2000})),
            input_type("relation",json!({"type":"array","maxItems":MAX_RELATIONS,"uniqueItems":true,
                "items":{"type":"string","pattern":UUID}}))
        ]}
    });
    let schema = json!({
        "type":"object","additionalProperties":false,
        "required":["root_page_id","data_source_id","title","markdown","properties","idempotency_key"],
        "properties":{
            "root_page_id":{"type":"string","pattern":UUID,"description":crate::mutations::ROOT_GUIDANCE},
            "data_source_id":{"type":"string","pattern":UUID,"description":"UUID of the original Notion data source."},
            "title":{"type":"string","minLength":1,"maxLength":200},
            "markdown":{"type":"string","minLength":1,"maxLength":200000},
            "properties":properties,
            "idempotency_key":{"type":"string","minLength":16,"maxLength":128}
        }
    });
    Tool::new(CREATE_TOOL,
        "Create one row in an authorized Notion data source with its title, Markdown, and typed properties in one operation. Use existing option IDs for status and select values. Keep the same idempotency key for retries.",
        schema.as_object().unwrap().clone())
        .with_annotations(ToolAnnotations::new().read_only(false).destructive(false).idempotent(true).open_world(true))
}
