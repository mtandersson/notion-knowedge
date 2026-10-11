//! ChatGPT bounded multi-file and legacy single-file parameter schemas for the staged Notion upload tool.
//! A file reference is input metadata, NOT authorization to fetch its URL.
//! Download/SSRF protection and Notion attachment are owned by #67 onward.

use rmcp::model::{Tool, ToolAnnotations};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatGptFile {
    pub download_url: String,
    pub file_id: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadFileRequest {
    #[serde(default)]
    pub file: Option<ChatGptFile>,
    #[serde(default)]
    pub files: Option<Vec<ChatGptFile>>,
}

/// Server-owned batch limit; the schema bounds the absolute maximum, and
/// this setting can only tighten it. No client input controls this value.
pub const DEFAULT_MAX_FILES: usize = 4;
pub const HARD_MAX_FILES: usize = 16;

pub fn configured_max_files() -> usize {
    std::env::var("NK_MAX_UPLOAD_FILES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=HARD_MAX_FILES).contains(value))
        .unwrap_or(DEFAULT_MAX_FILES)
}

pub fn validate_file(file: &ChatGptFile) -> Result<(), &'static str> {
    if file.download_url.trim().is_empty()
        || file.download_url.len() > 8192
        || file.download_url.chars().any(char::is_control)
        || file.file_id.trim().is_empty()
        || file.file_id.len() > 256
        || file.file_id.chars().any(char::is_control)
        || file.mime_type.as_ref().is_some_and(|s| s.len() > 256 || s.chars().any(char::is_control))
        || file.file_name.as_ref().is_some_and(|s| s.len() > 512 || s.chars().any(char::is_control))
    {
        return Err("invalid knowledge_upload_file metadata");
    }
    Ok(())
}

impl UploadFileRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.validate_with_limit(configured_max_files())
    }

    pub fn validate_with_limit(&self, max_files: usize) -> Result<(), &'static str> {
        if !(1..=HARD_MAX_FILES).contains(&max_files) {
            return Err("invalid knowledge_upload_file batch limit");
        }
        match (&self.file, &self.files) {
            (Some(file), None) => validate_file(file),
            (None, Some(files)) if !files.is_empty() && files.len() <= max_files => {
                for file in files {
                    validate_file(file)?;
                }
                Ok(())
            }
            _ => Err("invalid knowledge_upload_file file count"),
        }
    }

    /// Input-order file references for a future isolated attachment adapter.
    /// This parser never opens any URL and never authorizes a Notion write.
    pub fn ordered_files(&self) -> Vec<&ChatGptFile> {
        if let Some(files) = &self.files {
            files.iter().collect()
        } else {
            self.file.iter().collect()
        }
    }
}

pub fn tool() -> Tool {
    let file = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["download_url", "file_id"],
        "properties": {
            "download_url": {"type": "string"},
            "file_id": {"type": "string"},
            "mime_type": {"type": "string"},
            "file_name": {"type": "string"}
        }
    });
    let input = json!({
        "type": "object",
        "additionalProperties": false,
        "oneOf": [{"required":["file"]}, {"required":["files"]}],
        "properties": {
            "file": file,
            "files": {
                "type": "array",
                "minItems": 1,
                "maxItems": HARD_MAX_FILES,
                "items": file
            }
        }
    });
    Tool::new(
        "knowledge_upload_file",
        "Accept one or a bounded ordered array of ChatGPT file references for a future scoped Notion attachment. This stage validates the file parameter only: it never downloads, stores or uploads the file and reports file_upload_unavailable until the authenticated ingestion pipeline is implemented. Never treat the temporary URL as authorization.",
        input.as_object().expect("static upload schema").clone(),
    )
    .with_annotations(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .open_world(true),
    )
    .with_meta(
        serde_json::from_value(json!({"openai/fileParams": ["file","files"]}))
            .expect("static ChatGPT file metadata"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn item(id: &str) -> serde_json::Value {
        json!({"download_url":"https://files.example.test/file?signature=secret","file_id":id})
    }

    #[test]
    fn schema_preserves_legacy_input_and_adds_bounded_file_array() {
        let schema = serde_json::to_value(tool()).unwrap();
        assert_eq!(schema["_meta"]["openai/fileParams"], json!(["file", "files"]));
        let input = &schema["inputSchema"];
        assert_eq!(input["oneOf"], json!([{"required":["file"]},{"required":["files"]}]));
        assert_eq!(input["properties"]["files"]["minItems"],1);
        assert_eq!(input["properties"]["files"]["maxItems"], HARD_MAX_FILES);
        assert_eq!(input["properties"]["files"]["items"]["required"],json!(["download_url","file_id"]));
        assert_eq!(schema["annotations"]["readOnlyHint"],false);
    }

    #[test]
    fn ordered_inputs_and_configurable_limit_are_enforced() {
        let request: UploadFileRequest = serde_json::from_value(json!({
            "files":[item("first"),item("second"),item("third")]
        })).unwrap();
        assert!(request.validate_with_limit(3).is_ok());
        assert!(request.validate_with_limit(2).is_err());
        let ids: Vec<&str> = request.ordered_files().iter().map(|file| file.file_id.as_str()).collect();
        assert_eq!(ids,vec!["first","second","third"]);
        let legacy: UploadFileRequest = serde_json::from_value(json!({"file":item("old")})).unwrap();
        assert_eq!(legacy.ordered_files()[0].file_id,"old");
        assert!(legacy.validate_with_limit(1).is_ok());
    }

    #[test]
    fn reject_both_absent_oversize_or_invalid_metadata_without_echoing_url() {
        let invalid = [
            json!({}),
            json!({"file":item("a"),"files":[item("b")]}),
            json!({"files":[]}),
            json!({"files":[item("a"),item("b")]}),
            json!({"files":[{"file_id":"missing-url"}]}),
        ];
        for value in invalid {
            let request: UploadFileRequest=serde_json::from_value(value).unwrap();
            assert!(request.validate_with_limit(1).is_err());
        }
        let mut too_many = Vec::new();
        for _ in 0..HARD_MAX_FILES + 1 { too_many.push(item("file")); }
        let request: UploadFileRequest=serde_json::from_value(json!({"files":too_many})).unwrap();
        assert!(request.validate_with_limit(HARD_MAX_FILES).is_err());
        let error = request.validate_with_limit(HARD_MAX_FILES).unwrap_err();
        assert!(!error.contains("https://"));
        assert!(serde_json::from_value::<UploadFileRequest>(
            json!({"files":[item("a")],"target":"unauthorized-page"})
        ).is_err());
    }
}
