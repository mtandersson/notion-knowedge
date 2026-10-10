//! ChatGPT single-file parameter schema for the staged Notion upload tool.
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
    pub file: ChatGptFile,
}

impl UploadFileRequest {
    /// Bound only the incoming metadata. #67 and #68 must enforce URL origin,
    /// fetch size, redirect, MIME and content validation before using the URL.
    pub fn validate(&self) -> Result<(), &'static str> {
        let file = &self.file;
        if file.download_url.trim().is_empty()
            || file.download_url.len() > 8192
            || file.download_url.chars().any(char::is_control)
            || file.file_id.trim().is_empty()
            || file.file_id.len() > 256
            || file.file_id.chars().any(char::is_control)
            || file
                .mime_type
                .as_ref()
                .is_some_and(|s| s.len() > 256 || s.chars().any(char::is_control))
            || file
                .file_name
                .as_ref()
                .is_some_and(|s| s.len() > 512 || s.chars().any(char::is_control))
        {
            return Err("invalid knowledge_upload_file metadata");
        }
        Ok(())
    }
}

pub fn tool() -> Tool {
    let input = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["file"],
        "properties": {
            "file": {
                "type": "object",
                "additionalProperties": false,
                "required": ["download_url", "file_id"],
                "properties": {
                    "download_url": {"type": "string"},
                    "file_id": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "file_name": {"type": "string"}
                }
            }
        }
    });
    Tool::new(
        "knowledge_upload_file",
        "Accept one ChatGPT image/file reference for a future scoped Notion attachment. This stage validates the file parameter only: it never downloads, stores or uploads the file and reports file_upload_unavailable until the authenticated ingestion pipeline is implemented. Never treat the temporary URL as authorization.",
        input.as_object().expect("static upload schema").clone(),
    )
    .with_annotations(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .open_world(true),
    )
    .with_meta(
        serde_json::from_value(json!({"openai/fileParams": ["file"]}))
            .expect("static ChatGPT file metadata"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn openai_descriptor_has_top_level_file_meta_and_exact_required_fields() {
        let descriptor = serde_json::to_value(tool()).unwrap();
        assert_eq!(descriptor["_meta"]["openai/fileParams"], json!(["file"]));
        let schema = &descriptor["inputSchema"];
        assert_eq!(schema["required"], json!(["file"]));
        let file = &schema["properties"]["file"];
        assert_eq!(file["required"], json!(["download_url", "file_id"]));
        assert_eq!(file["additionalProperties"], false);
        for property in ["download_url", "file_id", "mime_type", "file_name"] {
            assert_eq!(file["properties"][property]["type"], "string");
        }
        assert_eq!(descriptor["annotations"]["readOnlyHint"], false);
    }

    #[test]
    fn single_chatgpt_file_accepts_optional_metadata_without_staging() {
        for value in [
            json!({
                "file": {
                    "download_url": "https://files.example.test/download?q=secret",
                    "file_id": "file_abc"
                }
            }),
            json!({
                "file": {
                    "download_url": "https://files.example.test/download",
                    "file_id": "file_xyz",
                    "mime_type": "image/png",
                    "file_name": "image.png"
                }
            }),
        ] {
            let request: UploadFileRequest = serde_json::from_value(value).unwrap();
            assert!(request.validate().is_ok());
        }
    }

    #[test]
    fn reject_missing_extra_or_malformed_file_without_echoing_contents() {
        for value in [
            json!({"file":{"file_id":"file_abc"}}),
            json!({"file":{"download_url":"https://files.example.test/"}}),
            json!({"file":{"download_url":"secret","file_id":"file_abc","extra":true}}),
            json!({"file":[{"download_url":"https://files.example.test","file_id":"file_abc"}]}),
            json!({
                "file": {
                    "download_url": "https://files.example.test",
                    "file_id": "file_abc"
                },
                "target": "sneaky"
            }),
        ] {
            assert!(serde_json::from_value::<UploadFileRequest>(value).is_err());
        }
        let invalid: UploadFileRequest = serde_json::from_value(json!({
            "file": {
                "download_url": "https://files.example.test/\nsecret",
                "file_id": "file_abc"
            }
        }))
        .unwrap();
        assert_eq!(
            invalid.validate(),
            Err("invalid knowledge_upload_file metadata")
        );
    }
}
