//! Fail-closed, upload-independent validation of local Notion file inputs.
//!
//! Run this after a bounded download and before creating a Notion File Upload.
//! No source bytes, filenames or signed URLs are included in errors.

/// Conservative Notion Free-plan limit for a single file.
pub const DEFAULT_MAX_FILE_BYTES: usize = 5 * 1024 * 1024;
/// Upper safety bound for this single-part validator; multipart is separate.
pub const MAX_SINGLE_PART_BYTES: usize = 20 * 1024 * 1024;
const MAX_FILENAME_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileValidationError {
    InvalidLimit,
    EmptyFile,
    FileTooLarge,
    InvalidFilename,
    UnsupportedExtension,
    UnsupportedMime,
    MimeExtensionMismatch,
    ContentTypeMismatch,
}

impl std::fmt::Display for FileValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidLimit => "invalid single-part file limit",
            Self::EmptyFile => "file is empty",
            Self::FileTooLarge => "file exceeds the configured upload limit",
            Self::InvalidFilename => "file has an invalid name",
            Self::UnsupportedExtension => "file extension is not allowed",
            Self::UnsupportedMime => "declared MIME type is not allowed",
            Self::MimeExtensionMismatch => "file extension does not match declared MIME type",
            Self::ContentTypeMismatch => "file bytes do not match the declared MIME type",
        };
        f.write_str(message)
    }
}

impl std::error::Error for FileValidationError {}

/// A prevalidated, presentation-safe filename plus trusted media metadata.
///
/// The caller must pass the **same** validated bytes to the later upload API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedFile {
    pub filename: String,
    pub mime_type: &'static str,
    pub size_bytes: usize,
}

/// Deliberately restricts the input surface to formats with useful signatures
/// or an inspectable UTF-8 representation. Active formats (SVG, HTML, Office
/// macros, archives and executables) are intentionally not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Png,
    Jpeg,
    Gif,
    Webp,
    Pdf,
    Text,
    Markdown,
    Csv,
}

impl Kind {
    fn mime(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Gif => "image/gif",
            Self::Webp => "image/webp",
            Self::Pdf => "application/pdf",
            Self::Text => "text/plain",
            Self::Markdown => "text/markdown",
            Self::Csv => "text/csv",
        }
    }

    fn from_mime(value: &str) -> Option<Self> {
        match value {
            "image/png" => Some(Self::Png),
            "image/jpeg" => Some(Self::Jpeg),
            "image/gif" => Some(Self::Gif),
            "image/webp" => Some(Self::Webp),
            "application/pdf" => Some(Self::Pdf),
            "text/plain" => Some(Self::Text),
            "text/markdown" => Some(Self::Markdown),
            "text/csv" => Some(Self::Csv),
            _ => None,
        }
    }

    fn accepts_extension(self, extension: &str) -> bool {
        match self {
            Self::Png => extension == "png",
            Self::Jpeg => matches!(extension, "jpg" | "jpeg"),
            Self::Gif => extension == "gif",
            Self::Webp => extension == "webp",
            Self::Pdf => extension == "pdf",
            Self::Text => extension == "txt",
            Self::Markdown => matches!(extension, "md" | "markdown"),
            Self::Csv => extension == "csv",
        }
    }

    fn accepts_content(self, bytes: &[u8]) -> bool {
        match self {
            Self::Png => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            Self::Jpeg => bytes.starts_with(b"\xff\xd8\xff"),
            Self::Gif => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
            Self::Webp => {
                bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP"
            }
            Self::Pdf => bytes.starts_with(b"%PDF-"),
            Self::Text | Self::Markdown | Self::Csv => {
                !looks_binary(bytes)
                    && std::str::from_utf8(bytes).is_ok_and(|value| {
                        value
                            .chars()
                            .all(|ch| !ch.is_control() || matches!(ch, '\r' | '\n' | '\t'))
                    })
            }
        }
    }
}

fn looks_binary(bytes: &[u8]) -> bool {
    bytes.starts_with(b"MZ")
        || bytes.starts_with(b"\x7fELF")
        || bytes.starts_with(b"PK\x03\x04")
        || bytes.starts_with(b"%PDF-")
        || bytes.starts_with(b"\x89PNG")
        || bytes.starts_with(b"\xff\xd8\xff")
        || bytes.starts_with(b"GIF87a")
        || bytes.starts_with(b"GIF89a")
        || (bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP")
}

fn parse_mime(declared: &str) -> Result<Kind, FileValidationError> {
    // Only a single optional UTF-8 charset parameter is accepted for text.
    // Never trust a MIME prefix such as image/png;application/x-executable.
    let mut parts = declared.trim().split(';');
    let name = parts.next().unwrap_or("").trim().to_ascii_lowercase();
    let kind = Kind::from_mime(&name).ok_or(FileValidationError::UnsupportedMime)?;
    if let Some(parameter) = parts.next()
        && (!matches!(kind, Kind::Text | Kind::Markdown | Kind::Csv)
            || !parameter.trim().eq_ignore_ascii_case("charset=utf-8")
            || parts.next().is_some())
    {
        return Err(FileValidationError::UnsupportedMime);
    }
    Ok(kind)
}

fn is_bidi_control(ch: char) -> bool {
    matches!(ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
}

fn sanitize_filename(input: &str) -> Result<String, FileValidationError> {
    if input.is_empty() || input.len() > 4096 || input.chars().any(|ch| ch == '\0') {
        return Err(FileValidationError::InvalidFilename);
    }
    let basename = input.rsplit(['/', '\\']).next().unwrap_or("");
    let basename = basename.trim().trim_matches('.');
    if basename.is_empty() || matches!(basename, "." | "..") {
        return Err(FileValidationError::InvalidFilename);
    }
    let (stem, extension) = basename
        .rsplit_once('.')
        .ok_or(FileValidationError::UnsupportedExtension)?;
    let extension = extension.to_ascii_lowercase();
    if !matches!(
        extension.as_str(),
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "pdf" | "txt" | "md" | "markdown" | "csv"
    ) {
        return Err(FileValidationError::UnsupportedExtension);
    }
    let stem: String = stem
        .trim()
        .chars()
        .map(|ch| {
            if !is_bidi_control(ch) && (ch.is_alphanumeric() || matches!(ch, ' ' | '-' | '_' | '.'))
            {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let stem = stem.trim_matches([' ', '.', '_']);
    if stem.is_empty() {
        return Err(FileValidationError::InvalidFilename);
    }
    let mut stem = stem.to_owned();
    // Windows recognizes device names even before an extra filename suffix.
    let device = stem.split('.').next().unwrap_or("").trim_end();
    let upper = device.to_ascii_uppercase();
    let numbered_device = upper
        .strip_prefix("COM")
        .or_else(|| upper.strip_prefix("LPT"))
        .is_some_and(|suffix| {
            matches!(
                suffix,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        });
    if matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL") || numbered_device {
        stem.insert(0, '_');
    }
    let max_stem_bytes = MAX_FILENAME_BYTES - extension.len() - 1;
    if stem.len() > max_stem_bytes {
        let mut cut = max_stem_bytes;
        while !stem.is_char_boundary(cut) {
            cut -= 1;
        }
        stem.truncate(cut);
        stem = stem.trim_end_matches([' ', '.', '_']).to_owned();
    }
    if stem.is_empty() {
        return Err(FileValidationError::InvalidFilename);
    }
    Ok(format!("{stem}.{extension}"))
}

/// Size and format policy for *one* single-part Notion upload.
///
/// The default 5 MiB setting is deliberately safe for the Free plan. Raising
/// it requires an operator to verify plan/API limits; multipart must not use
/// this policy. No caller-supplied value can bypass the single-part ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileValidationPolicy {
    max_bytes: usize,
}

impl Default for FileValidationPolicy {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_FILE_BYTES,
        }
    }
}

impl FileValidationPolicy {
    pub fn for_single_part(max_bytes: usize) -> Result<Self, FileValidationError> {
        if max_bytes == 0 || max_bytes > MAX_SINGLE_PART_BYTES {
            return Err(FileValidationError::InvalidLimit);
        }
        Ok(Self { max_bytes })
    }

    pub fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Validate fully downloaded bytes before opening any Notion upload.
    /// Errors deliberately reveal neither a private filename nor byte content.
    pub fn validate(
        self,
        filename: &str,
        declared_mime: &str,
        bytes: &[u8],
    ) -> Result<ValidatedFile, FileValidationError> {
        if bytes.is_empty() {
            return Err(FileValidationError::EmptyFile);
        }
        if bytes.len() > self.max_bytes {
            return Err(FileValidationError::FileTooLarge);
        }
        let filename = sanitize_filename(filename)?;
        let kind = parse_mime(declared_mime)?;
        let extension = filename
            .rsplit_once('.')
            .map(|(_, ext)| ext)
            .ok_or(FileValidationError::UnsupportedExtension)?;
        if !kind.accepts_extension(extension) {
            return Err(FileValidationError::MimeExtensionMismatch);
        }
        if !kind.accepts_content(bytes) {
            return Err(FileValidationError::ContentTypeMismatch);
        }
        Ok(ValidatedFile {
            filename,
            mime_type: kind.mime(),
            size_bytes: bytes.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNG: &[u8] = b"\x89PNG\r\n\x1a\n\x00fixture";
    const JPG: &[u8] = b"\xff\xd8\xff\xe0fixture";
    const GIF: &[u8] = b"GIF89a\x01\x00fixture";
    const WEBP: &[u8] = b"RIFF\x04\x00\x00\x00WEBPfixture";
    const PDF: &[u8] = b"%PDF-1.7\nfixture";

    #[test]
    fn default_bound_and_operator_overrides_are_checked() {
        let policy = FileValidationPolicy::default();
        assert_eq!(policy.max_bytes(), 5 * 1024 * 1024);
        assert_eq!(
            FileValidationPolicy::for_single_part(0),
            Err(FileValidationError::InvalidLimit)
        );
        assert_eq!(
            FileValidationPolicy::for_single_part(MAX_SINGLE_PART_BYTES + 1),
            Err(FileValidationError::InvalidLimit)
        );
        assert_eq!(
            FileValidationPolicy::for_single_part(1024)
                .unwrap()
                .max_bytes(),
            1024
        );
    }

    #[test]
    fn verifies_supported_magic_signatures_and_aliases() {
        for (name, mime, bytes) in [
            ("photo.png", "image/png", PNG),
            ("photo.jpeg", "IMAGE/JPEG", JPG),
            ("photo.jpg", "image/jpeg", JPG),
            ("animation.gif", "image/gif", GIF),
            ("picture.webp", "image/webp", WEBP),
            ("paper.pdf", "application/pdf", PDF),
            (
                "notes.txt",
                "text/plain; charset=utf-8",
                "hej\nvärlden".as_bytes(),
            ),
            ("notes.md", "text/markdown", b"# Header".as_slice()),
            ("notes.markdown", "text/markdown", b"hej".as_slice()),
            ("data.csv", "text/csv", b"name,value\nhej,1".as_slice()),
        ] {
            let result = FileValidationPolicy::default()
                .validate(name, mime, bytes)
                .unwrap();
            assert_eq!(result.size_bytes, bytes.len());
        }
    }

    #[test]
    fn validates_size_before_external_effects() {
        let policy = FileValidationPolicy::for_single_part(8).unwrap();
        assert_eq!(
            policy.validate("valid.txt", "text/plain", b"123456789"),
            Err(FileValidationError::FileTooLarge)
        );
        assert!(
            policy
                .validate("valid.txt", "text/plain", b"12345678")
                .is_ok()
        );
        assert_eq!(
            policy.validate("empty.txt", "text/plain", b""),
            Err(FileValidationError::EmptyFile)
        );
    }

    #[test]
    fn spoofed_mime_and_binary_payloads_fail_closed() {
        let policy = FileValidationPolicy::default();
        for (name, mime, bytes) in [
            ("image.png", "image/png", b"not a PNG".as_slice()),
            ("image.png", "image/jpeg", PNG),
            ("fake.txt", "text/plain", PDF),
            ("fake.csv", "text/csv", b"PK\x03\x04archive".as_slice()),
            ("fake.md", "text/markdown", b"MZexecutable".as_slice()),
            ("fake.txt", "text/plain", b"line\x00binary".as_slice()),
            ("fake.txt", "text/plain", b"\xffbad UTF8".as_slice()),
        ] {
            assert!(policy.validate(name, mime, bytes).is_err(), "{name}");
        }
    }

    #[test]
    fn unsupported_and_active_formats_are_rejected() {
        let policy = FileValidationPolicy::default();
        for (name, mime) in [
            ("evil.svg", "image/svg+xml"),
            ("active.html", "text/html"),
            ("script.exe", "application/octet-stream"),
            (
                "macro.docm",
                "application/vnd.ms-word.document.macroEnabled.12",
            ),
            ("bundle.zip", "application/zip"),
        ] {
            assert!(policy.validate(name, mime, b"payload").is_err(), "{name}");
        }
        assert_eq!(
            policy.validate("image.png", "image/png; charset=utf-8", PNG),
            Err(FileValidationError::UnsupportedMime)
        );
    }

    #[test]
    fn path_traversal_and_unsafe_filename_characters_are_normalized() {
        let policy = FileValidationPolicy::default();
        assert_eq!(
            policy
                .validate("../../my:secret?.PDF", "application/pdf", PDF)
                .unwrap()
                .filename,
            "my_secret.pdf"
        );
        assert_eq!(
            policy
                .validate(r"C:\Users\alice\report.pdf", "application/pdf", PDF)
                .unwrap()
                .filename,
            "report.pdf"
        );
        assert_eq!(
            policy
                .validate("räkning 2026.pdf", "application/pdf", PDF)
                .unwrap()
                .filename,
            "räkning 2026.pdf"
        );
    }

    #[test]
    fn rejects_bad_names_and_does_not_echo_private_inputs_in_errors() {
        let policy = FileValidationPolicy::default();
        for filename in [
            "",
            "../",
            ".pdf",
            "no_extension",
            "secret.txt.exe",
            "bad\0name.txt",
        ] {
            let error = policy
                .validate(filename, "text/plain", b"safe")
                .unwrap_err();
            if !filename.is_empty() {
                assert!(!error.to_string().contains(filename));
            }
        }
    }

    #[test]
    fn bounds_filename_bytes_without_splitting_unicode() {
        let long = format!("{}.pdf", "å".repeat(200));
        let result = FileValidationPolicy::default()
            .validate(&long, "application/pdf", PDF)
            .unwrap();
        assert!(result.filename.len() <= MAX_FILENAME_BYTES);
        assert!(result.filename.ends_with(".pdf"));
    }

    #[test]
    fn harmless_unicode_and_reserved_names_are_safe() {
        assert_eq!(sanitize_filename("CON.txt").unwrap(), "_CON.txt");
        assert_eq!(
            sanitize_filename("rapport\u{202e}evil.pdf").unwrap(),
            "rapport_evil.pdf"
        );
        assert!(is_bidi_control('\u{202e}'));
    }

    #[test]
    fn windows_device_names_are_safe_even_with_additional_suffixes() {
        let policy = FileValidationPolicy::default();
        for stem in [
            "CON",
            "con.notes",
            "NUL.backup",
            "PRN",
            "AUX",
            "COM4",
            "com9.notes",
            "LPT4",
            "lpt9.backup",
            "COM¹",
            "LPT²",
            "COM³",
            "CON .notes",
        ] {
            let name = format!("{stem}.txt");
            let result = policy.validate(&name, "text/plain", b"notes").unwrap();
            assert!(result.filename.starts_with('_'), "{name}");
            assert!(result.filename.ends_with(".txt"));
        }
        for name in ["company.txt", "COM10.txt", "LPT10.txt", "notes.CON.txt"] {
            assert_eq!(
                policy
                    .validate(name, "text/plain", b"notes")
                    .unwrap()
                    .filename,
                name
            );
        }
    }

    #[test]
    fn output_types_and_errors_do_not_contain_input_data() {
        let error = FileValidationPolicy::default()
            .validate("private.pdf", "image/png", PNG)
            .unwrap_err();
        assert_eq!(error, FileValidationError::MimeExtensionMismatch);
        assert!(!format!("{error:?} {error}").contains("private.pdf"));
    }
}
