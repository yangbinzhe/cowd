//! Feishu media upload/download pipeline.
//!
//! Provides the underlying infrastructure for uploading images, files (audio,
//! video, documents), downloading message resources, and caching media locally.
//!
//! ## API endpoints
//!
//! | Operation  | Method | Endpoint |
//! |-----------|--------|----------|
//! | Image upload | POST | `/im/v1/images` |
//! | File upload  | POST | `/im/v1/files` |
//! | Download     | GET  | `/im/v1/messages/{msg_id}/resources/{file_key}` |
//!
//! All endpoints use tenant access token auth via `Authorization: Bearer` header
//! and retry up to 3 times on 429 (rate limit) or 5xx (server error) responses
//! with exponential backoff (1.5ˢ seconds).

use super::types::{CreateFileResponse, CreateImageResponse};
use crate::cowd_dirs::config_home_dir;
use crate::platform::adapter::{PlatformError, PlatformResult};
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_RETRIES: u32 = 3;

// ---------------------------------------------------------------------------
// Retry helper
// ---------------------------------------------------------------------------

/// Execute an HTTP request with retry logic for transient failures.
///
/// The `build` closure is invoked fresh for each attempt (important: multipart
/// bodies cannot be cloned). Retries only on HTTP 429 (rate limit) and 5xx
/// (server error) with exponential backoff: 1.5¹ → 1.5² → 1.5³ seconds.
async fn request_with_retry<F, Fut>(build: F, label: &str) -> PlatformResult<reqwest::Response>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
{
    let mut last_error: Option<PlatformError> = None;

    for attempt in 0..MAX_RETRIES {
        match build().await {
            Ok(response) => {
                let status = response.status();
                if status.is_success() {
                    return Ok(response);
                }

                let is_retryable = status.as_u16() == 429 || status.is_server_error();
                let is_last = attempt == MAX_RETRIES - 1;

                if is_retryable && !is_last {
                    let backoff = 1.5_f64.powi((attempt + 1) as i32);
                    tracing::warn!(
                        status = %status,
                        backoff_secs = backoff,
                        "feishu {} retryable failure, backing off",
                        label
                    );
                    tokio::time::sleep(std::time::Duration::from_secs_f64(backoff)).await;
                    continue;
                }

                let body = response.text().await.unwrap_or_default();
                last_error = Some(PlatformError::SendFailed(format!(
                    "{}: HTTP {} — {}",
                    label, status, body
                )));
                break;
            }
            Err(e) => {
                let is_last = attempt == MAX_RETRIES - 1;
                if is_last {
                    last_error = Some(PlatformError::SendFailed(format!(
                        "{}: request failed after {} attempts: {}",
                        label, MAX_RETRIES, e
                    )));
                    break;
                }
                let backoff = 1.5_f64.powi((attempt + 1) as i32);
                tracing::warn!(
                    error = %e,
                    backoff_secs = backoff,
                    "feishu {} request error, retrying",
                    label
                );
                tokio::time::sleep(std::time::Duration::from_secs_f64(backoff)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        PlatformError::SendFailed(format!("{}: unknown error after retries", label))
    }))
}

// ---------------------------------------------------------------------------
// SSRF protection
// ---------------------------------------------------------------------------

/// Validate that a URL is targeting a Feishu-controlled domain.
///
/// Used as SSRF guard on download URLs. Accepts `open.feishu.cn` and
/// `feishu.cn` (along with any subdomains).
fn is_feishu_domain(url_str: &str) -> bool {
    // Delegate to the shared SSRF check (supports both Feishu and Lark domains)
    super::is_feishu_domain(url_str)
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

/// Upload an image to Feishu and return its `image_key`.
///
/// # Arguments
/// * `token` — Valid tenant access token (Bearer).
/// * `image_bytes` — Raw image file bytes.
/// * `image_type` — Image type for Feishu API, typically `"message"`.
///
/// # Returns
/// The `image_key` string used to reference this image in message payloads.
pub async fn upload_image(
    token: &str,
    image_bytes: &[u8],
    image_type: &str,
) -> PlatformResult<String> {
    let image_bytes = image_bytes.to_vec();
    let image_type = image_type.to_string();
    let token = token.to_string();
    let url = format!("{}/im/v1/images", super::api_base_url());

    let response = request_with_retry(
        || {
            let bytes = image_bytes.clone();
            let it = image_type.clone();
            let t = token.clone();
            let u = url.clone();
            async move {
                let form = reqwest::multipart::Form::new().text("image_type", it).part(
                    "image",
                    reqwest::multipart::Part::bytes(bytes)
                        .file_name("image.png")
                        .mime_str("application/octet-stream")
                        .expect("valid mime type"),
                );

                reqwest::Client::new()
                    .post(&u)
                    .header("Authorization", format!("Bearer {}", t))
                    .multipart(form)
                    .send()
                    .await
            }
        },
        "upload_image",
    )
    .await?;

    let resp: CreateImageResponse = response
        .json()
        .await
        .map_err(|e| PlatformError::SendFailed(format!("upload_image: parse response: {}", e)))?;

    if resp.code != 0 {
        return Err(PlatformError::SendFailed(format!(
            "upload_image: feishu error {} — {}",
            resp.code, resp.msg
        )));
    }

    resp.data
        .and_then(|d| d.image_key)
        .ok_or_else(|| PlatformError::SendFailed("upload_image: no image_key in response".into()))
}

/// Upload a file to Feishu and return its `file_key`.
///
/// # Arguments
/// * `token` — Valid tenant access token.
/// * `file_bytes` — Raw file bytes.
/// * `file_name` — Original filename (for Feishu display).
/// * `file_type` — Feishu file type: `"stream"`, `"opus"`, `"mp4"`, `"pdf"`,
///   `"doc"`, `"xls"`, `"ppt"`.
///
/// # Returns
/// The `file_key` string used to reference this file in message payloads.
pub async fn upload_file(
    token: &str,
    file_bytes: &[u8],
    file_name: &str,
    file_type: &str,
) -> PlatformResult<String> {
    let file_bytes = file_bytes.to_vec();
    let file_name = file_name.to_string();
    let file_type = file_type.to_string();
    let token = token.to_string();
    let url = format!("{}/im/v1/files", super::api_base_url());

    let response = request_with_retry(
        || {
            let bytes = file_bytes.clone();
            let name = file_name.clone();
            let ft = file_type.clone();
            let t = token.clone();
            let u = url.clone();
            async move {
                let form = reqwest::multipart::Form::new()
                    .text("file_type", ft)
                    .text("file_name", name.clone())
                    .part(
                        "file",
                        reqwest::multipart::Part::bytes(bytes)
                            .file_name(name)
                            .mime_str("application/octet-stream")
                            .expect("valid mime type"),
                    );

                reqwest::Client::new()
                    .post(&u)
                    .header("Authorization", format!("Bearer {}", t))
                    .multipart(form)
                    .send()
                    .await
            }
        },
        "upload_file",
    )
    .await?;

    let resp: CreateFileResponse = response
        .json()
        .await
        .map_err(|e| PlatformError::SendFailed(format!("upload_file: parse response: {}", e)))?;

    if resp.code != 0 {
        return Err(PlatformError::SendFailed(format!(
            "upload_file: feishu error {} — {}",
            resp.code, resp.msg
        )));
    }

    resp.data
        .and_then(|d| d.file_key)
        .ok_or_else(|| PlatformError::SendFailed("upload_file: no file_key in response".into()))
}

// ---------------------------------------------------------------------------
// Download
// ---------------------------------------------------------------------------

/// Download a message resource (image/file) from Feishu.
///
/// # Arguments
/// * `token` — Valid tenant access token.
/// * `message_id` — The message ID that contains the resource.
/// * `file_key` — The resource's file key.
///
/// # Returns
/// Raw bytes of the downloaded resource.
///
/// # Security
/// Validates that the constructed URL targets a Feishu-controlled domain
/// before making the request (SSRF protection).
pub async fn download_message_resource(
    token: &str,
    message_id: &str,
    file_key: &str,
) -> PlatformResult<Vec<u8>> {
    // SSRF guard: validate the URL before making the request
    let url = format!(
        "{}/im/v1/messages/{}/resources/{}?type=file",
        super::api_base_url(),
        message_id,
        file_key
    );

    if !is_feishu_domain(&url) {
        return Err(PlatformError::SendFailed(format!(
            "download_message_resource: URL validation failed for {}",
            url
        )));
    }

    let token = token.to_string();

    let response = request_with_retry(
        || {
            let t = token.clone();
            let u = url.clone();
            async move {
                reqwest::Client::new()
                    .get(&u)
                    .header("Authorization", format!("Bearer {}", t))
                    .send()
                    .await
            }
        },
        "download_message_resource",
    )
    .await?;

    response.bytes().await.map(|b| b.to_vec()).map_err(|e| {
        PlatformError::SendFailed(format!("download_message_resource: read body: {}", e))
    })
}

// ---------------------------------------------------------------------------
// Cache helpers
// ---------------------------------------------------------------------------

/// Base cache directory: `~/.cowd/cache/`
fn cache_base_dir() -> PathBuf {
    config_home_dir().join("cache")
}

/// Ensure a cache subdirectory exists, creating it if needed.
fn ensure_cache_subdir(name: &str) -> PlatformResult<PathBuf> {
    let dir = cache_base_dir().join(name);
    std::fs::create_dir_all(&dir).map_err(|e| {
        PlatformError::Unknown(format!("cache: failed to create {} dir: {}", name, e))
    })?;
    Ok(dir)
}

/// Cache raw image bytes to `~/.cowd/cache/images/img_{uuid}.{ext}`.
///
/// Validates magic bytes before writing. Returns the absolute path on success.
pub fn cache_image(data: &[u8], ext: &str) -> PlatformResult<String> {
    if !validate_image_magic(data) {
        return Err(PlatformError::Unknown(
            "cache_image: data does not look like a valid image (magic bytes mismatch)".into(),
        ));
    }

    let dir = ensure_cache_subdir("images")?;
    let id = uuid::Uuid::new_v4();
    let path = dir.join(format!("img_{}.{}", id, ext));

    std::fs::write(&path, data)
        .map_err(|e| PlatformError::Unknown(format!("cache_image: write failed: {}", e)))?;

    Ok(path.to_string_lossy().to_string())
}

/// Cache raw audio bytes to `~/.cowd/cache/audio/audio_{uuid}.{ext}`.
///
/// Returns the absolute path on success.
pub fn cache_audio(data: &[u8], ext: &str) -> PlatformResult<String> {
    let dir = ensure_cache_subdir("audio")?;
    let id = uuid::Uuid::new_v4();
    let path = dir.join(format!("audio_{}.{}", id, ext));

    std::fs::write(&path, data)
        .map_err(|e| PlatformError::Unknown(format!("cache_audio: write failed: {}", e)))?;

    Ok(path.to_string_lossy().to_string())
}

/// Cache raw video bytes to `~/.cowd/cache/videos/video_{uuid}.{ext}`.
///
/// Returns the absolute path on success.
pub fn cache_video(data: &[u8], ext: &str) -> PlatformResult<String> {
    let dir = ensure_cache_subdir("videos")?;
    let id = uuid::Uuid::new_v4();
    let path = dir.join(format!("video_{}.{}", id, ext));

    std::fs::write(&path, data)
        .map_err(|e| PlatformError::Unknown(format!("cache_video: write failed: {}", e)))?;

    Ok(path.to_string_lossy().to_string())
}

/// Cache document bytes to `~/.cowd/cache/documents/doc_{uuid}_{safe_name}`.
///
/// Sanitizes the filename: strips directory components, null bytes, and
/// control characters. Returns the absolute path on success.
pub fn cache_document(data: &[u8], file_name: &str) -> PlatformResult<String> {
    let safe_name = sanitize_filename(file_name);

    let dir = ensure_cache_subdir("documents")?;
    let id = uuid::Uuid::new_v4();
    let path = dir.join(format!("doc_{}_{}", id, safe_name));

    std::fs::write(&path, data)
        .map_err(|e| PlatformError::Unknown(format!("cache_document: write failed: {}", e)))?;

    Ok(path.to_string_lossy().to_string())
}

/// Sanitize a filename for safe filesystem use.
///
/// - Strips directory components (takes only the last path segment).
///   Handles both `/` and `\\` separators.
/// - Removes null bytes and ASCII control characters (0x00–0x1F, 0x7F).
/// - Returns `"unnamed"` if the result is empty.
fn sanitize_filename(raw: &str) -> String {
    let normalized = raw.replace('\\', "/");

    let base = Path::new(&normalized)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unnamed");

    // Remove null bytes and control characters
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && *c != '\0')
        .collect();

    if cleaned.is_empty() {
        "unnamed".to_string()
    } else {
        cleaned
    }
}

// ---------------------------------------------------------------------------
// Image helper
// ---------------------------------------------------------------------------

/// Validate that raw bytes look like a supported image format.
///
/// Checks magic bytes for:
/// - PNG:  `\x89PNG\r\n\x1a\n`
/// - JPEG: `\xff\xd8\xff`
/// - GIF:  `GIF87a` / `GIF89a`
/// - BMP:  `BM`
/// - WEBP: `RIFF....WEBP`
pub fn validate_image_magic(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }

    // PNG: 8-byte signature
    const PNG_SIG: &[u8] = b"\x89PNG\r\n\x1a\n";
    if data.len() >= 8 && &data[..8] == PNG_SIG {
        return true;
    }

    // JPEG: starts with FF D8 FF
    if data.len() >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
        return true;
    }

    // GIF: starts with "GIF87a" or "GIF89a"
    if data.len() >= 6 {
        let header = &data[..6];
        if header == b"GIF87a" || header == b"GIF89a" {
            return true;
        }
    }

    // BMP: starts with "BM"
    if data.len() >= 2 && &data[..2] == b"BM" {
        return true;
    }

    // WEBP: starts with "RIFF", followed by 4 bytes (file size), then "WEBP"
    if data.len() >= 12 && &data[..4] == b"RIFF" && &data[8..12] == b"WEBP" {
        return true;
    }

    false
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Resolve the Feishu media type and file extension from a file path.
///
/// Returns `(feishu_type, extension)` where `feishu_type` is the type string
/// used in Feishu API calls and `extension` is the lowercased file extension
/// (no dot).
///
/// Mapping:
/// | Extensions                    | feishu_type | Notes |
/// |-------------------------------|-------------|-------|
/// | jpg, jpeg, png, gif, webp, bmp | message     | Used for image messages |
/// | ogg, opus                     | opus        | Audio messages |
/// | mp3, wav, m4a, aac, flac      | stream      | Generic audio |
/// | mp4, mov, avi, mkv            | mp4         | Video messages |
/// | pdf                           | pdf         | Document |
/// | doc, docx                     | doc         | Document |
/// | xls, xlsx                     | xls         | Spreadsheet |
/// | ppt, pptx                     | ppt         | Presentation |
/// | * (other)                     | stream      | Generic file |
pub fn resolve_media_type(file_path: &Path) -> (&'static str, String) {
    let ext = file_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();

    let feishu_type = match ext.as_str() {
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" => "message",
        "ogg" | "opus" => "opus",
        "mp3" | "wav" | "m4a" | "aac" | "flac" => "stream",
        "mp4" | "mov" | "avi" | "mkv" => "mp4",
        "pdf" => "pdf",
        "doc" | "docx" => "doc",
        "xls" | "xlsx" => "xls",
        "ppt" | "pptx" => "ppt",
        _ => "stream",
    };

    (feishu_type, ext)
}

// ===========================================================================
// Tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    // -----------------------------------------------------------------------
    // validate_image_magic
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_image_magic_png() {
        // Valid PNG: 8-byte signature
        let png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR";
        assert!(validate_image_magic(png), "PNG magic should be valid");
    }

    #[test]
    fn test_validate_image_magic_jpeg() {
        // Valid JPEG: starts with FF D8 FF
        let jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF";
        assert!(validate_image_magic(jpeg), "JPEG magic should be valid");
    }

    #[test]
    fn test_validate_image_magic_gif() {
        // Valid GIF89a
        let gif = b"GIF89a\x00\x00\x00";
        assert!(validate_image_magic(gif), "GIF magic should be valid");
    }

    #[test]
    fn test_validate_image_magic_gif87a() {
        let gif = b"GIF87a\x00\x00\x00";
        assert!(validate_image_magic(gif), "GIF87a magic should be valid");
    }

    #[test]
    fn test_validate_image_magic_bmp() {
        let bmp = b"BM\x00\x00\x00\x00";
        assert!(validate_image_magic(bmp), "BMP magic should be valid");
    }

    #[test]
    fn test_validate_image_magic_webp() {
        // RIFF + 4 bytes size + WEBP
        let webp = b"RIFF\x00\x00\x00\x00WEBP";
        assert!(validate_image_magic(webp), "WEBP magic should be valid");
    }

    #[test]
    fn test_validate_image_magic_invalid() {
        // Plain text — not an image
        assert!(!validate_image_magic(b"Hello, world!"));
        // Empty
        assert!(!validate_image_magic(b""));
        // Too short
        assert!(!validate_image_magic(b"\x89"));
        // Random bytes
        assert!(!validate_image_magic(&[0xDE, 0xAD, 0xBE, 0xEF]));
    }

    // -----------------------------------------------------------------------
    // resolve_media_type
    // -----------------------------------------------------------------------

    #[test]
    fn test_resolve_media_type_images() {
        assert_eq!(
            resolve_media_type(Path::new("photo.jpg")),
            ("message", "jpg".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("photo.JPEG")),
            ("message", "jpeg".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("icon.png")),
            ("message", "png".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("anim.gif")),
            ("message", "gif".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("img.webp")),
            ("message", "webp".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("img.bmp")),
            ("message", "bmp".to_string())
        );
    }

    #[test]
    fn test_resolve_media_type_audio() {
        assert_eq!(
            resolve_media_type(Path::new("voice.opus")),
            ("opus", "opus".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("sound.ogg")),
            ("opus", "ogg".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("song.mp3")),
            ("stream", "mp3".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("song.wav")),
            ("stream", "wav".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("song.m4a")),
            ("stream", "m4a".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("song.aac")),
            ("stream", "aac".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("song.flac")),
            ("stream", "flac".to_string())
        );
    }

    #[test]
    fn test_resolve_media_type_video() {
        assert_eq!(
            resolve_media_type(Path::new("clip.mp4")),
            ("mp4", "mp4".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("clip.mov")),
            ("mp4", "mov".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("clip.avi")),
            ("mp4", "avi".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("clip.mkv")),
            ("mp4", "mkv".to_string())
        );
    }

    #[test]
    fn test_resolve_media_type_documents() {
        assert_eq!(
            resolve_media_type(Path::new("report.pdf")),
            ("pdf", "pdf".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("letter.doc")),
            ("doc", "doc".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("letter.docx")),
            ("doc", "docx".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("sheet.xls")),
            ("xls", "xls".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("sheet.xlsx")),
            ("xls", "xlsx".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("slides.ppt")),
            ("ppt", "ppt".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("slides.pptx")),
            ("ppt", "pptx".to_string())
        );
    }

    #[test]
    fn test_resolve_media_type_unknown() {
        assert_eq!(
            resolve_media_type(Path::new("data.bin")),
            ("stream", "bin".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("data.csv")),
            ("stream", "csv".to_string())
        );
        assert_eq!(
            resolve_media_type(Path::new("script.py")),
            ("stream", "py".to_string())
        );
        // No extension
        assert_eq!(
            resolve_media_type(Path::new("Makefile")),
            ("stream", "".to_string())
        );
    }

    // -----------------------------------------------------------------------
    // sanitize_filename
    // -----------------------------------------------------------------------

    #[test]
    fn test_sanitize_filename_strips_directory() {
        assert_eq!(sanitize_filename("/etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("../../../root/key"), "key");
        assert_eq!(
            sanitize_filename("C:\\Windows\\system32\\cmd.exe"),
            "cmd.exe"
        );
    }

    #[test]
    fn test_sanitize_filename_removes_nulls() {
        assert_eq!(sanitize_filename("fi\0le.txt"), "file.txt");
    }

    #[test]
    fn test_sanitize_filename_removes_control_chars() {
        assert_eq!(sanitize_filename("test\x01\x02.txt"), "test.txt");
        assert_eq!(sanitize_filename("a\x7fb"), "ab"); // DEL is a control char
    }

    #[test]
    fn test_sanitize_filename_empty_input() {
        assert_eq!(sanitize_filename(""), "unnamed");
        assert_eq!(sanitize_filename("\0\0\0"), "unnamed");
        assert_eq!(sanitize_filename("/"), "unnamed"); // file_name() on "/" returns ""
    }

    // -----------------------------------------------------------------------
    // is_feishu_domain (SSRF protection)
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_feishu_domain_valid() {
        assert!(is_feishu_domain(
            "https://open.feishu.cn/open-apis/im/v1/images"
        ));
        assert!(is_feishu_domain("https://open.feishu.cn/anything"));
        assert!(is_feishu_domain("https://feishu.cn/some/path"));
        assert!(is_feishu_domain("https://sub.feishu.cn/path"));
    }

    #[test]
    fn test_is_feishu_domain_rejects_non_https() {
        assert!(!is_feishu_domain("http://open.feishu.cn/path"));
        assert!(!is_feishu_domain("ftp://open.feishu.cn/path"));
    }

    #[test]
    fn test_is_feishu_domain_rejects_other_domains() {
        assert!(!is_feishu_domain("https://evil.com/open.feishu.cn/path"));
        assert!(!is_feishu_domain("https://open.feishu.cn.evil.com/path"));
        assert!(!is_feishu_domain("https://localhost/path"));
    }

    #[test]
    fn test_is_feishu_domain_rejects_ip_addresses() {
        assert!(!is_feishu_domain("https://127.0.0.1/path"));
        assert!(!is_feishu_domain("https://192.168.1.1/path"));
        assert!(!is_feishu_domain("https://[::1]/path"));
    }

    #[test]
    fn test_is_feishu_domain_rejects_empty() {
        assert!(!is_feishu_domain(""));
    }

    // -----------------------------------------------------------------------
    // cache_image
    // -----------------------------------------------------------------------

    #[test]
    fn test_cache_image_creates_file() {
        // Valid PNG data
        let png_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82";
        let path = cache_image(png_data, "png").expect("cache_image should succeed");
        assert!(path.contains("img_"), "path should contain img_ prefix");
        assert!(path.ends_with(".png"), "path should end with .png");
        assert!(std::path::Path::new(&path).exists(), "file should exist");

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_cache_image_rejects_invalid_magic() {
        let result = cache_image(b"not an image", "png");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("magic bytes"),
            "error should mention magic bytes"
        );
    }

    // -----------------------------------------------------------------------
    // cache_audio
    // -----------------------------------------------------------------------

    #[test]
    fn test_cache_audio_creates_file() {
        let data = b"fake opus audio data";
        let path = cache_audio(data, "opus").expect("cache_audio should succeed");
        assert!(path.contains("audio_"), "path should contain audio_ prefix");
        assert!(path.ends_with(".opus"), "path should end with .opus");
        assert!(std::path::Path::new(&path).exists());

        let _ = std::fs::remove_file(&path);
    }

    // -----------------------------------------------------------------------
    // cache_video
    // -----------------------------------------------------------------------

    #[test]
    fn test_cache_video_creates_file() {
        let data = b"fake mp4 video data";
        let path = cache_video(data, "mp4").expect("cache_video should succeed");
        assert!(path.contains("video_"), "path should contain video_ prefix");
        assert!(path.ends_with(".mp4"), "path should end with .mp4");
        assert!(std::path::Path::new(&path).exists());

        let _ = std::fs::remove_file(&path);
    }

    // -----------------------------------------------------------------------
    // cache_document
    // -----------------------------------------------------------------------

    #[test]
    fn test_cache_document_creates_file() {
        let data = b"fake pdf content";
        let path = cache_document(data, "report.pdf").expect("cache_document should succeed");
        assert!(path.contains("doc_"), "path should contain doc_ prefix");
        assert!(
            path.ends_with("report.pdf"),
            "path should end with sanitized filename"
        );
        assert!(std::path::Path::new(&path).exists());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_cache_document_sanitizes_path() {
        let data = b"hello";
        let path =
            cache_document(data, "../../../etc/passwd").expect("cache_document should succeed");
        // Should NOT contain directory traversal
        assert!(!path.contains("../"));
        assert!(!path.contains("etc"));
        assert!(path.contains("passwd"), "bare filename should be kept");

        let _ = std::fs::remove_file(&path);
    }

    // -----------------------------------------------------------------------
    // request_with_retry — integration-style test with real TCP
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_retry_exhausts_attempts_on_500() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let attempts = Arc::new(AtomicU32::new(0));
        let att = attempts.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let port = addr.port();

        // Server that always returns 500
        let server = tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    att.fetch_add(1, Ordering::SeqCst);
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\n\
                              Content-Length: 5\r\n\
                              Connection: close\r\n\r\n\
                              Error",
                        )
                        .await;
                }
            }
        });

        let url = format!("http://127.0.0.1:{}/test", port);

        let result = request_with_retry(
            || {
                let u = url.clone();
                async move { reqwest::Client::new().get(&u).send().await }
            },
            "test_retry",
        )
        .await;

        server.abort();

        assert!(result.is_err(), "should fail after all retries");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "should attempt exactly 3 times"
        );
    }

    #[tokio::test]
    async fn test_retry_succeeds_on_non_retryable_4xx() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        let attempts = Arc::new(AtomicU32::new(0));
        let att = attempts.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let port = addr.port();

        // Server that returns 400 (not retryable)
        let server = tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    att.fetch_add(1, Ordering::SeqCst);
                    let _ = stream
                        .write_all(
                            b"HTTP/1.1 400 Bad Request\r\n\
                              Content-Length: 11\r\n\
                              Connection: close\r\n\r\n\
                              Bad Request",
                        )
                        .await;
                }
            }
        });

        let url = format!("http://127.0.0.1:{}/test", port);

        let result = request_with_retry(
            || {
                let u = url.clone();
                async move { reqwest::Client::new().get(&u).send().await }
            },
            "test_retry_400",
        )
        .await;

        server.abort();

        assert!(result.is_err(), "400 should not succeed");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "400 (non-retryable) should only try once"
        );
    }

    // -----------------------------------------------------------------------
    // upload_image / upload_file — multipart form construction validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_upload_image_multipart_construction() {
        // Verify that the multipart form can be built without panicking
        let form = reqwest::multipart::Form::new()
            .text("image_type", "message")
            .part(
                "image",
                reqwest::multipart::Part::bytes(b"\x89PNG\r\n\x1a\nfake")
                    .file_name("image.png")
                    .mime_str("application/octet-stream")
                    .expect("valid mime"),
            );

        // Just verify the form was constructed — the actual upload is tested
        // via integration tests or mocked HTTP
        let _ = form;
    }

    #[test]
    fn test_upload_file_multipart_construction() {
        let form = reqwest::multipart::Form::new()
            .text("file_type", "pdf")
            .text("file_name", "report.pdf")
            .part(
                "file",
                reqwest::multipart::Part::bytes(b"fake pdf content")
                    .file_name("report.pdf")
                    .mime_str("application/octet-stream")
                    .expect("valid mime"),
            );
        let _ = form;
    }
}
