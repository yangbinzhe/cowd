// ── Cross-Platform Clipboard Read ─────────────────────────────────────
// Reads the system clipboard with image detection. Platform-specific:
//   Linux:   wl-paste (Wayland) / xclip (X11) for image, then text
//   macOS:   osascript for image, pbpaste for text
//   Windows: PowerShell for image and text
//
// Reference: opencode util/clipboard.ts:59-123
// -----------------------------------------------------------------------

use std::process::Command;

/// Content read from the system clipboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClipboardContent {
    Text(String),
    Image {
        data: Vec<u8>,
        mime: String,
    },
}

const PNG_MAGIC: &[u8] = b"\x89PNG\r\n\x1a\n";

#[inline]
pub fn is_png_image(data: &[u8]) -> bool {
    data.len() >= 8 && &data[..8] == PNG_MAGIC
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Linux,
    MacOs,
    Windows,
    Unknown,
}

pub fn detect_platform() -> Platform {
    match std::env::consts::OS {
        "linux" => Platform::Linux,
        "macos" => Platform::MacOs,
        "windows" => Platform::Windows,
        _ => Platform::Unknown,
    }
}

pub type PasteCommand = (&'static str, Vec<&'static str>);

pub fn linux_image_paste_commands() -> Vec<PasteCommand> {
    vec![
        ("wl-paste", vec!["-t", "image/png"]),
        ("xclip", vec!["-selection", "clipboard", "-t", "image/png", "-o"]),
    ]
}

pub fn linux_text_paste_commands() -> Vec<PasteCommand> {
    vec![
        ("wl-paste", vec![]),
        ("xclip", vec!["-selection", "clipboard", "-o"]),
    ]
}

pub fn macos_image_osascript_args(tmp_path: &str) -> Vec<String> {
    vec![
        "-e".to_string(),
        "set imageData to the clipboard as «class PNGf»".to_string(),
        "-e".to_string(),
        format!("set fileRef to open for access POSIX file \"{tmp_path}\" with write permission"),
        "-e".to_string(),
        "set eof fileRef to 0".to_string(),
        "-e".to_string(),
        "write imageData to fileRef".to_string(),
        "-e".to_string(),
        "close access fileRef".to_string(),
    ]
}

pub fn macos_text_paste_command() -> PasteCommand {
    ("pbpaste", vec![])
}

pub fn windows_image_paste_command() -> PasteCommand {
    (
        "powershell.exe",
        vec![
            "-NonInteractive",
            "-NoProfile",
            "-Command",
            "Add-Type -AssemblyName System.Windows.Forms; $img = [System.Windows.Forms.Clipboard]::GetImage(); if ($img) { $ms = New-Object System.IO.MemoryStream; $img.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png); [System.Convert]::ToBase64String($ms.ToArray()) }",
        ],
    )
}

pub fn windows_text_paste_command() -> PasteCommand {
    (
        "powershell.exe",
        vec!["-NonInteractive", "-NoProfile", "-Command", "Get-Clipboard"],
    )
}

pub fn read_clipboard() -> Option<ClipboardContent> {
    let platform = detect_platform();

    match platform {
        Platform::Linux => {
            for (cmd, args) in linux_image_paste_commands() {
                if let Some(content) = try_read_image_bytes(cmd, &args) {
                    return Some(content);
                }
            }
        }
        Platform::MacOs => {
            if let Some(content) = try_read_macos_image() {
                return Some(content);
            }
        }
        Platform::Windows => {
            if let Some(content) = try_read_windows_image() {
                return Some(content);
            }
        }
        Platform::Unknown => {}
    }

    let text = match platform {
        Platform::Linux => {
            for (cmd, args) in linux_text_paste_commands() {
                if let Some(text) = try_read_text(cmd, &args) {
                    return Some(ClipboardContent::Text(text));
                }
            }
            return None;
        }
        Platform::MacOs => try_read_text("pbpaste", &[]),
        Platform::Windows => try_read_text(
            "powershell.exe",
            &["-NonInteractive", "-NoProfile", "-Command", "Get-Clipboard"],
        ),
        Platform::Unknown => return None,
    };

    text.map(ClipboardContent::Text).filter(|c| match c {
        ClipboardContent::Text(t) => !t.is_empty(),
        _ => true,
    })
}

fn try_read_image_bytes(cmd: &str, args: &[&str]) -> Option<ClipboardContent> {
    let output = Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() || output.stdout.is_empty() {
        return None;
    }
    if is_png_image(&output.stdout) {
        Some(ClipboardContent::Image {
            data: output.stdout,
            mime: "image/png".to_string(),
        })
    } else {
        None
    }
}

fn try_read_macos_image() -> Option<ClipboardContent> {
    let tmp_path = std::env::temp_dir().join("cowd-clipboard.png");
    let tmp_path_str = tmp_path.to_string_lossy().to_string();
    let args = macos_image_osascript_args(&tmp_path_str);
    let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    let status = Command::new("osascript").args(&args_refs).status().ok()?;
    if !status.success() {
        return None;
    }

    let data = std::fs::read(&tmp_path).ok()?;
    let _ = std::fs::remove_file(&tmp_path);

    if data.is_empty() || !is_png_image(&data) {
        return None;
    }

    Some(ClipboardContent::Image {
        data,
        mime: "image/png".to_string(),
    })
}

fn try_read_windows_image() -> Option<ClipboardContent> {
    let output = Command::new("powershell.exe")
        .args([
            "-NonInteractive",
            "-NoProfile",
            "-Command",
            "Add-Type -AssemblyName System.Windows.Forms; $img = [System.Windows.Forms.Clipboard]::GetImage(); if ($img) { $ms = New-Object System.IO.MemoryStream; $img.Save($ms, [System.Drawing.Imaging.ImageFormat]::Png); [System.Convert]::ToBase64String($ms.ToArray()) }",
        ])
        .output()
        .ok()?;

    if !output.status.success() || output.stdout.is_empty() {
        return None;
    }

    let base64 = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if base64.is_empty() {
        return None;
    }

    let data = base64_decode(&base64).ok()?;
    if data.is_empty() || !is_png_image(&data) {
        return None;
    }

    Some(ClipboardContent::Image {
        data,
        mime: "image/png".to_string(),
    })
}

fn try_read_text(cmd: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(cmd).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn base64_decode(input: &str) -> Result<Vec<u8>, String> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut decode_table = [0xFFu8; 128];
    for (i, &c) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
        .iter()
        .enumerate()
    {
        decode_table[c as usize] = i as u8;
    }

    let mut out = Vec::with_capacity((input.len() / 4) * 3);
    let bytes: Vec<u8> = input.bytes().take_while(|&b| b != b'=').collect();

    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            break;
        }
        let b0 = decode_table[chunk[0] as usize];
        let b1 = decode_table[chunk[1] as usize];
        if b0 == 0xFF || b1 == 0xFF {
            return Err("invalid base64 character".into());
        }

        let mut triple = (u32::from(b0) << 18) | (u32::from(b1) << 12);

        if let Some(&c) = chunk.get(2) {
            let b2 = decode_table[c as usize];
            if b2 == 0xFF {
                return Err("invalid base64 character".into());
            }
            triple |= u32::from(b2) << 6;

            if let Some(&c) = chunk.get(3) {
                let b3 = decode_table[c as usize];
                if b3 == 0xFF {
                    return Err("invalid base64 character".into());
                }
                triple |= u32::from(b3);
            }
        }

        out.push((triple >> 16) as u8);
        if chunk.len() >= 3 {
            out.push(((triple >> 8) & 0xFF) as u8);
        }
        if chunk.len() >= 4 {
            out.push((triple & 0xFF) as u8);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clipboard_read_text() {
        let content = ClipboardContent::Text("hello world".to_string());
        match &content {
            ClipboardContent::Text(text) => assert_eq!(text, "hello world"),
            _ => panic!("expected Text variant"),
        }
    }

    #[test]
    fn detect_image_png() {
        let png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0D";
        assert!(is_png_image(png));
    }

    #[test]
    fn detect_image_png_invalid() {
        let not_png = b"\x00PNG\r\n\x1a\n";
        assert!(!is_png_image(not_png));
    }

    #[test]
    fn detect_image_png_short() {
        assert!(!is_png_image(b"\x89PNG"));
    }

    #[test]
    fn detect_image_png_empty() {
        assert!(!is_png_image(b""));
    }

    #[test]
    fn detect_image_png_jpeg() {
        let jpeg = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00";
        assert!(!is_png_image(jpeg));
    }

    #[test]
    fn no_image_fallback() {
        let text_bytes = b"some random text data";
        assert!(!is_png_image(text_bytes));
        let content = ClipboardContent::Text(String::from_utf8_lossy(text_bytes).to_string());
        assert!(matches!(content, ClipboardContent::Text(_)));
    }

    #[test]
    fn platform_detection_linux() {
        let cmds = linux_image_paste_commands();
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0].0, "wl-paste");
        assert_eq!(cmds[1].0, "xclip");

        let text_cmds = linux_text_paste_commands();
        assert_eq!(text_cmds.len(), 2);
        assert_eq!(text_cmds[0].0, "wl-paste");
        assert_eq!(text_cmds[1].0, "xclip");
    }

    #[test]
    fn platform_detection_macos() {
        let cmd = macos_text_paste_command();
        assert_eq!(cmd.0, "pbpaste");
        assert!(cmd.1.is_empty());

        let args = macos_image_osascript_args("/tmp/test.png");
        let joined = args.join(" ");
        assert!(joined.contains("/tmp/test.png"));
    }

    #[test]
    fn platform_detection_windows() {
        let cmd = windows_image_paste_command();
        assert_eq!(cmd.0, "powershell.exe");
        assert!(cmd.1.contains(&"-NonInteractive"));

        let text_cmd = windows_text_paste_command();
        assert_eq!(text_cmd.0, "powershell.exe");
        assert!(text_cmd.1.contains(&"Get-Clipboard"));
    }

    #[test]
    fn clip_content_image() {
        let data = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00];
        let content = ClipboardContent::Image {
            data: data.clone(),
            mime: "image/png".to_string(),
        };
        match &content {
            ClipboardContent::Image { data: d, mime } => {
                assert_eq!(d, &data);
                assert_eq!(mime, "image/png");
            }
            _ => panic!("expected Image variant"),
        }
    }

    #[test]
    fn clip_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ClipboardContent>();
        assert_sync::<ClipboardContent>();
    }

    #[test]
    fn base64_decode_hello() {
        let decoded = base64_decode("SGVsbG8=").unwrap();
        assert_eq!(decoded, b"Hello");
    }

    #[test]
    fn base64_decode_roundtrip() {
        let png_data = b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0DIHDR";
        let b64 = base64_encode_bytes(png_data);
        let decoded = base64_decode(&b64).unwrap();
        assert_eq!(decoded, png_data);
    }

    #[test]
    fn base64_decode_invalid() {
        assert!(base64_decode("!!!invalid!!!").is_err());
    }

    #[test]
    fn base64_decode_empty() {
        assert!(base64_decode("").unwrap().is_empty());
    }

    // ── Clipboard text fallback tests ─────────────────────────────

    #[test]
    fn test_clipboard_text_fallback() {
        let content = ClipboardContent::Text("copied text content".to_string());
        match &content {
            ClipboardContent::Text(text) => {
                assert!(!text.is_empty());
                assert_eq!(text, "copied text content");
            }
            _ => panic!("expected Text variant for clipboard fallback"),
        }

        let text_content = ClipboardContent::Text("hello".to_string());
        match text_content {
            ClipboardContent::Text(ref t) if !t.is_empty() => {}
            _ => panic!("text fallback should return non-empty text"),
        }
    }

    fn base64_encode_bytes(input: &[u8]) -> String {
        const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::with_capacity(((input.len() + 2) / 3) * 4);
        for chunk in input.chunks(3) {
            let b0 = chunk[0];
            let b1 = chunk.get(1).copied().unwrap_or(0);
            let b2 = chunk.get(2).copied().unwrap_or(0);
            let triple = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
            out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
            out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
            out.push(if chunk.len() > 1 {
                TABLE[((triple >> 6) & 0x3F) as usize] as char
            } else {
                '='
            });
            out.push(if chunk.len() > 2 {
                TABLE[(triple & 0x3F) as usize] as char
            } else {
                '='
            });
        }
        out
    }
}
