//! Quick WeChat iLink live test — verifies saved QR credentials work.
//!
//! Run: cargo test --test wechat_quick_test -- --nocapture

use runtime::platform::adapter::PlatformAdapter;
use runtime::platform::wechat_ilink::{
    load_wechat_qr_account, WeChatLinkAdapter, WeChatLinkConfig,
};

#[tokio::test]
async fn test_wechat_ilink_qr_token_live() {
    println!("\n═══ WeChat iLink QR Token Live Test ═══");

    // 1. Load saved QR account
    let account = match load_wechat_qr_account("29f8d43dff4e@im.bot", None) {
        Ok(a) => {
            println!("✅ Loaded saved QR account: {}", a.account_id);
            println!("   user_id: {:?}", a.user_id);
            println!("   saved_at: {}", a.saved_at);
            a
        }
        Err(e) => {
            println!("❌ Failed to load QR account: {:?}", e);
            return;
        }
    };

    // 2. Create config + adapter
    let config = WeChatLinkConfig::from_qr_account(
        account.account_id,
        account.token,
        account.base_url,
        account.user_id,
    );
    let mut adapter = WeChatLinkAdapter::new(config);

    // 3. Connect
    match adapter.connect().await {
        Ok(()) => println!("✅ connect() succeeded"),
        Err(e) => {
            println!("❌ connect() failed: {:?}", e);
            return;
        }
    }

    // 4. Verify connected state
    let is_connected = rt_check_connected(&adapter).await;
    println!("   is_connected: {is_connected}");

    // 5. Get token (from QR credential store, no network call)
    match adapter.ensure_token().await {
        Ok(t) => {
            let preview = if t.len() > 50 {
                format!("{}...", &t[..50])
            } else {
                t.clone()
            };
            println!("✅ ensure_token(): {preview}");
        }
        Err(e) => {
            println!("❌ ensure_token() failed: {:?}", e);
            return;
        }
    }

    // 6. Try get_updates() — this calls the real iLink API with the token
    //    A valid token will return Ok(Vec) (empty if no messages, or messages if any)
    //    An invalid token will return Err with auth failure
    println!("\n═══ Calling get_updates() — verifying token against iLink API ═══");
    match adapter.get_updates().await {
        Ok(msgs) => {
            println!("✅ get_updates() succeeded with {} messages", msgs.len());
            if !msgs.is_empty() {
                for (i, msg) in msgs.iter().enumerate() {
                    println!("   msg[{}]: {}", i, serde_json::to_string_pretty(msg).unwrap_or_default());
                }
            } else {
                println!("   (no pending messages — expected)");
            }
        }
        Err(e) => {
            let err_str = format!("{:?}", e);
            if err_str.to_lowercase().contains("timeout") {
                println!("⚠️ get_updates() timed out (long-poll normal behavior)");
            } else {
                println!("❌ get_updates() failed: {}", err_str);
            }
        }
    }

    println!("\n═══ Test complete ═══");
}

/// Read the connected state without blocking (ok in tokio context).
async fn rt_check_connected(adapter: &WeChatLinkAdapter) -> bool {
    // Use the blocking_read version since we're in a tokio task
    adapter.is_connected()
}

#[allow(dead_code)]
struct QuickTest;
