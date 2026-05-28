//! Mini Feishu gateway — listens for messages and responds.
//! Run: cargo run --example feishu_gateway
//! Or: cargo test --test feishu_gateway test_gateway_loop -- --ignored --nocapture

use runtime::platform::feishu::FeishuAdapter;
use runtime::platform::feishu::FeishuConfig;
use runtime::platform::adapter::PlatformAdapter;
use tokio::time::{timeout, Duration};

fn app_id() -> String {
    std::env::var("FEISHU_APP_ID").expect("FEISHU_APP_ID must be set")
}
fn app_secret() -> String {
    std::env::var("FEISHU_APP_SECRET").expect("FEISHU_APP_SECRET must be set")
}

#[tokio::test]
#[ignore = "runs indefinitely"]
async fn test_gateway_loop() {
    let config = FeishuConfig::new(app_id(), app_secret());
    let mut adapter = FeishuAdapter::new(config);

    println!("\n╔══════════════════════════════════╗");
    println!("║   COWD Feishu Gateway Starting   ║");
    println!("╚══════════════════════════════════╝\n");

    // Connect + start WebSocket event push
    match adapter.connect().await {
        Ok(()) => println!("✅ Connected to Feishu (WebSocket events active)"),
        Err(e) => {
            println!("❌ Connect failed: {:?}", e);
            return;
        }
    }

    println!("✅ is_connected: {}", adapter.is_connected());
    println!("\n🚀 Gateway running. Send a message to ClawAI in Feishu...\n");
    println!("   Press Ctrl+C to stop\n");

    let mut msg_count: u64 = 0;
    let start = std::time::Instant::now();

    loop {
        match timeout(Duration::from_secs(2), adapter.receive()).await {
            Ok(Ok(Some(msg))) => {
                msg_count += 1;
                let elapsed = start.elapsed().as_secs();
                
                println!("┌─────────────────────────────────────┐");
                println!("│ 📩 Message #{msg_count} @ {elapsed}s");
                println!("│ From:  {}", msg.sender_name.as_deref().unwrap_or("unknown"));
                println!("│ Chat:  {}", msg.session_key);
                println!("│ Text:  {}", msg.text);
                println!("│ Type:  {:?}", msg.message_type);
                println!("└─────────────────────────────────────┘");

                // Build reply
                let reply_text = format!("🤖 Cowd(Rust) 收到！你说：{}", msg.text);
                
                let reply = runtime::platform::adapter::OutboundMessage {
                    session_key: msg.session_key.clone(),
                    text: reply_text,
                    reply_to: msg.message_id.clone(),
                    metadata: serde_json::json!({}),
                };

                match adapter.send(&reply).await {
                    Ok(()) => println!("✅ Reply sent successfully\n"),
                    Err(e) => println!("❌ Reply failed: {:?}\n", e),
                }
            }
            Ok(Ok(None)) => {
                // No message — just waiting
                let elapsed = start.elapsed().as_secs();
                if elapsed % 30 == 0 && elapsed > 0 {
                    println!("⏳ Waiting for messages... ({elapsed}s elapsed, {msg_count} received)");
                }
            }
            Ok(Err(e)) => {
                println!("⚠️  Receive error: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(_timeout) => {
                // Just a polling timeout — normal, continue
            }
        }
    }
}
