//! Debug test: connect to Feishu WS and dump raw frame bytes.
//! Run: cargo test --test feishu_ws_debug test_ws_raw_dump -- --ignored --nocapture

use tokio_tungstenite::tungstenite::Message;
use futures::StreamExt;
use reqwest;

#[tokio::test]
#[ignore]
async fn test_ws_raw_dump() {
    let app_id = std::env::var("FEISHU_APP_ID").expect("FEISHU_APP_ID must be set");
    let app_secret = std::env::var("FEISHU_APP_SECRET").expect("FEISHU_APP_SECRET must be set");

    // 1. Get WS URL from Feishu (correct endpoint)
    println!("\n=== Step 1: Get WebSocket URL ===");
    let client = reqwest::Client::new();
    let resp = client
        .post("https://open.feishu.cn/callback/ws/endpoint")
        .header("locale", "zh")
        .json(&serde_json::json!({"AppID": app_id, "AppSecret": app_secret}))
        .send()
        .await
        .expect("HTTP POST failed");

    let body: serde_json::Value = resp.json().await.expect("JSON parse failed");
    let ws_url = body["data"]["URL"].as_str().expect("No URL in response");
    println!("WS URL: {}", ws_url);

    // 2. Connect via websocket
    println!("\n=== Step 2: Connect WebSocket ===");
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(ws_url)
        .await
        .expect("WS connect failed");
    println!("✅ WebSocket connected!");

    // 3. Read frames and dump raw bytes — respond to Ping with Pong
    println!("\n=== Step 3: Reading frames (waiting for events)... ===");
    println!("  📱 Send a message to ClawAI in Feishu NOW!");
    
    let now = std::time::Instant::now();
    let deadline = now + std::time::Duration::from_secs(120);
    
    for i in 1..=30 {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            println!("\n⏰ 2 minute deadline reached — no event frames received");
            break;
        }
        
        match tokio::time::timeout(remaining, ws_stream.next()).await {
            Ok(Some(Ok(msg))) => {
                match &msg {
                    Message::Text(t) => println!("\n📝 Frame #{i}: Text({} chars): {}", t.len(), &t[..200.min(t.len())]),
                    Message::Binary(data) => {
                        println!("\n📦 Frame #{i}: Binary({} bytes)", data.len());
                        if let Ok(text) = String::from_utf8(data.to_vec()) {
                            println!("   UTF-8: {}", &text[..300.min(text.len())]);
                        }
                        println!("   Hex first 256: {:02x?}", &data[..256.min(data.len())]);
                    }
                    Message::Ping(data) => {
                        println!("🏓 Ping ← Pong");
                        use futures::SinkExt;
                        ws_stream.send(Message::Pong(data.to_vec())).await.ok();
                    }
                    Message::Pong(_) => (),
                    Message::Close(f) => {
                        println!("\n🔴 Frame #{i}: Close({:?}) — disconnecting", f);
                        break;
                    }
                    msg => println!("\n❓ Frame #{i}: {msg:?}"),
                }
            }
            Ok(Some(Err(e))) => println!("\n❌ Frame #{i}: Error {:?}", e),
            Ok(None) => { println!("\n📭 Stream ended"); break; }
            Err(_) => { println!("\n⏰ Timeout — no event received"); break; }
        }
    }

    println!("\n=== Done ===");
    ws_stream.close(None).await.ok();
}
