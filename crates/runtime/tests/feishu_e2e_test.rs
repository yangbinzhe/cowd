//! 飞书端到端集成测试
//! 运行: cargo test --test feishu_e2e_test -- --ignored --nocapture

use runtime::platform::adapter::{OutboundMessage, PlatformAdapter};
use runtime::platform::feishu::{FeishuAdapter, FeishuConfig};

fn app_id() -> String {
    std::env::var("FEISHU_APP_ID").expect("FEISHU_APP_ID must be set")
}
fn app_secret() -> String {
    std::env::var("FEISHU_APP_SECRET").expect("FEISHU_APP_SECRET must be set")
}

#[tokio::test]
#[ignore = "需要飞书凭证"]
async fn test_feishu_e2e_send_receive() {
    let config = FeishuConfig::new(app_id(), app_secret());
    let mut adapter = FeishuAdapter::new(config);

    // 1. 连接
    adapter.connect().await.expect("connect failed");
    assert!(adapter.is_connected());

    // 2. 等待消息（60 秒超时）
    println!("等待飞书消息...");
    let mut received = false;
    for _ in 0..60 {
        if let Ok(Some(msg)) = adapter.receive().await {
            println!("收到消息: {:?}", msg);
            received = true;

            // 3. 回复
            let reply = OutboundMessage {
                session_key: msg.session_key.clone(),
                text: format!("收到！你说：{}", msg.text),
                reply_to: msg.message_id.clone(),
                metadata: serde_json::json!({}),
            };
            adapter.send(&reply).await.expect("send failed");
            println!("回复已发送");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    assert!(received, "未收到消息");

    // 4. 断开
    adapter.disconnect().await.expect("disconnect failed");
}
