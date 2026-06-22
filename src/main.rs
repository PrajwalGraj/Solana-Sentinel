use std::io;
use tokio_tungstenite::connect_async;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;


#[tokio::main]
async fn main() {
    let mut wallet_address = String::new();
    println!("Enter the Wallet Address:");
    io::stdin().read_line(&mut wallet_address).expect("Failed to read wallet address");
    let wallet_address = wallet_address.trim();

    let devnet_url = "wss://api.devnet.solana.com";
    let (mut ws_stream, _) = connect_async(devnet_url).await.expect("Failed to connect");

    println!("Connected!");

    let subscription_request = json!(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                wallet_address,
            {
                "encoding": "base64",
                "commitment": "confirmed"
            }
            ]
        }
    );

    ws_stream
        .send(Message::Text(subscription_request.to_string().into()))
        .await
        .expect("failed to send message");

    println!("Watching wallet: {}", wallet_address);
    println!("Waiting for account changes...\n");

    while let Some(message) = ws_stream.next().await{
        match message{
            Ok(Message::Text(text)) => {
                let parsed: serde_json::Value = serde_json::from_str(&text).expect("Failed to parse");

                if parsed["method"] == "accountNotification"{
                    let slot = parsed["params"]["result"]["context"]["slot"]
                        .as_u64()
                        .unwrap_or(0);

                    let lamports = parsed["params"]["result"]["value"]["lamports"]
                        .as_u64()
                        .unwrap_or(0);
                    let sol = lamports as f64/1_000_000_000.0;

                    println!("\nNew account change detected!");
                    println!("Wallet: {}", wallet_address);
                    println!("Slot: {}", slot);
                    println!("Balance: {:.9} SOL", sol);
                    println!("Lamports: {}\n", lamports);
                    println!("-----------------------------------");
                }else{
                    println!("Server Message: {}",parsed);
                }
            }
            Ok(Message::Ping(_)) => {
                println!("Received ping from server");
            }

            Ok(Message::Close(_)) => {
                println!("WebSocket connection closed");
                break;
            }

            Ok(_) => {}

            Err(error) => {
                println!("WebSocket error: {}", error);
                break;
            }
        }
    }
}
