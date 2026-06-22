use std::io;
use reqwest::Client;
use tokio_tungstenite::{connect_async};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;


const DEVNET_HTTP_URL: &str = "https://api.devnet.solana.com";
const DEVNET_WS_URL: &str = "wss://api.devnet.solana.com";

#[derive(Debug)]
struct TransactionDetails{
    slot: u64,
    block_time: Option<i64>,
    fee: u64,
    success: bool,
    wallet_balance_change_lamports: i64,
}

#[tokio::main]
async fn main() {
    let client = reqwest::Client::new();
    let mut wallet_address = String::new();
    println!("Enter the Wallet Address:");
    io::stdin().read_line(&mut wallet_address).expect("Failed to read wallet address");
    let wallet_address = wallet_address.trim();

    let (mut ws_stream, _) = connect_async(DEVNET_WS_URL).await.expect("Failed to connect");

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
                    println!("Lamports: {}", lamports);
                    
                    match fetch_latest_transaction(&client, wallet_address).await{
                        Ok(Some(signature)) => {
                            println!("Latest transaction: {}\n", signature);

                            match fetch_transaction_details(&client, &signature, wallet_address).await{
                                Ok(Some(tnx_details)) =>{
                                    println!("\nTransaction details");
                                    println!("Success: {}", if tnx_details.success { "Success" } else { "Failed" });
                                    println!("Slot: {}",tnx_details.slot);
                                    println!("Fee: {} SOL",tnx_details.fee as f64 / 1_000_000_000.0);
                                    println!("Block-Time: {:?}",tnx_details.block_time);
                                    println!("Wallet SOL change: {:.9} SOL",tnx_details.wallet_balance_change_lamports as f64 / 1_000_000_000.0);

                                }
                                Ok(None)=>{
                                    println!("Transaction details are not available.");
                                }
                                Err(error)=>{
                                    eprintln!("Failed to fetch transaction details: {}",error);
                                }
                            }
                        }
                        Ok(None) => {
                            println!("No Latest Transaction found\n");
                        }
                        Err(error) =>{
                            println!("Failed to fetch latest transaction: {}\n",error);
                        }
                    }
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


async fn fetch_latest_transaction(client: &Client, wallet_address: &str) -> Result<Option<String>,reqwest::Error> {
    let request_body = json!(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                wallet_address,
            {
                "commitment": "finalized",
                "limit": 1
            }
            ]
        }
    );

    let response : serde_json::Value = client
        .post(DEVNET_HTTP_URL)
        .json(&request_body)
        .send()
        .await?
        .json()
        .await?;

    let signature = response["result"][0]["signature"]
        .as_str()
        .map(|signature| signature.to_string());


    Ok(signature)
}


async fn fetch_transaction_details(client: &Client, signature: &str, wallet_address: &str) -> Result<Option<TransactionDetails>,reqwest::Error>{
    let request_body = json!(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTransaction",
            "params": [
                signature,
            {
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
                "encoding": "json"
            }
            ]
        }
    );

    let response: serde_json::Value = client
        .post(DEVNET_HTTP_URL)
        .json(&request_body)
        .send()
        .await?
        .json()
        .await?;

    let result = &response["result"];

    if result.is_null() {
        return Ok(None);
    }

    let slot = result["slot"]
        .as_u64()
        .expect("missing slot");
    let block_time = result["blockTime"]
        .as_i64();

    let fee = result["meta"]["fee"]
        .as_u64()
        .expect("missing fee");
    let success = result["meta"]["err"].is_null();

    let account_keys = result["transaction"]["message"]["accountKeys"]
        .as_array()
        .expect("missing account keys");

    let wallet_index = account_keys.iter().position(|key| key.as_str()== Some(wallet_address));

    let wallet_balance_change_lamports = match wallet_index{
        Some(index) =>{
            let pre_balance = result["meta"]["preBalances"][index]
                .as_u64()
                .expect("missing pre blance");
            let post_balance = result["meta"]["postBalances"][index]
                .as_u64()
                .expect("missing post balance");

            post_balance as i64 - pre_balance as i64

        }
        None => 0
    };

    Ok(Some(TransactionDetails{
        slot,
        block_time,
        fee,
        success,
        wallet_balance_change_lamports
    }))
}