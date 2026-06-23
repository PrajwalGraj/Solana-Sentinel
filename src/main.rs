use std::io;
use reqwest::Client;
use tokio_tungstenite::{connect_async};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use tokio::{sync::broadcast::error, time::{Duration, sleep}};
use std::fs;
use tokio::sync::mpsc;
use std::sync::Arc;
use sqlx::FromRow;
use std::env;

use sqlx::postgres::PgPoolOptions;

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

#[derive(Debug)]
struct WalletEvent {
    wallet_address: String,
    slot: u64,
    lamports: u64,
}

#[derive(Debug, FromRow)]
struct StoredTransaction {
    signature: String,
    transaction_slot: i64,
    success: bool,
    fee_lamports: i64,
    wallet_balance_change_lamports: i64,
    block_time: Option<i64>,
}

async fn watch_wallet(wallet_address: String, event_sender: mpsc::Sender<WalletEvent>){
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
                        .expect("Account notification missing context.slot");

                    let lamports = parsed["params"]["result"]["value"]["lamports"]
                        .as_u64()
                        .expect("Account notification missing value.lamports");

                    let event = WalletEvent{
                        wallet_address: wallet_address.clone(),
                        slot,
                        lamports
                    };

                    if let Err(err) = event_sender.send(event).await{
                        eprintln!("Event processor stopped for {wallet_address}: {err}");
                        break;
                    }
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

#[tokio::main]
async fn main(){

    dotenvy::dotenv().ok();

    let database_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to PostgreSQL");

    println!("Connected to PostgreSQL!");

    let args: Vec<String> = env::args().collect();

    if args.len() >= 3 && args[1] == "history" {
        let wallet_address = &args[2];

        if let Err(error) = show_history(&db_pool, wallet_address).await {
            eprintln!("Failed to fetch history: {error}");
        }
        return;
    }

    let client = reqwest::Client::new();

    let wallets_file = fs::read_to_string("wallets.txt")
        .expect("Failed to read wallets.txt");

    let wallets: Vec<String> = wallets_file
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();

    if wallets.is_empty() {
        eprintln!("No wallet addresses found in wallets.txt");
        return;
    }

    println!("Loaded {} wallet(s)", wallets.len());

    for wallet in &wallets {
        println!("- {}", wallet);
    }

    let (event_sender, mut event_receiver) = mpsc::channel::<WalletEvent>(100);

    for wallet_address in wallets{
        let sender_for_task = event_sender.clone();

        tokio::spawn(async move{
            watch_wallet(wallet_address, sender_for_task).await;
        });
    }
    drop(event_sender);


    while let Some(event) = event_receiver.recv().await {

        match sqlx::query(
            r#"
            INSERT INTO wallet_events (wallet_address, account_slot, lamports)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(&event.wallet_address)
        .bind(event.slot as i64)
        .bind(event.lamports as i64)
        .execute(&db_pool)
        .await
        {
            Ok(_) => println!("Event saved to PostgreSQL."),
            Err(error) => eprintln!("Failed to save event to PostgreSQL: {error}"),
        }

        let balance_sol = event.lamports as f64 / 1_000_000_000.0;

        println!("\nNew account change detected!");
        println!("Wallet: {}", event.wallet_address);
        println!("Slot: {}", event.slot);
        println!("Balance: {:.9} SOL", balance_sol);
        println!("Lamports: {}", event.lamports);

        match fetch_latest_transaction(&client, &event.wallet_address).await {
            Ok(Some(signature)) => {
                println!("Latest transaction: {}", signature);

                match fetch_transaction_with_retry(&client, &signature, &event.wallet_address).await {
                    Ok(Some(details)) => {

                        match save_transaction( &db_pool, &event.wallet_address, &signature, &details ).await {
                            Ok(_) => println!("Transaction saved to PostgreSQL."),
                            Err(error) => eprintln!("Failed to save transaction: {error}"),
                        }

                        let balance_change_sol = details.wallet_balance_change_lamports as f64 / 1_000_000_000.0;
                        println!("\nTransaction details");
                        println!("Status: {}",if details.success { "Success" } else { "Failed" } );
                        println!("Transaction slot: {}", details.slot);

                        match details.block_time {
                            Some(time) => println!("Block time (Unix): {}", time),
                            None => println!("Block time: unavailable"),
                        }
                        println!("Fee: {:.9} SOL", details.fee as f64 / 1_000_000_000.0 );
                        println!("Wallet SOL change: {:.9} SOL", balance_change_sol);
                    }

                    Ok(None) => {
                        println!("Transaction details are not available after retries.");
                    }

                    Err(error) => {
                        eprintln!("Failed to fetch transaction details: {}", error);
                    }
                }
            }

            Ok(None) => {
                println!("No recent transaction found.");
            }

            Err(error) => {
                eprintln!("Failed to fetch latest transaction: {}", error);
            }
        }

        println!("-----------------------------------");
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

async fn fetch_transaction_with_retry(client: &Client, signature: &str, wallet_address: &str)-> Result<Option<TransactionDetails>, reqwest::Error>{
    let max_try = 3;

    for i in 1..=max_try{
        match fetch_transaction_details(client, signature, wallet_address).await{
            Ok(Some(details))=>{
                return Ok(Some(details));
            }
            Ok(None) =>{
                println!("Transaction details not ready yet. Retry {}/{}...",i,max_try);
                sleep(Duration::from_millis(500)).await;
            }
            Err(error)=>{
                return Err(error);
            }
        }
    }

    Ok(None)
}

async fn save_transaction( db_pool: &sqlx::PgPool, wallet_address: &str, signature: &str,details: &TransactionDetails ) -> Result<(), sqlx::Error> {

    sqlx::query(
        r#"
        INSERT INTO transactions (
            wallet_address,
            signature,
            transaction_slot,
            success,
            fee_lamports,
            wallet_balance_change_lamports,
            block_time
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (wallet_address, signature) DO NOTHING
        "#,
    )
    .bind(wallet_address)
    .bind(signature)
    .bind(details.slot as i64)
    .bind(details.success)
    .bind(details.fee as i64)
    .bind(details.wallet_balance_change_lamports)
    .bind(details.block_time)
    .execute(db_pool)
    .await?;

    Ok(())
}


async fn show_history( db_pool: &sqlx::PgPool, wallet_address: &str ) -> Result<(), sqlx::Error> {
    let transactions = sqlx::query_as::<_, StoredTransaction>(
        r#"
        SELECT
            signature,
            transaction_slot,
            success,
            fee_lamports,
            wallet_balance_change_lamports,
            block_time
        FROM transactions
        WHERE wallet_address = $1
        ORDER BY transaction_slot DESC
        LIMIT 10
        "#,
    )
    .bind(wallet_address)
    .fetch_all(db_pool)
    .await?;

    if transactions.is_empty() {
        println!("No stored transactions found for {wallet_address}");
        return Ok(());
    }

    println!("\nRecent transactions for {}\n",wallet_address);

    for transaction in transactions {
        let sol_change = transaction.wallet_balance_change_lamports as f64 / 1_000_000_000.0;
        let fee_sol = transaction.fee_lamports as f64 / 1_000_000_000.0;

        println!("Signature: {}", transaction.signature);
        println!("Status: {}",if transaction.success { "Success" } else { "Failed" });
        println!("Transaction slot: {}", transaction.transaction_slot);
        println!("SOL change: {:.9} SOL", sol_change);
        println!("Fee: {:.9} SOL", fee_sol);

        match transaction.block_time {
            Some(time) => println!("Block time (Unix): {time}"),
            None => println!("Block time: unavailable"),
        }

        println!("-----------------------------------");
    }

    Ok(())
}