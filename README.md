# Solana Sentinel

A Rust-based, real-time Solana wallet monitoring service.

Solana Sentinel watches multiple Solana wallets using WebSocket subscriptions, fetches transaction details through Solana RPC, and stores wallet events and transactions in PostgreSQL.

## Features

* Monitor multiple Solana Devnet wallets concurrently
* Real-time wallet account-change notifications using Solana WebSockets
* Fetch latest transaction signatures with `getSignaturesForAddress`
* Fetch transaction metadata with `getTransaction`
* Calculate wallet SOL balance changes
* Retry transaction fetches when RPC data is not immediately available
* Store wallet events and transaction details in PostgreSQL
* Query saved transaction history from the CLI
* Prevent duplicate transaction rows using PostgreSQL unique constraints

## Architecture

```text
wallets.txt
    ↓
Tokio task per wallet
    ↓
Solana WebSocket accountSubscribe
    ↓
mPSC event channel
    ↓
Central event processor
    ↓
Solana HTTP RPC
(getSignaturesForAddress + getTransaction)
    ↓
SQLx
    ↓
PostgreSQL
```

## Tech Stack

* Rust
* Tokio
* tokio-tungstenite
* Reqwest
* Serde JSON
* SQLx
* PostgreSQL
* Docker Compose
* Solana JSON-RPC API

## How It Works

1. Wallet addresses are loaded from `wallets.txt`.
2. A separate Tokio task is spawned for each wallet.
3. Each task connects to Solana Devnet WebSocket RPC.
4. The watcher subscribes using `accountSubscribe`.
5. When a wallet balance/account changes, the watcher sends a `WalletEvent` through an async mPSC channel.
6. The central processor:

   * saves the raw wallet event
   * fetches the latest transaction signature
   * fetches transaction details
   * calculates the wallet’s SOL balance change
   * saves transaction details in PostgreSQL

## Project Structure

```text
solana-sentinel/
├── src/
│   └── main.rs
├── wallets.txt
├── compose.yaml
├── Cargo.toml
├── .env
├── .env.example
└── README.md
```

## Prerequisites

Make sure you have installed:

* Rust and Cargo
* Docker Desktop

## Setup

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd solana-sentinel
```

### 2. Create environment file

Create a `.env` file:

```env
DATABASE_URL=postgres://sentinel:sentinel_password@localhost:5432/solana_sentinel
```

You can use `.env.example` as a reference.

### 3. Add wallet addresses

Create or update `wallets.txt`:

```text
9mSSAxDAcHgR2mAu39Xw9pn7yjB2xHRsiV16bnUSWfcK
EC2SsCi68tmnLga7HGdD8aZZ4LGBfZoTKy8ESz8p3FTb
```

Use valid Solana Devnet wallet addresses, one per line.

### 4. Start PostgreSQL

```bash
docker compose up -d
```

Check that PostgreSQL is running:

```bash
docker compose ps
```

### 5. Create database tables

Open PostgreSQL:

```bash
docker compose exec postgres psql -U sentinel -d solana_sentinel
```

Create the wallet events table:

```sql
CREATE TABLE wallet_events (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    account_slot BIGINT NOT NULL,
    lamports BIGINT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Create the transactions table:

```sql
CREATE TABLE transactions (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    signature TEXT NOT NULL,
    transaction_slot BIGINT NOT NULL,
    success BOOLEAN NOT NULL,
    fee_lamports BIGINT NOT NULL,
    wallet_balance_change_lamports BIGINT NOT NULL,
    block_time BIGINT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (wallet_address, signature)
);
```

Exit PostgreSQL:

```sql
\q
```

### 6. Run the watcher

```bash
cargo run
```

## Example Output

```text
Loaded 2 wallet(s)

Connected watcher for: EC2SsCi...
Connected watcher for: 9mSSAx...

New account change detected!
Wallet: EC2SsCi...
Slot: 471213640
Balance: 1.322994876 SOL
Lamports: 1322994876

Event saved to PostgreSQL.
Latest transaction: 3WF3szqp...

Transaction details
Status: Success
Transaction slot: 471211437
Block time (Unix): 1782141872
Fee: 0.000080000 SOL
Wallet SOL change: 0.001000000 SOL
-----------------------------------
```

## Query Transaction History

To view stored transactions for a wallet:

```bash
cargo run -- history <WALLET_ADDRESS>
```

Example:

```bash
cargo run -- history 9mSSAxDAcHgR2mAu39Xw9pn7yjB2xHRsiV16bnUSWfcK
```

Example output:

```text
Recent transactions for 9mSSAx...

Signature: 2agpK5R3...
Status: Success
Transaction slot: 471333219
SOL change: -0.000180000 SOL
Fee: 0.000080000 SOL
Block time (Unix): 1782188000
-----------------------------------
```

## Database Queries

View recent wallet events:

```sql
SELECT *
FROM wallet_events
ORDER BY id DESC
LIMIT 10;
```

View recent transactions:

```sql
SELECT
    wallet_address,
    signature,
    transaction_slot,
    success,
    fee_lamports,
    wallet_balance_change_lamports,
    block_time
FROM transactions
ORDER BY id DESC
LIMIT 10;
```

## What I Learned

* Async Rust with Tokio
* WebSocket communication with `tokio-tungstenite`
* Solana WebSocket RPC subscriptions
* Solana HTTP JSON-RPC methods
* Rust ownership across concurrent Tokio tasks
* `tokio::spawn`
* Async mPSC channels
* Retry logic with `tokio::time::sleep`
* PostgreSQL basics
* Docker Compose
* SQLx connection pools, inserts, and queries
* Building an event-driven data pipeline

## License

MIT
