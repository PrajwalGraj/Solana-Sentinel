# Solana Sentinel

A Rust-based real-time Solana wallet watcher.

## Milestone 1
- Connects to Solana Devnet through WebSocket RPC
- Subscribes to one wallet using `accountSubscribe`
- Receives live account-change notifications
- Extracts slot, lamports, and SOL balance

## Stack
Rust, Tokio, tokio-tungstenite, serde_json, Solana JSON-RPC WebSockets
