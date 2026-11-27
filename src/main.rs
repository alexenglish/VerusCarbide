mod app;
mod block_forecast;
mod block_time;
mod bookmarks;
mod cache;
mod chain_state;
mod cli;
mod clipboard;
mod config;
mod copy_values;
mod currency_activity;
mod currency_status;
mod defi;
mod display_time;
mod domain;
mod geoip;
mod height_display;
mod log_feed;
mod protocol;
mod reconciliation;
mod relationships;
mod resolver;
mod rpc;
mod sources;
mod theme;
mod witnesses;
mod world;
mod zmq;

use anyhow::Context;
use cache::Cache;
use cli::Cli;
use config::Config;
use rpc::RpcClient;
use serde_json::{Value, json};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse_verus_style();
    if cli.clipboard_test {
        return clipboard::diagnose_and_test();
    }
    let config = Config::load(&cli).context("failed to load configuration")?;
    if cli.reset_cache {
        Cache::reset_at(
            &config.cache_path,
            world::Network::from_testnet(config.chain.testnet),
        )?;
        println!(
            "Created schema-{} cache at {}",
            crate::cache::SCHEMA_VERSION,
            config.cache_path.display()
        );
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start async runtime")?;
    if let Some(scan) = cli.scan_auto_arbitrage.as_deref() {
        return runtime.block_on(scan_auto_arbitrage(&config, scan));
    }
    let cache = Cache::open(&config.cache_path).context("failed to open cache")?;

    cache.bind_network(world::Network::from_testnet(config.chain.testnet))?;

    if cli.probe {
        return runtime.block_on(probe_daemon(&config, &cache));
    }

    let registry = sources::Registry::load(config)?;
    app::run(runtime, cache, cli.requested_object(), registry)
}

async fn scan_auto_arbitrage(config: &Config, scan: &[String]) -> anyhow::Result<()> {
    let [pool, start, end] = scan else {
        anyhow::bail!("auto-arbitrage scan requires POOL START END");
    };
    let start = start.parse::<u64>().context("invalid scan start height")?;
    let end = end.parse::<u64>().context("invalid scan end height")?;
    anyhow::ensure!(start <= end, "scan start height exceeds end height");
    let client = RpcClient::for_config(config).context("failed to create RPC client")?;
    let mut matches = 0_usize;

    for window_start in (start..=end).step_by(1_000) {
        let window_end = window_start.saturating_add(999).min(end);
        let output = client
            .call_nullable_with_raw::<Value>(
                "getimports",
                &[json!(pool), json!(window_start), json!(window_end)],
            )
            .await
            .with_context(|| {
                format!("getimports failed for {pool} at {window_start}-{window_end}")
            })?;
        for import in output
            .result
            .as_ref()
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let arbitrage_transfers = import
                .get("transfers")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|transfer| {
                    transfer
                        .get("arbitrageonly")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
                .cloned()
                .collect::<Vec<_>>();
            if arbitrage_transfers.is_empty() {
                continue;
            }
            matches += 1;
            println!(
                "{}",
                json!({
                    "pool": pool,
                    "importheight": import.get("importheight"),
                    "importtxid": import.get("importtxid"),
                    "arbitragetransfers": arbitrage_transfers,
                })
            );
        }
    }
    eprintln!("scanned {pool} heights {start}-{end}; found {matches} matching imports");
    Ok(())
}

async fn probe_daemon(config: &Config, _cache: &Cache) -> anyhow::Result<()> {
    let client = RpcClient::for_config(config).context("failed to create RPC client")?;
    let info = client
        .get_info()
        .await
        .context("failed to identify daemon")?;
    config.bind_system(&info.result)?;
    let tip = client
        .get_block_count()
        .await
        .context("failed to call getblockcount")?;

    println!(
        "connected to {}:{} on {} at height {}",
        config.daemon.host,
        config.daemon.port,
        config.chain_cache_key(),
        tip.result
    );
    Ok(())
}
