//! Reference meathook consumer: collects NEA (data.gov.sg) realtime weather
//! readings and ships configured parquet windows to a `HuggingFace` dataset
//! repo.
//!
//! Each pipeline's stack is `Tier(MemStore) → Tier(JsonlStore) → HfSink`.
//! The outer memory tier batches for five minutes or 10,000 records; its
//! batches are then appended to an fsynced JSONL segment, and the configured
//! durable tier flushes windows to HF. Records still held in memory remain
//! volatile; leftover JSONL segments replay on the next start.
//!
//! Collectors, records, and config plumbing are shared with the bucket
//! variant in `examples/nea_weather_bucket.rs` via `examples/common/mod.rs`.
//!
//! ```bash
//! HF_TOKEN=hf_... cargo run --example nea_weather -- examples/meathook.toml
//! ```

#[path = "common/mod.rs"]
mod common;

use common::{
    Config, Ctx, air_temperature_collector, init_tracing, load_config, pm25_collector,
    rainfall_collector,
};
use meathook::{CommitGate, HfSink, Meathook, Pipeline};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct SinkConfig {
    huggingface: HfConfig,
}

#[derive(Debug, Deserialize)]
struct HfConfig {
    repo: String,
    #[serde(default = "default_branch")]
    branch: String,
}

fn default_branch() -> String {
    "main".to_owned()
}

/// Terminal-sink wiring specific to the dataset example.
#[derive(Clone)]
struct SinkCtx {
    repo: String,
    branch: String,
    /// Shared by every pipeline's sink: commits to the one HF repo go out
    /// one at a time instead of racing into HF's per-repo commit queue.
    gate: CommitGate,
}

impl SinkCtx {
    fn from_config(config: &Config<SinkConfig>) -> Self {
        Self {
            repo: config.sink.huggingface.repo.clone(),
            branch: config.sink.huggingface.branch.clone(),
            gate: CommitGate::new(),
        }
    }

    fn tiered<R>(&self, ctx: &Ctx, pipeline: &str) -> common::TieredStack<R, HfSink<R>>
    where
        R: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        let terminal = HfSink::new(ctx.client.clone(), self.repo.clone(), ctx.token.clone())
            .branch(self.branch.clone())
            .gate(self.gate.clone());
        ctx.tiered(pipeline, terminal)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/meathook.toml".to_owned());
    let config = load_config::<SinkConfig>(&config_path)?;
    let ctx = Ctx::from_config(&config)?;
    let sink = SinkCtx::from_config(&config);

    let air_temperature = {
        let ctx = ctx.clone();
        let sink = sink.clone();
        let interval = config.interval("air_temperature");
        move || {
            let collector = air_temperature_collector(&ctx);
            Pipeline::new(
                collector,
                sink.tiered::<common::StationReading>(&ctx, "air_temperature"),
                interval,
            )
            .with_key_fn(|r: &common::StationReading| (r.station_id.clone(), r.timestamp.clone()))
        }
    };

    let rainfall = {
        let ctx = ctx.clone();
        let sink = sink.clone();
        let interval = config.interval("rainfall");
        move || {
            let collector = rainfall_collector(&ctx);
            Pipeline::new(
                collector,
                sink.tiered::<common::StationReading>(&ctx, "rainfall"),
                interval,
            )
            .with_key_fn(|r: &common::StationReading| (r.station_id.clone(), r.timestamp.clone()))
        }
    };

    let pm25 = {
        let ctx = ctx.clone();
        let sink = sink.clone();
        let interval = config.interval("pm25");
        move || {
            let collector = pm25_collector(&ctx);
            Pipeline::new(
                collector,
                sink.tiered::<common::RegionReading>(&ctx, "pm25"),
                interval,
            )
            .with_key_fn(|r: &common::RegionReading| (r.region.clone(), r.timestamp.clone()))
        }
    };

    Meathook::builder()
        .pipeline(air_temperature)
        .pipeline(rainfall)
        .pipeline(pm25)
        .run()
        .await?;
    Ok(())
}
