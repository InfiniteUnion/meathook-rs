//! Bucket variant of the reference consumer: the same NEA collectors and
//! buffering tiers as `nea_weather`, but each window is written to a
//! Hugging Face storage bucket (`hf://buckets/{namespace}/{bucket}`) via
//! `HfBucketSink` instead of committed to a dataset repo.
//!
//! Each pipeline's stack is `Tier(MemStore) → Tier(JsonlStore) →
//! HfBucketSink`. Windows land at the same deterministic Hive-style paths,
//! uploaded through Xet with chunk deduplication, then registered with one
//! sans-IO `BatchAction`. Buckets have no commit queue, so there is no
//! `CommitGate` to share; the JSONL spool still owns custody between
//! collection and delivery.
//!
//! ```bash
//! HF_TOKEN=hf_... cargo run --example nea_weather_bucket -- examples/meathook_bucket.toml
//! ```
//!
//! Requires the `hf-bucket` feature.

#[path = "common/mod.rs"]
mod common;

use common::{
    Config, Ctx, air_temperature_collector, init_tracing, load_config, pm25_collector,
    rainfall_collector,
};
use meathook::{HfBucketSink, Meathook, Pipeline};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct SinkConfig {
    bucket: BucketConfig,
}

#[derive(Debug, Deserialize)]
struct BucketConfig {
    /// `{namespace}/{bucket}`; must already exist — create it with
    /// `CreateBucketAction` or `hf buckets create`.
    id: String,
}

#[derive(Clone)]
struct SinkCtx {
    bucket: String,
}

impl SinkCtx {
    fn from_config(config: &Config<SinkConfig>) -> Self {
        Self {
            bucket: config.sink.bucket.id.clone(),
        }
    }

    fn tiered<R>(
        &self,
        ctx: &Ctx,
        pipeline: &str,
    ) -> Result<common::TieredStack<R, HfBucketSink<R>>, meathook::HfBucketSinkError>
    where
        R: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        let terminal =
            HfBucketSink::new(ctx.client.clone(), self.bucket.clone(), ctx.token.clone())?;
        Ok(ctx.tiered(pipeline, terminal))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/meathook_bucket.toml".to_owned());
    let config = load_config::<SinkConfig>(&config_path)?;
    let ctx = Ctx::from_config(&config)?;
    let sink = SinkCtx::from_config(&config);

    let air_temperature = {
        let ctx = ctx.clone();
        let sink = sink.clone();
        let interval = config.interval("air_temperature");
        move || {
            let collector = air_temperature_collector(&ctx);
            let stack = sink
                .tiered::<common::StationReading>(&ctx, "air_temperature")
                .expect("xet session");
            Pipeline::new(collector, stack, interval).with_key_fn(|r: &common::StationReading| {
                (r.station_id.clone(), r.timestamp.clone())
            })
        }
    };

    let rainfall = {
        let ctx = ctx.clone();
        let sink = sink.clone();
        let interval = config.interval("rainfall");
        move || {
            let collector = rainfall_collector(&ctx);
            let stack = sink
                .tiered::<common::StationReading>(&ctx, "rainfall")
                .expect("xet session");
            Pipeline::new(collector, stack, interval).with_key_fn(|r: &common::StationReading| {
                (r.station_id.clone(), r.timestamp.clone())
            })
        }
    };

    let pm25 = {
        let ctx = ctx.clone();
        let sink = sink.clone();
        let interval = config.interval("pm25");
        move || {
            let collector = pm25_collector(&ctx);
            let stack = sink
                .tiered::<common::RegionReading>(&ctx, "pm25")
                .expect("xet session");
            Pipeline::new(collector, stack, interval)
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
