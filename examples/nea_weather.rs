//! Reference meathook consumer: collects NEA (data.gov.sg) realtime weather
//! readings and ships hourly parquet windows to a HuggingFace dataset repo.
//!
//! Each pipeline's stack is `DiskSpool → HfSink`: every tick is appended to
//! an fsynced JSONL segment before the ingest returns (write-ahead), and the
//! spool flushes a parquet file per window to HF. Leftover segments from a
//! crash replay on the next start.
//!
//! ```bash
//! HF_TOKEN=hf_... cargo run --example nea_weather -- examples/meathook.toml
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use meathook::{DiskSpool, FlushPolicy, HfSink, Meathook, Pipeline, SatayCollector, SinkExt as _};
use nea_rs::{
    AirTemperatureOperationResponse, NeaReadingSnapshot, NeaWeatherStation, Pm25OperationResponse,
    RainfallOperationResponse,
};
use satay_reqwest::ReqwestActionExt as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use tracing::warn;

#[derive(Debug, Deserialize)]
struct Config {
    spool_dir: PathBuf,
    flush: FlushConfig,
    sink: SinkConfig,
    #[serde(default)]
    collectors: HashMap<String, CollectorConfig>,
}

#[derive(Debug, Deserialize)]
struct FlushConfig {
    #[serde(with = "humantime_serde")]
    every: Duration,
    max_records: usize,
}

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

#[derive(Debug, Deserialize)]
struct CollectorConfig {
    #[serde(with = "humantime_serde")]
    interval: Duration,
}

impl Config {
    fn interval(&self, collector: &str) -> Duration {
        self.collectors
            .get(collector)
            .map(|c| c.interval)
            .unwrap_or(Duration::from_secs(60))
    }
}

/// One station reading, flattened row-shape for parquet.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StationReading {
    station_id: String,
    station_name: String,
    timestamp: String,
    value: f64,
}

/// One regional PM2.5 reading.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegionReading {
    region: String,
    timestamp: String,
    value: f64,
}

fn fmt_ts(ts: time::OffsetDateTime) -> String {
    ts.format(&Rfc3339).unwrap_or_else(|_| ts.to_string())
}

fn flatten_station_data(
    stations: &[NeaWeatherStation],
    readings: &[NeaReadingSnapshot],
) -> Vec<StationReading> {
    let names: HashMap<String, &str> = stations
        .iter()
        .map(|s| (s.id.to_string(), s.name.as_str()))
        .collect();
    let names = &names;
    readings
        .iter()
        .flat_map(|snapshot| {
            let timestamp = fmt_ts(snapshot.timestamp);
            snapshot.data.iter().map(move |reading| {
                let station_id = reading.station_id.to_string();
                StationReading {
                    station_name: names.get(&station_id).copied().unwrap_or("").to_owned(),
                    station_id,
                    timestamp: timestamp.clone(),
                    value: reading.value,
                }
            })
        })
        .collect()
}

/// Shared wiring context cloned into every pipeline factory.
#[derive(Clone)]
struct Ctx {
    client: reqwest::Client,
    api_key: Option<String>,
    repo: String,
    branch: String,
    token: String,
    spool_dir: PathBuf,
    policy: FlushPolicy,
}

impl Ctx {
    fn api(&self) -> nea_rs::Api {
        let api = nea_rs::Api::new();
        match &self.api_key {
            Some(key) => api.x_api_key(key.clone()),
            None => api,
        }
    }

    /// Terminal HF sink behind a per-pipeline durable spool.
    fn spooled_hf<R>(&self, pipeline: &str) -> DiskSpool<R, HfSink<R>>
    where
        R: Serialize + DeserializeOwned + Send + 'static,
    {
        HfSink::new(self.client.clone(), self.repo.clone(), self.token.clone())
            .branch(self.branch.clone())
            .spooled(self.spool_dir.join(pipeline), self.policy)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,meathook=debug".into()),
        )
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/meathook.toml".to_owned());
    let config: Config = toml::from_str(
        &std::fs::read_to_string(&config_path)
            .with_context(|| format!("reading config {config_path}"))?,
    )
    .with_context(|| format!("parsing config {config_path}"))?;

    let ctx = Ctx {
        client: reqwest::Client::new(),
        api_key: std::env::var("X_API_KEY").ok(),
        repo: config.sink.huggingface.repo.clone(),
        branch: config.sink.huggingface.branch.clone(),
        token: std::env::var("HF_TOKEN").context("HF_TOKEN must be set")?,
        spool_dir: config.spool_dir.clone(),
        policy: FlushPolicy::new(config.flush.every, config.flush.max_records),
    };

    let air_temperature = {
        let ctx = ctx.clone();
        let interval = config.interval("air_temperature");
        move || {
            let api = ctx.api();
            let collector = SatayCollector::new(
                "air_temperature",
                ctx.client.clone(),
                move |client| {
                    let api = api.clone();
                    async move { api.air_temperature().send_with(&client).await }
                },
                |response| match response {
                    AirTemperatureOperationResponse::Ok(ok) => {
                        flatten_station_data(&ok.data.stations, &ok.data.readings)
                    }
                    other => {
                        warn!(?other, "air_temperature returned non-ok response");
                        Vec::new()
                    }
                },
            );
            Pipeline::new(collector, ctx.spooled_hf("air_temperature"), interval)
                .with_key_fn(|r: &StationReading| (r.station_id.clone(), r.timestamp.clone()))
        }
    };

    let rainfall = {
        let ctx = ctx.clone();
        let interval = config.interval("rainfall");
        move || {
            let api = ctx.api();
            let collector = SatayCollector::new(
                "rainfall",
                ctx.client.clone(),
                move |client| {
                    let api = api.clone();
                    async move { api.rainfall().send_with(&client).await }
                },
                |response| match response {
                    RainfallOperationResponse::Ok(ok) => {
                        flatten_station_data(&ok.data.stations, &ok.data.readings)
                    }
                    other => {
                        warn!(?other, "rainfall returned non-ok response");
                        Vec::new()
                    }
                },
            );
            Pipeline::new(collector, ctx.spooled_hf("rainfall"), interval)
                .with_key_fn(|r: &StationReading| (r.station_id.clone(), r.timestamp.clone()))
        }
    };

    let pm25 = {
        let ctx = ctx.clone();
        let interval = config.interval("pm25");
        move || {
            let api = ctx.api();
            let collector = SatayCollector::new(
                "pm25",
                ctx.client.clone(),
                move |client| {
                    let api = api.clone();
                    async move { api.pm25().send_with(&client).await }
                },
                |response| match response {
                    Pm25OperationResponse::Ok(ok) => ok
                        .data
                        .items
                        .iter()
                        .flat_map(|item| {
                            let timestamp = fmt_ts(item.timestamp);
                            let regional = &item.readings.pm25_one_hourly;
                            [
                                ("east", regional.east),
                                ("west", regional.west),
                                ("north", regional.north),
                                ("south", regional.south),
                                ("central", regional.central),
                            ]
                            .map(|(region, value)| RegionReading {
                                region: region.to_owned(),
                                timestamp: timestamp.clone(),
                                value: f64::from(u16::from(value)),
                            })
                        })
                        .collect(),
                    other => {
                        warn!(?other, "pm25 returned non-ok response");
                        Vec::new()
                    }
                },
            );
            Pipeline::new(collector, ctx.spooled_hf("pm25"), interval)
                .with_key_fn(|r: &RegionReading| (r.region.clone(), r.timestamp.clone()))
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
