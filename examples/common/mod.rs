//! Wiring shared by the `nea_weather*` examples: NEA collectors, row-shaped
//! records, TOML config plumbing, tracing setup, and the durable
//! `Tier(JsonlStore)` each example finishes with its own terminal sink
//! (`HfSink` for dataset repos, `HfBucketSink` for storage buckets).
//!
//! This file is a module of each example, not an example itself: cargo only
//! discovers `examples/<name>.rs` and `examples/<name>/main.rs`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as _;
use meathook::{Collector, FlushPolicy, JsonlStore, SatayCollector, Sink, SinkStack, Tier};
use nea_rs::{
    AirTemperatureOperationResponse, NeaReadingSnapshot, NeaWeatherStation, Pm25OperationResponse,
    RainfallOperationResponse,
};
use satay_reqwest::ReqwestActionExt as _;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use tracing::warn;

/// The stack every `nea_weather*` example builds:
/// durable spool directly wrapping the example's terminal sink `S`.
pub type DurableStack<R, S> = Tier<R, JsonlStore<R>, S>;

/// Example configuration. `S` is the example-specific sink section
/// (`[sink.huggingface]` or `[sink.bucket]`).
#[derive(Debug, Deserialize)]
pub struct Config<S> {
    pub spool_dir: PathBuf,
    pub flush: FlushConfig,
    pub sink: S,
    #[serde(default)]
    pub collectors: HashMap<String, CollectorConfig>,
}

#[derive(Debug, Deserialize)]
pub struct FlushConfig {
    #[serde(with = "humantime_serde")]
    pub every: Duration,
    pub max_records: usize,
}

#[derive(Debug, Deserialize)]
pub struct CollectorConfig {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
}

impl<S> Config<S> {
    /// Configured poll interval for a collector, 1m when unlisted.
    pub fn interval(&self, collector: &str) -> Duration {
        self.collectors
            .get(collector)
            .map_or(Duration::from_secs(60), |c| c.interval)
    }
}

/// One station reading, flattened row-shape for parquet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationReading {
    pub station_id: String,
    pub station_name: String,
    pub timestamp: String,
    pub value: f64,
}

/// One regional PM2.5 reading.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionReading {
    pub region: String,
    pub timestamp: String,
    pub value: f64,
}

fn fmt_ts(ts: time::OffsetDateTime) -> String {
    ts.format(&Rfc3339).unwrap_or_else(|_| ts.to_string())
}

fn flatten_station_data(
    stations: &[NeaWeatherStation],
    readings: &[NeaReadingSnapshot],
) -> Vec<StationReading> {
    readings
        .iter()
        .flat_map(|snapshot| {
            let timestamp = fmt_ts(snapshot.timestamp);
            snapshot.data.iter().filter_map(move |reading| {
                let station = stations
                    .iter()
                    .find(|station| station.id == reading.station_id)?;
                Some(StationReading {
                    station_id: station.id.clone().into(),
                    station_name: station.name.clone(),
                    timestamp: timestamp.clone(),
                    value: reading.value,
                })
            })
        })
        .collect()
}

fn flatten_air_temperature(response: AirTemperatureOperationResponse) -> Vec<StationReading> {
    match response {
        AirTemperatureOperationResponse::Ok(ok) => {
            flatten_station_data(&ok.data.stations, &ok.data.readings)
        }
        other => {
            warn!(?other, "air_temperature returned non-ok response");
            Vec::new()
        }
    }
}

fn flatten_rainfall(response: RainfallOperationResponse) -> Vec<StationReading> {
    match response {
        RainfallOperationResponse::Ok(ok) => {
            flatten_station_data(&ok.data.stations, &ok.data.readings)
        }
        other => {
            warn!(?other, "rainfall returned non-ok response");
            Vec::new()
        }
    }
}

fn flatten_pm25(response: Pm25OperationResponse) -> Vec<RegionReading> {
    match response {
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
    }
}

/// Wiring shared by every pipeline, independent of the terminal sink.
#[derive(Clone)]
pub struct Ctx {
    pub client: reqwest::Client,
    pub api_key: Option<String>,
    pub token: String,
    pub spool_dir: PathBuf,
    pub policy: FlushPolicy,
}

impl Ctx {
    pub fn from_config<S>(config: &Config<S>) -> anyhow::Result<Self> {
        Ok(Self {
            // The timeout matters beyond hygiene: a stalled upload holds
            // sinks (and the dataset example's commit gate) until the OS
            // abandons the connection.
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .context("building http client")?,
            api_key: std::env::var("X_API_KEY").ok(),
            token: std::env::var("HF_TOKEN").context("HF_TOKEN must be set")?,
            spool_dir: config.spool_dir.clone(),
            policy: FlushPolicy::new(config.flush.every, config.flush.max_records),
        })
    }

    pub fn api(&self) -> nea_rs::Api {
        let api = nea_rs::Api::new();
        match &self.api_key {
            Some(key) => api.x_api_key(key.clone()),
            None => api,
        }
    }

    /// Durable write-ahead spool directly wrapping `terminal`.
    pub fn durable<R, S>(&self, pipeline: &str, terminal: S) -> DurableStack<R, S>
    where
        R: Serialize + DeserializeOwned + Send + 'static,
        S: Sink<R>,
    {
        SinkStack::new()
            .tier(JsonlStore::new(self.spool_dir.join(pipeline)), self.policy)
            .terminal(terminal)
    }
}

pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,meathook=debug".into()),
        )
        .init();
}

pub fn load_config<S: DeserializeOwned>(config_path: &str) -> anyhow::Result<Config<S>> {
    toml::from_str(
        &std::fs::read_to_string(config_path)
            .with_context(|| format!("reading config {config_path}"))?,
    )
    .with_context(|| format!("parsing config {config_path}"))
}

pub fn air_temperature_collector(
    ctx: &Ctx,
) -> impl Collector<Record = StationReading, Error = satay_reqwest::Error> + 'static + use<> {
    let api = ctx.api();
    SatayCollector::new(
        "air_temperature",
        ctx.client.clone(),
        move |client| {
            let api = api.clone();
            async move {
                api.weather_readings()
                    .air_temperature()
                    .send_with(&client)
                    .await
            }
        },
        flatten_air_temperature,
    )
}

pub fn rainfall_collector(
    ctx: &Ctx,
) -> impl Collector<Record = StationReading, Error = satay_reqwest::Error> + 'static + use<> {
    let api = ctx.api();
    SatayCollector::new(
        "rainfall",
        ctx.client.clone(),
        move |client| {
            let api = api.clone();
            async move { api.weather_readings().rainfall().send_with(&client).await }
        },
        flatten_rainfall,
    )
}

pub fn pm25_collector(
    ctx: &Ctx,
) -> impl Collector<Record = RegionReading, Error = satay_reqwest::Error> + 'static + use<> {
    let api = ctx.api();
    SatayCollector::new(
        "pm25",
        ctx.client.clone(),
        move |client| {
            let api = api.clone();
            async move { api.air_quality().pm25().send_with(&client).await }
        },
        flatten_pm25,
    )
}
