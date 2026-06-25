//! Network integration test against a real (scratch) `HuggingFace` dataset
//! repo. Ignored by default; run with:
//!
//! ```bash
//! HF_TOKEN=hf_... MEATHOOK_TEST_REPO=you/meathook-test \
//!     cargo test --test hf_integration -- --ignored
//! ```

#![cfg(feature = "huggingface")]

use std::env;

use meathook::{HfSink, Sink as _, WindowMeta};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    station_id: String,
    timestamp: String,
    value: f64,
}

#[tokio::test]
#[ignore = "requires HF_TOKEN and MEATHOOK_TEST_REPO with write access"]
async fn commits_parquet_window_to_scratch_repo() {
    let token = env::var("HF_TOKEN").expect("HF_TOKEN must be set");
    let repo = env::var("MEATHOOK_TEST_REPO").expect("MEATHOOK_TEST_REPO must be set");

    let mut sink = HfSink::new(reqwest::Client::new(), repo, token);
    let now = OffsetDateTime::now_utc();
    let meta = WindowMeta {
        pipeline: "integration_test".to_owned(),
        start: now,
        end: now,
    };

    sink.ingest(
        &meta,
        vec![
            Sample {
                station_id: "S100".into(),
                timestamp: now.to_string(),
                value: 1.0,
            },
            Sample {
                station_id: "S117".into(),
                timestamp: now.to_string(),
                value: 2.0,
            },
        ],
    )
    .await
    .expect("commit should succeed");
}
