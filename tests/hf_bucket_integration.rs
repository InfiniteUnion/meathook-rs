//! Network integration test against a real (scratch) `HuggingFace` storage
//! bucket. Ignored by default; run with:
//!
//! ```bash
//! HF_TOKEN=hf_... MEATHOOK_TEST_BUCKET=you/meathook-test \
//!     cargo test --features hf-bucket --test hf_bucket_integration -- --ignored
//! ```
//!
//! The bucket is created (or must already exist) before the window is
//! written; verify afterwards with
//! `hf buckets list you/meathook-test --recursive`.

#![cfg(feature = "hf-bucket")]

use std::env;

use meathook::{CreateBucketAction, CreateBucketOutcome, HfBucketSink, Sink as _, WindowMeta};
use satay_reqwest::ReqwestActionExt as _;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    station_id: String,
    timestamp: String,
    value: f64,
}

#[tokio::test]
#[ignore = "requires HF_TOKEN and MEATHOOK_TEST_BUCKET with write access"]
async fn writes_parquet_window_to_scratch_bucket() {
    let token = env::var("HF_TOKEN").expect("HF_TOKEN must be set");
    let bucket = env::var("MEATHOOK_TEST_BUCKET").expect("MEATHOOK_TEST_BUCKET must be set");
    let (namespace, name) = bucket
        .split_once('/')
        .expect("MEATHOOK_TEST_BUCKET must be {namespace}/{bucket}");

    let client = reqwest::Client::new();
    let created = CreateBucketAction {
        namespace: namespace.to_owned(),
        name: name.to_owned(),
        token: token.clone(),
        private: true,
    }
    .send_with(&client)
    .await
    .expect("bucket creation should succeed");
    assert!(
        matches!(
            created,
            CreateBucketOutcome::Created | CreateBucketOutcome::AlreadyExists
        ),
        "unexpected create outcome: {created:?}"
    );

    let mut sink = HfBucketSink::new(client, bucket, token).expect("xet session should start");
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
    .expect("bucket write should succeed");
}
