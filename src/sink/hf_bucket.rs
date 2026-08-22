//! [`HfBucketSink`]: terminal sink writing encoded window files (parquet
//! by default) to a [Hugging Face storage bucket]
//! (<https://huggingface.co/storage>), `hf://buckets/{namespace}/{bucket}`.
//!
//! Two legs, unlike the git-commit [`HfSink`]:
//!
//! 1. Content upload through Xet, the Hub's content-addressed chunk
//!    store, via the official `hf-xet` crate. Xet chunks the bytes and
//!    deduplicates them against everything already stored, so replaying
//!    identical bytes (spool recovery, retried drain) transfers almost
//!    nothing. `hf-xet` fetches and refreshes its CAS write tokens from
//!    `/api/buckets/{bucket}/xet-write-token` itself.
//! 2. One sans-IO [`BatchAction`] registering the uploaded content under
//!    its bucket path: `POST /api/buckets/{bucket}/batch` with a single
//!    NDJSON `addFile` line carrying the xet hash — sent through the same
//!    satay transport as every other meathook HTTP call.
//!
//! Buckets have no server-side commit queue, so there is no
//! [`CommitGate`] equivalent; transient failures retry in-sink like
//! `HfSink`'s commits. The `/batch` endpoint is non-transactional: if the
//! process dies after the Xet commit but before registration, the chunks
//! sit unreferenced in CAS and the replayed upload deduplicates against
//! them, so a JSONL-tier replay costs almost nothing.
//!
//! [`HfSink`]: crate::sink::huggingface::HfSink
//! [`CommitGate`]: crate::sink::huggingface::CommitGate

use std::error;
use std::marker::PhantomData;
use std::time::Duration;

use http::StatusCode;
use http::header;
use satay_reqwest::ReqwestActionExt;
use satay_runtime::{Action, RequestParts, ResponseParts, insert_header, into_request};
use serde::Serialize;
use serde::de;
use tokio::time::sleep;
use tracing::{info, warn};
use xet::XetError;
use xet::xet_session::{
    DeduplicationMetrics, HeaderMap, HeaderValue, Sha256Policy, XetSession, XetSessionBuilder,
};

use crate::encode::{Encoder, ParquetEncodeError, ParquetEncoder};
use crate::sink::{Sink, WindowMeta, object_path};

/// Error from the `HuggingFace` bucket sink.
#[derive(Debug, thiserror::Error)]
pub enum HfBucketSinkError<E: error::Error + Send + Sync + 'static = ParquetEncodeError> {
    #[error(transparent)]
    Encode(E),
    #[error("xet upload failed: {0}")]
    Xet(#[from] XetError),
    #[error("invalid hugging face token: {0}")]
    Token(#[from] header::InvalidHeaderValue),
    #[error("transport error: {0}")]
    Transport(#[from] satay_reqwest::Error),
    #[error("hugging face rejected batch ({status}): {body}")]
    Rejected { status: StatusCode, body: String },
}

/// One `addFile` registration in a `HuggingFace` storage bucket, as a
/// sans-IO [`Action`]: `POST /api/buckets/{bucket}/batch` with an NDJSON
/// payload referencing previously uploaded Xet content by hash.
///
/// The xet hash is filled in after the content upload; send the action
/// only once the bytes are committed to CAS.
#[derive(Debug, Clone)]
pub struct BatchAction {
    pub bucket: String,
    pub token: String,
    /// Path of the file inside the bucket, e.g.
    /// `data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet`.
    pub path_in_bucket: String,
    /// Xet hash of the uploaded content.
    pub xet_hash: String,
    /// File modification time in Unix milliseconds.
    pub mtime_ms: i64,
}

/// Decoded result of a [`BatchAction`].
///
/// Non-2xx responses decode into [`Rejected`](BatchOutcome::Rejected)
/// rather than an error so the typed status/body survive the fixed
/// `satay_runtime::Error` decode signature.
#[derive(Debug, Clone)]
pub enum BatchOutcome {
    Added,
    Rejected { status: StatusCode, body: String },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AddFileLine<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    path: &'a str,
    xet_hash: &'a str,
    mtime: i64,
}

impl Action for BatchAction {
    type Response = BatchOutcome;

    fn request(self) -> Result<http::Request<Vec<u8>>, satay_runtime::Error> {
        let uri = format!("https://huggingface.co/api/buckets/{}/batch", self.bucket);

        let mut body = serde_json::to_vec(&AddFileLine {
            kind: "addFile",
            path: &self.path_in_bucket,
            xet_hash: &self.xet_hash,
            mtime: self.mtime_ms,
        })?;
        body.push(b'\n');

        let mut headers = http::HeaderMap::new();
        insert_header(
            &mut headers,
            "authorization",
            &format!("Bearer {}", self.token),
        )?;
        insert_header(&mut headers, "content-type", "application/x-ndjson")?;
        if let Some(auth) = headers.get_mut(header::AUTHORIZATION) {
            auth.set_sensitive(true);
        }

        into_request(RequestParts {
            method: http::Method::POST,
            uri,
            headers,
            body,
        })
    }

    fn decode<B: AsRef<[u8]>>(
        response: ResponseParts<B>,
    ) -> Result<Self::Response, satay_runtime::Error> {
        if response.status.is_success() {
            Ok(BatchOutcome::Added)
        } else {
            Ok(BatchOutcome::Rejected {
                status: response.status,
                body: String::from_utf8_lossy(response.body.as_ref()).into_owned(),
            })
        }
    }
}

/// Sans-IO creation of a `HuggingFace` storage bucket:
/// `POST /api/buckets/{namespace}/{name}`.
///
/// A 409 decodes to [`AlreadyExists`](CreateBucketOutcome::AlreadyExists).
/// A 401/403 can also mean "exists, but this token may not create in the
/// namespace" — verify with `hf buckets info` when that distinction
/// matters.
#[derive(Debug, Clone)]
pub struct CreateBucketAction {
    pub namespace: String,
    pub name: String,
    pub token: String,
    pub private: bool,
}

/// Decoded result of a [`CreateBucketAction`].
#[derive(Debug, Clone)]
pub enum CreateBucketOutcome {
    Created,
    AlreadyExists,
    Rejected { status: StatusCode, body: String },
}

#[derive(Serialize)]
struct CreateBucketBody {
    private: bool,
}

impl Action for CreateBucketAction {
    type Response = CreateBucketOutcome;

    fn request(self) -> Result<http::Request<Vec<u8>>, satay_runtime::Error> {
        let uri = format!(
            "https://huggingface.co/api/buckets/{}/{}",
            self.namespace, self.name
        );
        let body = serde_json::to_vec(&CreateBucketBody {
            private: self.private,
        })?;

        let mut headers = http::HeaderMap::new();
        insert_header(
            &mut headers,
            "authorization",
            &format!("Bearer {}", self.token),
        )?;
        insert_header(&mut headers, "content-type", "application/json")?;
        if let Some(auth) = headers.get_mut(header::AUTHORIZATION) {
            auth.set_sensitive(true);
        }

        into_request(RequestParts {
            method: http::Method::POST,
            uri,
            headers,
            body,
        })
    }

    fn decode<B: AsRef<[u8]>>(
        response: ResponseParts<B>,
    ) -> Result<Self::Response, satay_runtime::Error> {
        if response.status.is_success() {
            Ok(CreateBucketOutcome::Created)
        } else if response.status == StatusCode::CONFLICT {
            Ok(CreateBucketOutcome::AlreadyExists)
        } else {
            Ok(CreateBucketOutcome::Rejected {
                status: response.status,
                body: String::from_utf8_lossy(response.body.as_ref()).into_owned(),
            })
        }
    }
}

/// Refresh route `hf-xet` uses to obtain CAS write tokens for a bucket.
fn xet_write_token_url(bucket: &str) -> String {
    format!("https://huggingface.co/api/buckets/{bucket}/xet-write-token")
}

/// One Xet session bound to a bucket's write-token refresh route.
struct XetUploader {
    session: XetSession,
    refresh_url: String,
    refresh_headers: HeaderMap,
}

impl XetUploader {
    fn new(bucket: &str, token: &str) -> Result<Self, HfBucketSinkError> {
        let session = XetSessionBuilder::new()
            .build()
            .map_err(HfBucketSinkError::Xet)?;
        let mut refresh_headers = HeaderMap::new();
        let auth =
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(HfBucketSinkError::Token)?;
        refresh_headers.insert(header::AUTHORIZATION, auth);
        Ok(Self {
            session,
            refresh_url: xet_write_token_url(bucket),
            refresh_headers,
        })
    }

    /// Upload the bytes, finalize the Xet commit, and return the hash
    /// referencing them plus dedup metrics for observability.
    async fn upload(
        &self,
        path: &str,
        content: &[u8],
    ) -> Result<(String, DeduplicationMetrics), XetError> {
        let commit = self
            .session
            .new_upload_commit()?
            .with_token_refresh_url(self.refresh_url.clone(), self.refresh_headers.clone())
            .build()
            .await?;
        let upload = commit
            .upload_bytes(content.to_vec(), Sha256Policy::Skip, Some(path.to_owned()))
            .await?;
        let metadata = upload.finalize_ingestion().await?;
        let uploaded = (metadata.xet_info.hash, metadata.dedup_metrics);
        commit.commit().await?;
        Ok(uploaded)
    }
}

/// Total upload+register attempts per window (1 initial + 3 retries;
/// backoff between attempts is 2s, 4s, 8s).
const WRITE_ATTEMPTS: u32 = 4;

/// Whether a failed write is worth retrying in-sink: transport flakes,
/// Xet network failures, contention/server-side statuses. Authentication
/// problems (both legs), bad requests, and encode failures propagate
/// immediately.
fn transient<E: error::Error + Send + Sync + 'static>(error: &HfBucketSinkError<E>) -> bool {
    match error {
        HfBucketSinkError::Xet(XetError::Authentication(_))
        | HfBucketSinkError::Encode(_)
        | HfBucketSinkError::Token(_) => false,
        HfBucketSinkError::Xet(_) | HfBucketSinkError::Transport(_) => true,
        HfBucketSinkError::Rejected { status, .. } => {
            *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
        }
    }
}

/// Deterministic `mtime` for the batch `addFile` line: the window end,
/// not wall-clock, so replayed windows register byte-identical payloads.
fn mtime_ms(meta: &WindowMeta) -> i64 {
    meta.end.unix_timestamp().saturating_mul(1000) + i64::from(meta.end.nanosecond() / 1_000_000)
}

/// Terminal sink: encodes each ingested window with its [`Encoder`]
/// (parquet by default) and writes it to a `HuggingFace` storage bucket
/// at the same deterministic, Hive-style path [`HfSink`] uses:
///
/// ```text
/// data/{pipeline}/{YYYY-MM-DD}/{HH}-{MM}-{SS}-{fnv1a64(content)}.{E::EXT}
/// }
/// ```
///
/// Replays stay idempotent like `HfSink`'s — and cheaper: Xet
/// deduplication means re-uploading identical bytes transfers almost
/// nothing before the registration overwrites the same path.
///
/// Transient failures (transport errors, Xet network errors, 429, 5xx)
/// retry a few times in-sink with backoff before the error propagates;
/// an upstream [`Tier`] retains records when this sink errors and
/// retries at its next firing (or replays them on the next start).
///
/// [`HfSink`]: crate::sink::huggingface::HfSink
/// [`Tier`]: crate::Tier
pub struct HfBucketSink<R, E = ParquetEncoder> {
    client: reqwest::Client,
    bucket: String,
    token: String,
    xet: XetUploader,
    encoder: E,
    _record: PhantomData<fn(R)>,
}

impl<R> HfBucketSink<R> {
    /// Sink writing to `bucket` (e.g. `"zeon256/weather-windows"`). The
    /// bucket must already exist — create it with [`CreateBucketAction`]
    /// or `hf buckets create`. The token is a `HuggingFace` access token
    /// with write access, typically from the `HF_TOKEN` env var.
    ///
    /// # Errors
    ///
    /// Fails only if the token is not valid header material or the
    /// underlying Xet session cannot start.
    pub fn new(
        client: reqwest::Client,
        bucket: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, HfBucketSinkError> {
        let bucket = bucket.into();
        let token = token.into();
        Ok(Self {
            xet: XetUploader::new(&bucket, &token)?,
            client,
            bucket,
            token,
            encoder: ParquetEncoder::default(),
            _record: PhantomData,
        })
    }
}

impl<R, E: Encoder> HfBucketSink<R, E> {
    /// Swap the wire format this sink ships (parquet by default):
    ///
    /// ```no_run
    /// use meathook::{HfBucketSink, JsonEncoder};
    ///
    /// let sink = HfBucketSink::<()>::new(reqwest::Client::new(), "you/bucket", "hf_token")
    ///     .expect("xet session")
    ///     .encoder(JsonEncoder);
    /// # let _ = sink;
    /// ```
    #[must_use]
    pub fn encoder<E2: Encoder>(self, encoder: E2) -> HfBucketSink<R, E2> {
        HfBucketSink {
            client: self.client,
            bucket: self.bucket,
            token: self.token,
            xet: self.xet,
            encoder,
            _record: PhantomData,
        }
    }

    /// One upload-and-register round trip: Xet commit, then the batch
    /// `addFile` referencing it.
    async fn ship(
        &self,
        mut action: BatchAction,
        content: &[u8],
    ) -> Result<DeduplicationMetrics, HfBucketSinkError<E::Error>> {
        let (xet_hash, metrics) = self.xet.upload(&action.path_in_bucket, content).await?;
        action.xet_hash = xet_hash;
        match action.send_with(&self.client).await? {
            BatchOutcome::Added => Ok(metrics),
            BatchOutcome::Rejected { status, body } => {
                Err(HfBucketSinkError::Rejected { status, body })
            }
        }
    }
}

impl<R, E> Sink<R> for HfBucketSink<R, E>
where
    R: Serialize + de::DeserializeOwned + Send + 'static,
    E: Encoder,
{
    type Error = HfBucketSinkError<E::Error>;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        if records.is_empty() {
            return Ok(());
        }
        let content = self
            .encoder
            .encode(&records)
            .map_err(HfBucketSinkError::Encode)?;
        let path_in_bucket = object_path(meta, &content, E::EXT);
        let action = BatchAction {
            bucket: self.bucket.clone(),
            token: self.token.clone(),
            path_in_bucket: path_in_bucket.clone(),
            xet_hash: String::new(),
            mtime_ms: mtime_ms(meta),
        };

        let mut attempt = 1;
        let metrics = loop {
            match self.ship(action.clone(), &content).await {
                Ok(metrics) => break metrics,
                Err(error) if attempt < WRITE_ATTEMPTS && transient(&error) => {
                    let backoff = Duration::from_secs(1 << attempt);
                    warn!(
                        pipeline = %meta.pipeline,
                        %error,
                        attempt,
                        ?backoff,
                        "hugging face bucket write failed; retrying"
                    );
                    sleep(backoff).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
        info!(
            pipeline = %meta.pipeline,
            path = %path_in_bucket,
            records = records.len(),
            bytes = metrics.total_bytes,
            uploaded = metrics.new_bytes,
            "wrote window to hugging face bucket"
        );
        Ok(())
    }

    /// No-op: this terminal sink ships every batch as it is ingested.
    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;
    use crate::encode::JsonEncoder;

    fn batch_action() -> BatchAction {
        BatchAction {
            bucket: "zeon256/sg-weather".into(),
            token: "hf_secret".into(),
            path_in_bucket: "data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet".into(),
            xet_hash: "96e637d9665bd35477b1908a23f2e254edfba0618dbd2d62f90a6baee7d139cf".into(),
            mtime_ms: 1_781_254_800_000,
        }
    }

    #[test]
    fn batch_request_shape() {
        let request = batch_action().request().unwrap();

        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(
            request.uri(),
            "https://huggingface.co/api/buckets/zeon256/sg-weather/batch"
        );
        assert_eq!(
            request.headers().get("content-type").unwrap(),
            "application/x-ndjson"
        );
        assert_eq!(
            request.headers().get("authorization").unwrap(),
            "Bearer hf_secret"
        );

        let body = String::from_utf8(request.body().clone()).unwrap();
        let line: serde_json::Value = serde_json::from_str(body.trim_end()).unwrap();
        assert_eq!(line["type"], "addFile");
        assert_eq!(
            line["path"],
            "data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet"
        );
        assert_eq!(
            line["xetHash"],
            "96e637d9665bd35477b1908a23f2e254edfba0618dbd2d62f90a6baee7d139cf"
        );
        assert_eq!(line["mtime"], 1_781_254_800_000_i64);
    }

    #[test]
    fn batch_decode_success_and_rejection() {
        let ok = BatchAction::decode(ResponseParts {
            status: StatusCode::OK,
            headers: http::HeaderMap::new(),
            body: b"".as_slice(),
        })
        .unwrap();
        assert!(matches!(ok, BatchOutcome::Added));

        let rejected = BatchAction::decode(ResponseParts {
            status: StatusCode::UNAUTHORIZED,
            headers: http::HeaderMap::new(),
            body: b"Invalid credentials".as_slice(),
        })
        .unwrap();
        match rejected {
            BatchOutcome::Rejected { status, body } => {
                assert_eq!(status, StatusCode::UNAUTHORIZED);
                assert_eq!(body, "Invalid credentials");
            }
            other @ BatchOutcome::Added => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn create_bucket_request_shape() {
        let request = CreateBucketAction {
            namespace: "zeon256".into(),
            name: "sg-weather".into(),
            token: "hf_secret".into(),
            private: true,
        }
        .request()
        .unwrap();

        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(
            request.uri(),
            "https://huggingface.co/api/buckets/zeon256/sg-weather"
        );
        assert_eq!(
            request.headers().get("content-type").unwrap(),
            "application/json"
        );

        let body: serde_json::Value = serde_json::from_slice(request.body()).unwrap();
        assert_eq!(body, serde_json::json!({ "private": true }));
    }

    #[test]
    fn create_bucket_decode_created_conflict_rejected() {
        let decode = |status, body: &[u8]| {
            CreateBucketAction::decode(ResponseParts {
                status,
                headers: http::HeaderMap::new(),
                body,
            })
            .unwrap()
        };

        assert!(matches!(
            decode(StatusCode::CREATED, b""),
            CreateBucketOutcome::Created
        ));
        assert!(matches!(
            decode(StatusCode::CONFLICT, b"already exists"),
            CreateBucketOutcome::AlreadyExists
        ));
        match decode(StatusCode::FORBIDDEN, b"no write permission") {
            CreateBucketOutcome::Rejected { status, body } => {
                assert_eq!(status, StatusCode::FORBIDDEN);
                assert_eq!(body, "no write permission");
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn xet_write_token_url_matches_hub_route() {
        assert_eq!(
            xet_write_token_url("zeon256/sg-weather"),
            "https://huggingface.co/api/buckets/zeon256/sg-weather/xet-write-token"
        );
    }

    /// The `mtime` is load-bearing for deterministic replay payloads: it
    /// must come from the window, never from wall-clock.
    #[test]
    fn mtime_is_window_end_in_ms() {
        let meta = WindowMeta {
            pipeline: "pm25".into(),
            start: datetime!(2026-06-12 08:00 UTC),
            end: datetime!(2026-06-12 09:00 UTC),
        };
        assert_eq!(mtime_ms(&meta), 1_781_254_800_000);
    }

    #[test]
    fn transient_classifies_retryable_failures() {
        let rejected =
            |status: StatusCode| HfBucketSinkError::<crate::encode::ParquetEncodeError>::Rejected {
                status,
                body: String::new(),
            };
        assert!(transient(&rejected(StatusCode::TOO_MANY_REQUESTS)));
        assert!(transient(&rejected(StatusCode::INTERNAL_SERVER_ERROR)));
        assert!(transient(&rejected(StatusCode::SERVICE_UNAVAILABLE)));
        assert!(!transient(&rejected(StatusCode::UNAUTHORIZED)));
        assert!(transient(&HfBucketSinkError::<ParquetEncodeError>::Xet(
            XetError::Network("connection reset".into())
        )));
        assert!(!transient(&HfBucketSinkError::<ParquetEncodeError>::Xet(
            XetError::Authentication("bad token".into())
        )));
    }
    /// The bucket sink shares the dataset sink's path derivation, so the
    /// pinned fingerprint must not drift between the two modules.
    #[test]
    fn object_path_matches_dataset_sink_scheme() {
        let meta = WindowMeta {
            pipeline: "pm25".into(),
            start: datetime!(2026-06-12 08:00 UTC),
            end: datetime!(2026-06-12 09:00 UTC),
        };
        assert_eq!(
            object_path(&meta, b"PARQUET", <ParquetEncoder as Encoder>::EXT),
            "data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet"
        );
        let json_path = object_path(&meta, b"PARQUET", JsonEncoder::EXT);
        assert_eq!(
            json_path,
            "data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.json"
        );
    }
}
