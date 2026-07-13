//! [`HfSink`]: terminal sink committing encoded window files (parquet by
//! default) to a `HuggingFace` dataset repo.
//!
//! Sans-IO, satay-style: a hand-written [`CommitAction`] implements
//! [`satay_runtime::Action`] and is sent through
//! `satay_reqwest::ReqwestActionExt::send_with` — the same transport path as
//! every collector. A satay-*generated* HF client isn't possible yet
//! (satay-codegen rejects non-JSON request bodies; NDJSON gets first-class
//! `OpenAPI` treatment only in 3.2 `itemSchema`); once one exists, swapping it
//! in is a drop-in change behind the `Action` boundary.

use std::error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use http::header;
use satay_reqwest::ReqwestActionExt;
use satay_runtime::{Action, RequestParts, ResponseParts, insert_header, into_request};
use serde::de;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::encode::{Encoder, ParquetEncodeError, ParquetEncoder};
use crate::sink::{Sink, WindowMeta};

/// Error from the `HuggingFace` sink.
#[derive(Debug, thiserror::Error)]
pub enum HfSinkError<E: error::Error + Send + Sync + 'static = ParquetEncodeError> {
    #[error(transparent)]
    Encode(E),
    #[error("transport error: {0}")]
    Transport(#[from] satay_reqwest::Error),
    #[error("hugging face rejected commit ({status}): {body}")]
    Rejected {
        status: http::StatusCode,
        body: String,
    },
}

/// One commit of a single file to a `HuggingFace` dataset repo, as a sans-IO
/// [`Action`]: `POST /api/datasets/{repo}/commit/{branch}` with an NDJSON
/// payload (commit header line + base64-inlined file line).
#[derive(Debug, Clone)]
pub struct CommitAction {
    pub repo: String,
    pub branch: String,
    pub token: String,
    pub summary: String,
    /// Path of the file inside the repo, e.g.
    /// `data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet`.
    pub path_in_repo: String,
    pub content: Vec<u8>,
}

/// Decoded result of a [`CommitAction`].
///
/// Non-2xx responses decode into [`Rejected`](CommitOutcome::Rejected)
/// rather than an error so the typed status/body survive the fixed
/// `satay_runtime::Error` decode signature.
#[derive(Debug, Clone)]
pub enum CommitOutcome {
    Committed(CommitResponse),
    Rejected {
        status: http::StatusCode,
        body: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommitResponse {
    #[serde(default)]
    pub commit_url: Option<String>,
    #[serde(default)]
    pub commit_oid: Option<String>,
}

#[derive(Serialize)]
struct NdjsonLine<V> {
    key: &'static str,
    value: V,
}

impl Action for CommitAction {
    type Response = CommitOutcome;

    fn request(self) -> Result<http::Request<Vec<u8>>, satay_runtime::Error> {
        let uri = format!(
            "https://huggingface.co/api/datasets/{}/commit/{}",
            self.repo, self.branch
        );

        let header_line = serde_json::to_vec(&NdjsonLine {
            key: "header",
            value: serde_json::json!({ "summary": self.summary }),
        })?;
        let file_line = serde_json::to_vec(&NdjsonLine {
            key: "file",
            value: serde_json::json!({
                "path": self.path_in_repo,
                "content": BASE64.encode(&self.content),
                "encoding": "base64",
            }),
        })?;

        let mut body = header_line;
        body.push(b'\n');
        body.extend_from_slice(&file_line);
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
            Ok(CommitOutcome::Committed(satay_runtime::from_json_slice(
                response.body.as_ref(),
            )?))
        } else {
            Ok(CommitOutcome::Rejected {
                status: response.status,
                body: String::from_utf8_lossy(response.body.as_ref()).into_owned(),
            })
        }
    }
}

/// Client-side commit serialization: at most one in-flight commit per gate.
///
/// `HuggingFace` serializes commits to a repo in a server-side concurrency
/// queue and rejects requests that queue too long (`429 "maximum time in
/// concurrency queue reached"`), so sinks committing to the same repo —
/// e.g. one [`HfSink`] per pipeline — should share one clone of the same
/// gate and queue client-side instead. Waiters acquire in FIFO order.
///
/// The permit is held for the duration of one send attempt, so give gated
/// clients a request timeout ([`reqwest::ClientBuilder::timeout`]) — with
/// the reqwest default of no timeout, one upload stalled by the network
/// blocks every sink sharing the gate until the OS abandons the
/// connection.
#[derive(Debug, Clone)]
pub struct CommitGate(Arc<Semaphore>);

impl CommitGate {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(Semaphore::new(1)))
    }
}

impl Default for CommitGate {
    fn default() -> Self {
        Self::new()
    }
}

/// Total send attempts per window commit (1 initial + 3 retries; backoff
/// between attempts is 2s, 4s, 8s).
const COMMIT_ATTEMPTS: u32 = 4;

/// Whether a failed commit is worth retrying in-sink: transport flakes and
/// contention/server-side statuses. Other rejections (auth, bad request)
/// and encode failures propagate immediately.
fn transient<E: error::Error + Send + Sync + 'static>(error: &HfSinkError<E>) -> bool {
    match error {
        HfSinkError::Transport(_) => true,
        HfSinkError::Rejected { status, .. } => {
            *status == http::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
        }
        HfSinkError::Encode(_) => false,
    }
}

/// Terminal sink: encodes each ingested window with its [`Encoder`]
/// (parquet by default) and commits it to a `HuggingFace` dataset repo at
/// a deterministic, Hive-style path:
///
/// ```text
/// data/{pipeline}/{YYYY-MM-DD}/{HH}-{MM}-{SS}-{fnv1a64(content)}.{E::EXT}
/// ```
///
/// The path is keyed by the full window start *and* a fingerprint of the
/// encoded bytes:
///
/// - Replaying a window (crash after upload but before the spool segment
///   was deleted) re-encodes the same records to the same bytes and
///   overwrites the same file — replays stay idempotent.
/// - Distinct windows never collide: the start is spelled out to the
///   second, covering any `FlushPolicy::every` down to 1s.
/// - Repeated drains of *one* window key — the `FlushPolicy`
///   `max_records` valve firing mid-window, or a failed drain retried
///   after more records arrived — differ in content, so each chunk lands
///   in its own file instead of silently overwriting the previous commit.
///
/// Transient failures (transport errors, 429, 5xx) retry a few times
/// in-sink with backoff before the error propagates; an upstream [`Tier`]
/// retains records when this sink errors and retries at its next firing
/// (or replays them on the next start).
///
/// [`Tier`]: crate::Tier
pub struct HfSink<R, E = ParquetEncoder> {
    client: reqwest::Client,
    repo: String,
    branch: String,
    token: String,
    gate: Option<CommitGate>,
    encoder: E,
    _record: PhantomData<fn(R)>,
}

impl<R> HfSink<R> {
    /// Sink committing to `repo` (e.g. `"zeon256/sg-weather"`) on branch
    /// `main`. The token is a `HuggingFace` access token with write access,
    /// typically from the `HF_TOKEN` env var.
    #[must_use]
    pub fn new(client: reqwest::Client, repo: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client,
            repo: repo.into(),
            branch: "main".to_owned(),
            token: token.into(),
            gate: None,
            encoder: ParquetEncoder::default(),
            _record: PhantomData,
        }
    }
}

impl<R, E: Encoder> HfSink<R, E> {
    #[must_use]
    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = branch.into();
        self
    }

    /// Serialize commits through `gate`; share one clone across every sink
    /// targeting the same repo.
    #[must_use]
    pub fn gate(mut self, gate: CommitGate) -> Self {
        self.gate = Some(gate);
        self
    }

    /// Swap the wire format this sink ships (parquet by default):
    ///
    /// ```
    /// use meathook::{HfSink, JsonEncoder};
    ///
    /// let sink = HfSink::<()>::new(reqwest::Client::new(), "you/repo", "hf_token")
    ///     .encoder(JsonEncoder);
    /// # let _ = sink;
    /// ```
    #[must_use]
    pub fn encoder<E2: Encoder>(self, encoder: E2) -> HfSink<R, E2> {
        HfSink {
            client: self.client,
            repo: self.repo,
            branch: self.branch,
            token: self.token,
            gate: self.gate,
            encoder,
            _record: PhantomData,
        }
    }

    /// One gated send attempt, with a rejection mapped into a typed error.
    async fn commit(&self, action: CommitAction) -> Result<CommitResponse, HfSinkError<E::Error>> {
        // The gate's semaphore is never closed, so acquire cannot fail;
        // falling back to an ungated send keeps that invariant harmless.
        let _permit = match &self.gate {
            Some(gate) => gate.0.acquire().await.ok(),
            None => None,
        };
        match action.send_with(&self.client).await? {
            CommitOutcome::Committed(commit) => Ok(commit),
            CommitOutcome::Rejected { status, body } => Err(HfSinkError::Rejected { status, body }),
        }
    }
}

/// FNV-1a 64-bit — stable across releases (the fingerprint is load-bearing
/// for replay idempotency: same bytes must map to the same path forever).
fn fingerprint(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}

/// Keyed by the full window start plus a fingerprint of the encoded
/// bytes. The start (to the second) identifies the window — distinct
/// windows get distinct paths no matter their content; the fingerprint
/// separates repeated drains of one window — paths only ever collide when
/// window *and* content match, and then overwriting is a no-op (see
/// [`HfSink`] docs).
fn object_path(meta: &WindowMeta, content: &[u8], ext: &str) -> String {
    let date = meta.start.date();
    format!(
        "data/{}/{:04}-{:02}-{:02}/{:02}-{:02}-{:02}-{:016x}.{}",
        meta.pipeline,
        date.year(),
        u8::from(date.month()),
        date.day(),
        meta.start.hour(),
        meta.start.minute(),
        meta.start.second(),
        fingerprint(content),
        ext,
    )
}

impl<R, E> Sink<R> for HfSink<R, E>
where
    R: Serialize + de::DeserializeOwned + Send + 'static,
    E: Encoder,
{
    type Error = HfSinkError<E::Error>;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        if records.is_empty() {
            return Ok(());
        }
        let content = self.encoder.encode(&records).map_err(HfSinkError::Encode)?;
        let path_in_repo = object_path(meta, &content, E::EXT);
        let action = CommitAction {
            repo: self.repo.clone(),
            branch: self.branch.clone(),
            token: self.token.clone(),
            summary: format!(
                "meathook: {} window {} → {}",
                meta.pipeline, meta.start, meta.end
            ),
            path_in_repo: path_in_repo.clone(),
            content,
        };

        let mut attempt = 1;
        let commit = loop {
            match self.commit(action.clone()).await {
                Ok(commit) => break commit,
                Err(error) if attempt < COMMIT_ATTEMPTS && transient(&error) => {
                    let backoff = Duration::from_secs(1 << attempt);
                    warn!(
                        pipeline = %meta.pipeline,
                        %error,
                        attempt,
                        ?backoff,
                        "hugging face commit failed; retrying"
                    );
                    sleep(backoff).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
        info!(
            pipeline = %meta.pipeline,
            path = %path_in_repo,
            records = records.len(),
            commit = commit.commit_oid.as_deref().unwrap_or("?"),
            "committed window to hugging face"
        );
        Ok(())
    }

    /// No-op: this terminal sink ships every batch as it is ingested.
    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;
    use crate::encode::JsonEncoder;

    fn action() -> CommitAction {
        CommitAction {
            repo: "zeon256/sg-weather".into(),
            branch: "main".into(),
            token: "hf_secret".into(),
            summary: "meathook: pm25 window".into(),
            path_in_repo: "data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet".into(),
            content: b"PARQUET".to_vec(),
        }
    }

    #[test]
    fn commit_request_shape() {
        let request = action().request().unwrap();

        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(
            request.uri(),
            "https://huggingface.co/api/datasets/zeon256/sg-weather/commit/main"
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
        let lines = body.lines().collect::<Vec<&str>>();
        assert_eq!(lines.len(), 2);

        let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["key"], "header");
        assert_eq!(header["value"]["summary"], "meathook: pm25 window");

        let file: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(file["key"], "file");
        assert_eq!(
            file["value"]["path"],
            "data/pm25/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet"
        );
        assert_eq!(file["value"]["encoding"], "base64");
        let decoded = BASE64
            .decode(file["value"]["content"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, b"PARQUET");
    }

    #[test]
    fn decode_success_and_rejection() {
        let ok = CommitAction::decode(ResponseParts {
            status: http::StatusCode::OK,
            headers: http::HeaderMap::new(),
            body: br#"{"commitUrl":"https://hf.co/c/abc","commitOid":"abc123"}"#.as_slice(),
        })
        .unwrap();
        match ok {
            CommitOutcome::Committed(c) => {
                assert_eq!(c.commit_oid.as_deref(), Some("abc123"));
            }
            other @ CommitOutcome::Rejected { .. } => panic!("expected Committed, got {other:?}"),
        }

        let rejected = CommitAction::decode(ResponseParts {
            status: http::StatusCode::UNAUTHORIZED,
            headers: http::HeaderMap::new(),
            body: b"Invalid credentials".as_slice(),
        })
        .unwrap();
        match rejected {
            CommitOutcome::Rejected { status, body } => {
                assert_eq!(status, http::StatusCode::UNAUTHORIZED);
                assert_eq!(body, "Invalid credentials");
            }
            other @ CommitOutcome::Committed(_) => panic!("expected Rejected, got {other:?}"),
        }
    }

    /// Pins the exact path for known bytes: the fingerprint is
    /// load-bearing for replay idempotency, so a hash-algorithm change
    /// must show up as a failure here.
    #[test]
    fn object_path_is_hive_partitioned() {
        let meta = WindowMeta {
            pipeline: "air_temperature".into(),
            start: datetime!(2026-06-12 08:00 UTC),
            end: datetime!(2026-06-12 09:00 UTC),
        };
        assert_eq!(
            object_path(&meta, b"PARQUET", <ParquetEncoder as Encoder>::EXT),
            "data/air_temperature/2026-06-12/08-00-00-abe4fb8a17f5800b.parquet"
        );
    }

    #[test]
    fn object_path_keeps_windows_distinct_down_to_seconds() {
        let at = |start: time::OffsetDateTime| WindowMeta {
            pipeline: "pm25".into(),
            start,
            end: start + Duration::from_secs(30),
        };
        assert_eq!(
            object_path(
                &at(datetime!(2026-07-02 13:20 UTC)),
                b"PARQUET",
                <ParquetEncoder as Encoder>::EXT
            ),
            "data/pm25/2026-07-02/13-20-00-abe4fb8a17f5800b.parquet"
        );
        // Two 30s windows in the same minute with identical payloads:
        // the seconds field keeps their files apart.
        assert_eq!(
            object_path(
                &at(datetime!(2026-07-02 13:20:30 UTC)),
                b"PARQUET",
                <ParquetEncoder as Encoder>::EXT
            ),
            "data/pm25/2026-07-02/13-20-30-abe4fb8a17f5800b.parquet"
        );
    }

    /// Repeated drains of one window key (`max_records` valve mid-window,
    /// or a failed drain retried after more records arrived) must not
    /// overwrite each other; identical content must (replay idempotency).
    #[test]
    fn object_path_separates_chunks_of_one_window() {
        let meta = WindowMeta {
            pipeline: "rainfall".into(),
            start: datetime!(2026-07-02 13:20 UTC),
            end: datetime!(2026-07-02 13:30 UTC),
        };
        let first = object_path(&meta, b"chunk-1", <ParquetEncoder as Encoder>::EXT);
        let second = object_path(&meta, b"chunk-2", <ParquetEncoder as Encoder>::EXT);
        assert_ne!(first, second);
        assert_eq!(
            first,
            "data/rainfall/2026-07-02/13-20-00-e58fad5f76c7ba24.parquet"
        );
        assert_eq!(
            object_path(&meta, b"chunk-1", <ParquetEncoder as Encoder>::EXT),
            first
        );
    }

    #[test]
    fn transient_classifies_retryable_failures() {
        let rejected = |status: http::StatusCode| -> HfSinkError {
            HfSinkError::Rejected {
                status,
                body: String::new(),
            }
        };
        assert!(transient(&rejected(http::StatusCode::TOO_MANY_REQUESTS)));
        assert!(transient(&rejected(
            http::StatusCode::INTERNAL_SERVER_ERROR
        )));
        assert!(transient(&rejected(http::StatusCode::SERVICE_UNAVAILABLE)));
        assert!(!transient(&rejected(http::StatusCode::UNAUTHORIZED)));
        assert!(!transient(&rejected(http::StatusCode::BAD_REQUEST)));
    }

    #[test]
    fn json_encoder_swaps_extension_and_body() {
        let records = vec![serde_json::json!({"station_id": "S100", "value": 29.4})];
        let content = JsonEncoder.encode(&records).unwrap();
        let meta = WindowMeta {
            pipeline: "pm25".into(),
            start: datetime!(2026-06-12 08:00 UTC),
            end: datetime!(2026-06-12 09:00 UTC),
        };
        let path = object_path(&meta, &content, JsonEncoder::EXT);
        assert!(path.starts_with("data/pm25/2026-06-12/08-00-00-"));
        assert_eq!(
            std::path::Path::new(&path)
                .extension()
                .and_then(std::ffi::OsStr::to_str),
            Some("json")
        );
        let parsed: serde_json::Value = serde_json::from_slice(&content).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!([{"station_id": "S100", "value": 29.4}])
        );
        // Builder swaps the type param — compile-level proof of pluggability.
        let _sink =
            HfSink::<serde_json::Value>::new(reqwest::Client::new(), "you/repo", "hf_token")
                .encoder(JsonEncoder);
    }
}
