//! [`HfSink`]: terminal sink committing parquet files to a HuggingFace
//! dataset repo.
//!
//! Sans-IO, satay-style: a hand-written [`CommitAction`] implements
//! [`satay_runtime::Action`] and is sent through
//! `satay_reqwest::ReqwestActionExt::send_with` — the same transport path as
//! every collector. A satay-*generated* HF client isn't possible yet
//! (satay-codegen rejects non-JSON request bodies; NDJSON gets first-class
//! OpenAPI treatment only in 3.2 `itemSchema`); once one exists, swapping it
//! in is a drop-in change behind the `Action` boundary.

use std::marker::PhantomData;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use satay_reqwest::ReqwestActionExt;
use satay_runtime::{Action, RequestParts, ResponseParts, insert_header, into_request};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::encode::{self, EncodeError};
use crate::sink::{Sink, WindowMeta};

/// Error from the HuggingFace sink.
#[derive(Debug, thiserror::Error)]
pub enum HfSinkError {
    #[error(transparent)]
    Encode(#[from] EncodeError),
    #[error("transport error: {0}")]
    Transport(#[from] satay_reqwest::Error),
    #[error("hugging face rejected commit ({status}): {body}")]
    Rejected {
        status: http::StatusCode,
        body: String,
    },
}

/// One commit of a single file to a HuggingFace dataset repo, as a sans-IO
/// [`Action`]: `POST /api/datasets/{repo}/commit/{branch}` with an NDJSON
/// payload (commit header line + base64-inlined file line).
#[derive(Debug, Clone)]
pub struct CommitAction {
    pub repo: String,
    pub branch: String,
    pub token: String,
    pub summary: String,
    /// Path of the file inside the repo, e.g. `data/pm25/2026-06-12/08.parquet`.
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
        if let Some(auth) = headers.get_mut(http::header::AUTHORIZATION) {
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

/// Terminal sink: encodes each ingested window to parquet and commits it to
/// a HuggingFace dataset repo at a deterministic, Hive-style path:
///
/// ```text
/// data/{pipeline}/{YYYY-MM-DD}/{HH}.parquet
/// ```
///
/// The path depends only on the window start, so replaying a window (crash
/// after upload but before the spool segment was deleted) overwrites the
/// same file with the same content — replays are idempotent.
///
/// Retry/backoff is *not* handled here: an upstream [`DiskSpool`] or
/// [`Buffered`] tier retains records when this sink errors and retries at
/// its next firing.
///
/// [`DiskSpool`]: crate::DiskSpool
/// [`Buffered`]: crate::Buffered
pub struct HfSink<R> {
    client: reqwest::Client,
    repo: String,
    branch: String,
    token: String,
    _record: PhantomData<fn(R)>,
}

impl<R> HfSink<R> {
    /// Sink committing to `repo` (e.g. `"zeon256/sg-weather"`) on branch
    /// `main`. The token is a HuggingFace access token with write access,
    /// typically from the `HF_TOKEN` env var.
    pub fn new(client: reqwest::Client, repo: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            client,
            repo: repo.into(),
            branch: "main".to_owned(),
            token: token.into(),
            _record: PhantomData,
        }
    }

    pub fn branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = branch.into();
        self
    }
}

fn object_path(meta: &WindowMeta) -> String {
    let date = meta.start.date();
    format!(
        "data/{}/{:04}-{:02}-{:02}/{:02}.parquet",
        meta.pipeline,
        date.year(),
        u8::from(date.month()),
        date.day(),
        meta.start.hour(),
    )
}

impl<R> Sink<R> for HfSink<R>
where
    R: Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    type Error = HfSinkError;

    async fn ingest(&mut self, meta: &WindowMeta, records: Vec<R>) -> Result<(), Self::Error> {
        if records.is_empty() {
            return Ok(());
        }
        let content = encode::to_parquet(&records)?;
        let path_in_repo = object_path(meta);
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

        match action.send_with(&self.client).await? {
            CommitOutcome::Committed(commit) => {
                info!(
                    pipeline = %meta.pipeline,
                    path = %path_in_repo,
                    records = records.len(),
                    commit = commit.commit_oid.as_deref().unwrap_or("?"),
                    "committed window to hugging face"
                );
                Ok(())
            }
            CommitOutcome::Rejected { status, body } => Err(HfSinkError::Rejected { status, body }),
        }
    }

    /// No-op: this terminal sink ships every batch as it is ingested.
    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn action() -> CommitAction {
        CommitAction {
            repo: "zeon256/sg-weather".into(),
            branch: "main".into(),
            token: "hf_secret".into(),
            summary: "meathook: pm25 window".into(),
            path_in_repo: "data/pm25/2026-06-12/08.parquet".into(),
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
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);

        let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["key"], "header");
        assert_eq!(header["value"]["summary"], "meathook: pm25 window");

        let file: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(file["key"], "file");
        assert_eq!(file["value"]["path"], "data/pm25/2026-06-12/08.parquet");
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
            other => panic!("expected Committed, got {other:?}"),
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
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn object_path_is_hive_partitioned() {
        let meta = WindowMeta {
            pipeline: "air_temperature".into(),
            start: datetime!(2026-06-12 08:00 UTC),
            end: datetime!(2026-06-12 09:00 UTC),
        };
        assert_eq!(
            object_path(&meta),
            "data/air_temperature/2026-06-12/08.parquet"
        );
    }
}
