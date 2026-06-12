//! [`SatayCollector`]: adapter turning any satay-generated API client into a
//! [`Collector`].
//!
//! Satay actions implement [`satay_runtime::Action`] and are consumed by
//! `request()`, and generated actions borrow their `Api` — so a fresh action
//! must be built inside each tick's future. The `make` closure receives a
//! cheap clone of the shared [`reqwest::Client`] and returns the future for
//! one poll; generated `Api` types are `Clone`, so the closure clones its
//! captured `Api` into the future:
//!
//! ```ignore
//! let api = nea_rs::Api::new();
//! let collector = SatayCollector::new(
//!     "air_temperature",
//!     client,
//!     move |client| {
//!         let api = api.clone();
//!         async move { api.air_temperature().send_with(&client).await }
//!     },
//!     |response| flatten(response), // API response -> row-shaped records
//! );
//! ```

use std::future::Future;
use std::marker::PhantomData;

use crate::collector::Collector;

/// Adapter from a satay-generated client to a [`Collector`].
///
/// `make` builds and sends one request per tick (via
/// `satay_reqwest::ReqwestActionExt::send_with`); `transform` flattens the
/// typed API response into row-shaped records.
pub struct SatayCollector<M, T, Fut, Rec> {
    name: String,
    client: reqwest::Client,
    make: M,
    transform: T,
    _marker: PhantomData<fn() -> (Fut, Rec)>,
}

impl<M, T, Fut, Rec> SatayCollector<M, T, Fut, Rec> {
    pub fn new(name: impl Into<String>, client: reqwest::Client, make: M, transform: T) -> Self {
        Self {
            name: name.into(),
            client,
            make,
            transform,
            _marker: PhantomData,
        }
    }
}

impl<M, T, Fut, Resp, Rec> Collector for SatayCollector<M, T, Fut, Rec>
where
    M: FnMut(reqwest::Client) -> Fut + Send,
    Fut: Future<Output = Result<Resp, satay_reqwest::Error>> + Send,
    T: FnMut(Resp) -> Vec<Rec> + Send,
    Rec: Send + 'static,
{
    type Record = Rec;
    type Error = satay_reqwest::Error;

    fn name(&self) -> &str {
        &self.name
    }

    async fn collect(&mut self) -> Result<Vec<Rec>, satay_reqwest::Error> {
        let response = (self.make)(self.client.clone()).await?;
        Ok((self.transform)(response))
    }
}
