//! Pluggable window encoders: an [`Encoder`] turns one window of records
//! into the bytes of a single file. Records stay plain
//! `#[derive(Serialize)]` structs — no format-specific builders — and a
//! terminal sink's wire format is swapped by choosing an encoder.
//! [`ParquetEncoder`] (feature `parquet`) stays the `HfSink` default;
//! [`JsonEncoder`] is always available; [`CsvEncoder`] is gated behind the
//! `csv` feature.

use std::error;

#[cfg(feature = "parquet")]
use arrow::datatypes::FieldRef;
#[cfg(feature = "parquet")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "parquet")]
use parquet::errors::ParquetError;
use serde::Serialize;
use serde::de::DeserializeOwned;
#[cfg(feature = "parquet")]
use serde_arrow::schema::{SchemaLike, TracingOptions};

/// Encodes one window of records into the bytes of a single file.
///
/// Implementations must succeed on an empty slice (a valid empty file),
/// since callers may hand over drained-but-empty windows.
pub trait Encoder: Send + Sync + 'static {
    /// Error produced when encoding fails.
    type Error: error::Error + Send + Sync + 'static;

    /// File extension (no leading dot) for files this encoder produces,
    /// e.g. `"parquet"`.
    const EXT: &'static str;

    /// Encode records into an in-memory file.
    ///
    /// # Errors
    ///
    /// Returns the encoder's error if serialization fails.
    fn encode<R: Serialize + DeserializeOwned>(
        &self,
        records: &[R],
    ) -> Result<Vec<u8>, Self::Error>;
}

/// Encodes a window as one JSON array per file.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEncoder;

impl Encoder for JsonEncoder {
    type Error = serde_json::Error;
    const EXT: &'static str = "json";

    fn encode<R: Serialize + DeserializeOwned>(
        &self,
        records: &[R],
    ) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(records)
    }
}

/// Error encoding records to parquet.
#[cfg(feature = "parquet")]
#[derive(Debug, thiserror::Error)]
pub enum ParquetEncodeError {
    #[error("failed to derive arrow schema from record type: {0}")]
    Schema(#[source] serde_arrow::Error),
    #[error("failed to build record batch: {0}")]
    Batch(#[source] serde_arrow::Error),
    #[error("failed to write parquet: {0}")]
    Parquet(#[from] ParquetError),
}

/// Encodes a window into a parquet file held in memory.
///
/// The arrow schema is derived from `R` itself (not sampled from values, so
/// an empty slice still produces a valid zero-row file), which is why
/// `DeserializeOwned` is required alongside `Serialize`.
#[cfg(feature = "parquet")]
#[derive(Debug, Clone, Copy, Default)]
pub struct ParquetEncoder;

#[cfg(feature = "parquet")]
impl Encoder for ParquetEncoder {
    type Error = ParquetEncodeError;
    const EXT: &'static str = "parquet";

    /// # Errors
    ///
    /// Returns an error if schema derivation, record batch construction, or
    /// parquet writing fails.
    fn encode<R: Serialize + DeserializeOwned>(
        &self,
        records: &[R],
    ) -> Result<Vec<u8>, Self::Error> {
        let fields = Vec::<FieldRef>::from_type::<R>(TracingOptions::default())
            .map_err(ParquetEncodeError::Schema)?;
        let batch =
            serde_arrow::to_record_batch(&fields, &records).map_err(ParquetEncodeError::Batch)?;

        let mut buf = vec![];
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buf)
    }
}

/// Error encoding records to CSV.
#[cfg(feature = "csv")]
#[derive(Debug, thiserror::Error)]
pub enum CsvError {
    #[error("failed to serialize record to csv: {0}")]
    Serialize(#[from] csv::Error),
    #[error("failed to flush csv writer: {0}")]
    Flush(#[from] std::io::Error),
}

/// Encodes a window as one CSV file.
///
/// The header row is derived from the record's field names (the csv
/// crate's default); records must be flat — nested structs fail with
/// [`CsvError::Serialize`]. An empty slice encodes to an empty file, since
/// headers are only written together with the first record.
#[cfg(feature = "csv")]
#[derive(Debug, Clone, Copy, Default)]
pub struct CsvEncoder;

#[cfg(feature = "csv")]
impl Encoder for CsvEncoder {
    type Error = CsvError;
    const EXT: &'static str = "csv";

    fn encode<R: Serialize + DeserializeOwned>(
        &self,
        records: &[R],
    ) -> Result<Vec<u8>, Self::Error> {
        let mut writer = csv::Writer::from_writer(Vec::new());
        for record in records {
            writer.serialize(record)?;
        }
        writer
            .into_inner()
            .map_err(|e| CsvError::Flush(e.into_error()))
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        station_id: String,
        timestamp: String,
        value: f64,
    }

    fn samples() -> Vec<Sample> {
        vec![
            Sample {
                station_id: "S100".into(),
                timestamp: "2026-06-12T08:00:00+08:00".into(),
                value: 29.4,
            },
            Sample {
                station_id: "S117".into(),
                timestamp: "2026-06-12T08:00:00+08:00".into(),
                value: 30.1,
            },
        ]
    }

    #[cfg(feature = "parquet")]
    mod parquet_encoder {
        use arrow::array::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        use super::*;

        #[test]
        fn parquet_round_trip() {
            let records = samples();

            let bytes = ParquetEncoder.encode(&records).unwrap();

            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap();
            let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

            let round_tripped = serde_arrow::from_record_batch::<Vec<Sample>>(&batches[0]).unwrap();

            assert_eq!(round_tripped, records);
        }

        #[test]
        fn empty_slice_encodes_zero_row_file() {
            let bytes = ParquetEncoder.encode::<Sample>(&[]).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap();
            let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
            assert_eq!(rows, 0);
        }
    }

    mod json_encoder {
        use super::*;

        #[test]
        fn json_round_trip() {
            let records = samples();
            let bytes = JsonEncoder.encode(&records).unwrap();
            let round_tripped: Vec<Sample> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(round_tripped, records);
        }

        #[test]
        fn empty_slice_encodes_empty_array() {
            assert_eq!(JsonEncoder.encode::<Sample>(&[]).unwrap(), b"[]");
        }
    }

    #[cfg(feature = "csv")]
    mod csv_encoder {
        use super::*;

        #[test]
        fn csv_round_trip_with_headers() {
            let records = samples();
            let bytes = CsvEncoder.encode(&records).unwrap();
            let round_tripped = ::csv::Reader::from_reader(bytes.as_slice())
                .deserialize()
                .collect::<Result<Vec<Sample>, _>>()
                .unwrap();
            assert_eq!(round_tripped, records);
        }

        #[test]
        fn empty_slice_encodes_empty_output() {
            assert!(CsvEncoder.encode::<Sample>(&[]).unwrap().is_empty());
        }
    }
}
