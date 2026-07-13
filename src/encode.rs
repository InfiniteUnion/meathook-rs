//! Pluggable window encoders: an [`Encoder`] turns one window of records
//! into the bytes of a single file. Records stay plain
//! `#[derive(Serialize)]` structs — no format-specific builders — and a
//! terminal sink's wire format is swapped by choosing an encoder.
//! [`ParquetEncoder`] (feature `parquet`) stays the `HfSink` default;
//! [`JsonEncoder`] is always available; [`CsvEncoder`] is gated behind the
//! `csv` feature.

use std::error;
#[cfg(feature = "parquet")]
use std::marker::PhantomData;

#[cfg(feature = "parquet")]
use arrow::datatypes::FieldRef;
#[cfg(feature = "parquet")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "parquet")]
use parquet::basic::{Compression, ZstdLevel};
#[cfg(feature = "parquet")]
use parquet::errors::ParquetError;
#[cfg(feature = "parquet")]
use parquet::file::properties::WriterProperties;
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

#[cfg(feature = "parquet")]
mod private {
    pub trait Sealed {}
}

/// A type-level parquet compression policy.
///
/// This trait is sealed. The built-in policies are [`Uncompressed`] and
/// [`Zstd<LEVEL>`](Zstd), where only levels `1..=22` implement this trait.
#[cfg(feature = "parquet")]
pub trait ParquetCompression: private::Sealed + Send + Sync + 'static {
    #[doc(hidden)]
    fn parquet_compression() -> Result<Compression, ParquetError>;
}

/// Type-level policy for uncompressed parquet output.
#[cfg(feature = "parquet")]
#[derive(Debug, Clone, Copy, Default)]
pub struct Uncompressed;

#[cfg(feature = "parquet")]
impl private::Sealed for Uncompressed {}

#[cfg(feature = "parquet")]
impl ParquetCompression for Uncompressed {
    fn parquet_compression() -> Result<Compression, ParquetError> {
        Ok(Compression::UNCOMPRESSED)
    }
}

/// Type-level policy for zstd-compressed parquet output.
///
/// `LEVEL` defaults to `1`. Only levels `1..=22` implement
/// [`ParquetCompression`], so an encoder with an invalid level cannot be
/// constructed.
///
/// ```compile_fail
/// use meathook::{ParquetEncoder, Zstd};
///
/// let encoder = ParquetEncoder::<Zstd<23>>::new();
/// ```
#[cfg(feature = "parquet")]
#[derive(Debug, Clone, Copy, Default)]
pub struct Zstd<const LEVEL: u8 = 1>;

#[cfg(feature = "parquet")]
macro_rules! impl_zstd_levels {
    ($($level:literal),* $(,)?) => {
        $(
            impl private::Sealed for Zstd<$level> {}

            impl ParquetCompression for Zstd<$level> {
                fn parquet_compression() -> Result<Compression, ParquetError> {
                    ZstdLevel::try_new($level).map(Compression::ZSTD)
                }
            }
        )*
    };
}

#[cfg(feature = "parquet")]
impl_zstd_levels!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
);

/// Encodes a window into a parquet file held in memory.
///
/// The arrow schema is derived from `R` itself (not sampled from values, so
/// an empty slice still produces a valid zero-row file), which is why
/// `DeserializeOwned` is required alongside `Serialize`.
///
/// The default compression policy is [`Uncompressed`]. Select zstd and its
/// level in the encoder type:
///
/// ```
/// use meathook::{ParquetEncoder, Zstd};
///
/// let uncompressed = ParquetEncoder::default();
/// let zstd_1 = ParquetEncoder::<Zstd>::new();
/// let zstd_3 = ParquetEncoder::<Zstd<3>>::new();
/// ```
#[cfg(feature = "parquet")]
#[derive(Debug, Clone, Copy)]
pub struct ParquetEncoder<C = Uncompressed> {
    _compression: PhantomData<C>,
}

#[cfg(feature = "parquet")]
impl<C: ParquetCompression> ParquetEncoder<C> {
    /// Creates an encoder using the compression policy `C`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _compression: PhantomData,
        }
    }

    fn writer_properties() -> Result<WriterProperties, ParquetError> {
        Ok(WriterProperties::builder()
            .set_compression(C::parquet_compression()?)
            .build())
    }
}

#[cfg(feature = "parquet")]
impl Default for ParquetEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "parquet")]
impl<C: ParquetCompression> Encoder for ParquetEncoder<C> {
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
        let mut writer =
            ArrowWriter::try_new(&mut buf, batch.schema(), Some(Self::writer_properties()?))?;
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
        use parquet::basic::{Compression, ZstdLevel};
        use parquet::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};

        use super::*;

        fn compressions(bytes: Vec<u8>) -> Vec<Compression> {
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
            builder
                .metadata()
                .row_groups()
                .iter()
                .flat_map(RowGroupMetaData::columns)
                .map(ColumnChunkMetaData::compression)
                .collect()
        }

        #[test]
        fn parquet_round_trip() {
            let records = samples();

            let encoder = ParquetEncoder::default();
            let bytes = encoder.encode(&records).unwrap();

            assert!(
                compressions(bytes.clone())
                    .iter()
                    .all(|compression| *compression == Compression::UNCOMPRESSED)
            );

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
        fn default_policy_uses_uncompressed_codec() {
            let bytes = ParquetEncoder::default().encode(&samples()).unwrap();

            assert!(
                compressions(bytes)
                    .iter()
                    .all(|compression| *compression == Compression::UNCOMPRESSED)
            );
        }

        #[test]
        fn empty_slice_encodes_zero_row_file() {
            let encoded = [
                ParquetEncoder::default().encode::<Sample>(&[]).unwrap(),
                ParquetEncoder::<Zstd>::new().encode::<Sample>(&[]).unwrap(),
            ];

            for bytes in encoded {
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                    .unwrap()
                    .build()
                    .unwrap();
                let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
                assert_eq!(rows, 0);
            }
        }

        #[test]
        fn zstd_default_level_round_trips() {
            let records = samples();
            let bytes = ParquetEncoder::<Zstd>::new().encode(&records).unwrap();

            assert!(
                compressions(bytes.clone())
                    .iter()
                    .all(|compression| { *compression == Compression::ZSTD(ZstdLevel::default()) })
            );

            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap();
            let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
            let round_tripped = serde_arrow::from_record_batch::<Vec<Sample>>(&batches[0]).unwrap();

            assert_eq!(round_tripped, records);
        }

        #[test]
        fn zstd_explicit_level_is_applied() {
            let level = ZstdLevel::try_new(7).unwrap();
            let encoder = ParquetEncoder::<Zstd<7>>::new();
            assert_eq!(
                Zstd::<7>::parquet_compression().unwrap(),
                Compression::ZSTD(level)
            );

            let bytes = encoder.encode(&samples()).unwrap();

            assert!(
                compressions(bytes.clone())
                    .iter()
                    .all(|compression| matches!(compression, Compression::ZSTD(_)))
            );
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        }

        #[test]
        fn zstd_boundary_levels_are_applied() {
            let policies = [
                Zstd::<1>::parquet_compression().unwrap(),
                Zstd::<22>::parquet_compression().unwrap(),
            ];

            assert_eq!(
                policies,
                [
                    Compression::ZSTD(ZstdLevel::try_new(1).unwrap()),
                    Compression::ZSTD(ZstdLevel::try_new(22).unwrap()),
                ]
            );
        }

        #[test]
        fn zstd_shrinks_compressible_records() {
            let records: Vec<_> = (0..4_096)
                .map(|index| Sample {
                    station_id: format!("weather-station-{index:08}"),
                    timestamp: format!("2026-07-13T12:{:02}:{:02}+08:00", index / 60, index % 60),
                    value: 29.0 + f64::from(index % 10) / 10.0,
                })
                .collect();

            let uncompressed = ParquetEncoder::default().encode(&records).unwrap();
            let compressed = ParquetEncoder::<Zstd>::new().encode(&records).unwrap();

            assert!(compressed.len() < uncompressed.len());
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
