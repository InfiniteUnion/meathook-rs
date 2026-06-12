//! Parquet encoding via `serde_arrow`: records stay plain
//! `#[derive(Serialize)]` structs, no manual arrow builders.

use arrow::datatypes::FieldRef;
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_arrow::schema::{SchemaLike, TracingOptions};

/// Error encoding records to parquet.
#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("failed to derive arrow schema from record type: {0}")]
    Schema(#[source] serde_arrow::Error),
    #[error("failed to build record batch: {0}")]
    Batch(#[source] serde_arrow::Error),
    #[error("failed to write parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

/// Encode records into a parquet file held in memory.
///
/// The arrow schema is derived from `R` itself (not sampled from values, so
/// an empty slice still produces a valid zero-row file), which is why
/// `DeserializeOwned` is required alongside `Serialize`.
pub fn to_parquet<R: Serialize + DeserializeOwned>(records: &[R]) -> Result<Vec<u8>, EncodeError> {
    let fields =
        Vec::<FieldRef>::from_type::<R>(TracingOptions::default()).map_err(EncodeError::Schema)?;
    let batch = serde_arrow::to_record_batch(&fields, &records).map_err(EncodeError::Batch)?;

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Sample {
        station_id: String,
        timestamp: String,
        value: f64,
    }

    #[test]
    fn parquet_round_trip() {
        let records = vec![
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
        ];

        let bytes = to_parquet(&records).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

        let round_tripped: Vec<Sample> = serde_arrow::from_record_batch(&batches[0]).unwrap();
        assert_eq!(round_tripped, records);
    }

    #[test]
    fn empty_slice_encodes_zero_row_file() {
        let bytes = to_parquet::<Sample>(&[]).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, 0);
    }
}
