use std::{fs::File, path::Path};

use arrow_array::{Array, Int64Array, RecordBatch, UInt8Array, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{
    error::{MarketForgeError, Result},
    formats::depth::{DepthEventBoundary, DepthSourceOperation},
    process::parquet::{IndexedDepthEvent, depth_event_schema},
};

pub struct DepthEventCursor {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
}

impl DepthEventCursor {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to open depth event index {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| invalid_error(error.to_string()))?;

        if builder.schema().as_ref() != depth_event_schema().as_ref() {
            return invalid(format!("unexpected depth event schema: {}", path.display()));
        }

        let reader = builder
            .with_batch_size(8192)
            .build()
            .map_err(|error| invalid_error(error.to_string()))?;

        Ok(Self {
            reader,
            batch: None,
            row: 0,
        })
    }

    pub fn next_event(&mut self) -> Result<Option<IndexedDepthEvent>> {
        loop {
            if let Some(batch) = &self.batch {
                if self.row < batch.num_rows() {
                    let event = decode_event(batch, self.row)?;
                    self.row += 1;
                    return Ok(Some(event));
                }
            }

            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.batch = Some(batch);
                    self.row = 0;
                }

                Some(Err(error)) => {
                    return invalid(format!("failed to read depth event index: {error}"));
                }

                None => return Ok(None),
            }
        }
    }
}

fn decode_event(batch: &RecordBatch, row: usize) -> Result<IndexedDepthEvent> {
    let u64_column = |name: &str| -> Result<&UInt64Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid_error(format!("invalid UInt64 column: {name}")))
    };

    let i64_column = |name: &str| -> Result<&Int64Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| invalid_error(format!("invalid Int64 column: {name}")))
    };

    let u8_column = |name: &str| -> Result<&UInt8Array> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
            .ok_or_else(|| invalid_error(format!("invalid UInt8 column: {name}")))
    };

    let optional_u64 = |name: &str| -> Result<Option<u64>> {
        let column = u64_column(name)?;

        Ok((!column.is_null(row)).then(|| column.value(row)))
    };

    let optional_i64 = |name: &str| -> Result<Option<i64>> {
        let column = i64_column(name)?;

        Ok((!column.is_null(row)).then(|| column.value(row)))
    };

    let event_boundary = match u8_column("event_boundary")?.value(row) {
        0 => DepthEventBoundary::Changes,
        1 => DepthEventBoundary::Initialization,

        value => {
            return invalid(format!("unknown depth event boundary: {value}"));
        }
    };

    let source_operation = match u8_column("source_operation")?.value(row) {
        0 => DepthSourceOperation::Snapshot,
        1 => DepthSourceOperation::AbsoluteUpdate,
        2 => DepthSourceOperation::RelativeUpdate,

        value => {
            return invalid(format!("unknown depth source operation: {value}"));
        }
    };

    Ok(IndexedDepthEvent {
        event_ordinal: u64_column("event_ordinal")?.value(row),
        canonical_row_offset: u64_column("canonical_row_offset")?.value(row),
        canonical_row_count: u64_column("canonical_row_count")?.value(row),

        event_timestamp_ns: i64_column("event_timestamp_ns")?.value(row),
        system_timestamp_ns: optional_i64("system_timestamp_ns")?,

        sequence_start: optional_u64("sequence_start")?,
        sequence_end: optional_u64("sequence_end")?,

        event_boundary,
        source_operation,
    })
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
