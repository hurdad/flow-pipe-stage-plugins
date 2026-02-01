# parquet_arrow_source

Reads a Parquet file with Apache Arrow's Parquet reader and emits Arrow IPC stream payloads
(RecordBatch stream format).

## Configuration

See `parquet_arrow_source.proto` for full options. Key settings:

- `path`: Parquet file path.
- `output_type`: `OUTPUT_TYPE_TABLE` (single payload) or `OUTPUT_TYPE_RECORD_BATCH` (one payload per batch);
  both are serialized as Arrow IPC streams.
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `batch_size`: optional record batch size when emitting batches.
