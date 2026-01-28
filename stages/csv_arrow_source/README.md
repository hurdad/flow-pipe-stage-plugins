# csv_arrow_source

Reads a CSV file with Apache Arrow's CSV reader and emits Arrow IPC stream payloads.

## Configuration

See `csv_arrow_source.proto` for full options. Key settings:

- `path`: CSV file path.
- `output_type`: `OUTPUT_TYPE_TABLE` (single payload) or `OUTPUT_TYPE_RECORD_BATCH` (one payload per batch).
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `compression`: select compression codec (auto, uncompressed, snappy, gzip, brotli, zstd, lz4, lz4_frame, lzo, bz2).
- Arrow CSV options mapped from `ReadOptions`, `ParseOptions`, and `ConvertOptions`.
