# orc_arrow_source

Reads an ORC file with Apache Arrow's ORC adapter and emits Arrow IPC stream payloads.

## Configuration

See `orc_arrow_source.proto` for full options. Key settings:

- `path`: ORC file path.
- `output_type`: `OUTPUT_TYPE_TABLE` (single payload) or `OUTPUT_TYPE_RECORD_BATCH` (one payload per batch).
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
