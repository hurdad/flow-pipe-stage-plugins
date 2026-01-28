# orc_arrow_sink

Consumes Arrow IPC stream payloads and writes ORC using Apache Arrow's ORC adapter.

## Configuration

See `orc_arrow_sink.proto` for full options. Key settings:

- `path`: ORC output path.
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `write_options`: ORC write options such as stripe size, compression, batch size, and bloom filters.
