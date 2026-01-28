# orc_arrow_sink

Consumes Arrow IPC stream payloads and writes ORC using Apache Arrow's ORC adapter.

## Configuration

See `orc_arrow_sink.proto` for full options. Key settings:

- `path`: ORC output path.
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `writer_options`: configure ORC writer settings such as batch size, compression,
  stripe size, and bloom filters.
