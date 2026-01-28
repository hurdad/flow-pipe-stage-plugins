# csv_arrow_sink

Consumes Arrow IPC stream payloads and writes CSV using Apache Arrow's CSV writer.

## Configuration

See `csv_arrow_sink.proto` for full options. Key settings:

- `path`: CSV output path.
- `append`: append to existing file when true.
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `compression`: select compression codec (auto, uncompressed, snappy, gzip, brotli, zstd, lz4, lz4_frame, lzo, bz2).
- Arrow CSV `WriteOptions` mapped via delimiter, header, quoting style, and null string.
