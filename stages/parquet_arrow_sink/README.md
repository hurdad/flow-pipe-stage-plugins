# parquet_arrow_sink

Writes Arrow IPC stream payloads to a Parquet file using Apache Arrow.

## Configuration

See `parquet_arrow_sink.proto` for full options. Key settings:

- `path`: Parquet file path to write.
- `row_group_size`: optional row group size to use when writing.
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `compression`: compression codec for parquet output (uncompressed, snappy, gzip, brotli, zstd, lz4, lz4_frame, lzo, bz2).
- `writer_properties`: optional block for all Parquet writer properties, including page sizes, encoding,
  statistics, page index, sorting columns, bloom filters, and content-defined chunking options.
