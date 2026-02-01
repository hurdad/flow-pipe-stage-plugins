# parquet_arrow_sink

Reads Arrow IPC stream payloads (RecordBatch stream format) into a table and writes Parquet files
using Apache Arrow.

## Configuration

See `parquet_arrow_sink.proto` for full options. Key settings:

- `path`: Parquet file path to write.
- `row_group_size`: optional row group size to use when writing.
- `filesystem`: select local, s3, gcs, hdfs, or auto-detect from URI.
- `compression`: compression codec for parquet output (uncompressed, snappy, gzip, brotli, zstd, lz4, lz4_frame, lzo, bz2).
- `writer_properties`: optional block for all Parquet writer properties, including page sizes, encoding,
  statistics, page index, sorting columns, bloom filters, and content-defined chunking options.
- `write_opts`: optional block to write via Arrow Dataset with hive partitioning. When set, `path`
  is treated as the dataset base directory, and `partition_columns` controls hive partition keys.
- Input payloads must be Arrow IPC streams (for example, produced by `parquet_arrow_source`).
