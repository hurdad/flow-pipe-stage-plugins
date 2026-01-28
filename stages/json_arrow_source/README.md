# json_arrow_source

Reads a line-delimited JSON file using Apache Arrow's JSON reader and emits Arrow IPC stream payloads.

See `json_arrow_source.proto` for full options. Key settings:

- `path`: file path or URI to read.
- `output_type`: emit a single table payload or individual record batches.
- `use_threads` and `block_size`: Arrow JSON read options.
- `filesystem` and `compression`: storage and compression handling.
