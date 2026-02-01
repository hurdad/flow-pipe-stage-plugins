# json_arrow_source

Reads a line-delimited JSON file using Apache Arrow's JSON reader and emits Arrow IPC stream payloads.

See `json_arrow_source.proto` for full options. Key settings:

- `path`: file path or URI to read.
- `output_type`: emit a single table payload or individual record batches.
- `read_options` and `parse_options`: Arrow JSON read and parse options (including
  `use_threads`, `block_size`, `newlines_in_values`, `unexpected_field_behavior`, and
  `explicit_schema`).
- `explicit_schema` maps column names to Arrow types as defined in `arrow_schema.proto`.
- `filesystem` and `compression`: storage and compression handling.
