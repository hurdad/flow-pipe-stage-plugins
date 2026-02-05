# file_sink

See `file_sink.proto` for full options.
## Configuration

```yaml
path: "/data/output.bin"
append: false
compression: COMPRESSION_NONE
compression_level: 6
```

## Inputs

- Payload: raw bytes to write to the file. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
