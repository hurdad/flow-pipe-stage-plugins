# file_source

See `file_source.proto` for full options.
## Configuration

```yaml
path: "/data/input.bin"
compression: COMPRESSION_AUTO
max_bytes: 0
```

Set `max_bytes` to a positive value to cap file reads and prevent large files from
being buffered entirely in memory.

## Inputs

- None (source stage).

## Outputs

- Payload: raw bytes of the file contents. Schema: `bytes` (opaque binary data).
