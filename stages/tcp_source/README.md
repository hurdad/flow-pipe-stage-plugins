# tcp_source

See `tcp_source.proto` for full options.
## Configuration

```yaml
unix_socket_path: ""
bind_address: "0.0.0.0"
port: 9000
max_payload_size: 65536
poll_timeout_ms: 1000
```

## Inputs

- None (source stage).

## Outputs

- Payload: raw bytes read from the TCP connection (up to `max_payload_size`). Schema: `bytes` (opaque binary data).
