# udp_source

## Configuration

```yaml
unix_socket_path: ""
bind_address: "0.0.0.0"
port: 9001
max_payload_size: 65507
poll_timeout_ms: 1000
```

## Inputs

- None (source stage).

## Outputs

- Payload: raw bytes from the UDP datagram. Schema: `bytes` (opaque binary data).
