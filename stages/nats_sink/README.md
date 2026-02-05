# nats_sink

See `nats_sink.proto` for full options.
## Configuration

```yaml
url: "nats://127.0.0.1:4222"
subject: "events"
flush_timeout_ms: 1000
```

## Inputs

- Payload: raw bytes published to the NATS subject. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
