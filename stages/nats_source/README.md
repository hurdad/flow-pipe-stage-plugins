# nats_source

See `nats_source.proto` for full options.
## Configuration

```yaml
url: "nats://127.0.0.1:4222"
subject: "events"
queue_group: "workers"
poll_timeout_ms: 1000
```

## Inputs

- None (source stage).

## Outputs

- Payload: NATS message payload as raw bytes. Schema: `bytes` (opaque binary data).
