# nats_jetstream_source

See `nats_jetstream_source.proto` for full options.
## Configuration

```yaml
url: "nats://127.0.0.1:4222"
subject: "events"
stream_name: "EVENTS"
durable_name: "flowpipe"
poll_timeout_ms: 1000
```

## Inputs

- None (source stage).

## Outputs

- Payload: JetStream message payload as raw bytes. Schema: `bytes` (opaque binary data).
