# nats_jetstream_sink

## Configuration

```yaml
url: "nats://127.0.0.1:4222"
subject: "events"
```

## Inputs

- Payload: raw bytes published to the JetStream subject. Schema: `bytes` (opaque binary data).

## Outputs

- None (sink stage).
